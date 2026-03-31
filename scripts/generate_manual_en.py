#!/usr/bin/env python3
"""Generate OtoCPA User Manual (English) as PDF using ReportLab."""

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

# ── Colour palette ──────────────────────────────────────────────
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
OUT_PATH = OUT_DIR / "OtoCPA_User_Manual_EN.pdf"


# ── Custom styles ───────────────────────────────────────────────
def get_styles():
    """Return a dict of custom ParagraphStyles for the manual."""
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


# ── Helper utilities ────────────────────────────────────────────
def warning_box(text, styles):
    """Amber warning box."""
    content = Paragraph(f"<b>Warning:</b> {text}", styles["Body"])
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
    """Teal tip box."""
    content = Paragraph(f"<b>Tip:</b> {text}", styles["Body"])
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
    """Return a list of bullet Paragraphs."""
    return [Paragraph(f"\u2022 {item}", styles["Bullet"]) for item in items]


def make_table(headers, rows, col_widths=None):
    """Styled data table with blue header row."""
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
    """Return numbered paragraph list."""
    return [Paragraph(f"<b>{i}.</b> {item}", styles["Bullet"])
            for i, item in enumerate(items, 1)]


def sp():
    return Spacer(1, 12)


# ═══════════════════════════════════════════════════════════════
#  SECTION BUILDERS
# ═══════════════════════════════════════════════════════════════

def build_cover_page(story, styles):
    """Cover page with title, subtitle, and version info."""
    story.append(Spacer(1, 2.5 * inch))
    story.append(Paragraph("OtoCPA", styles["Title"]))
    story.append(Spacer(1, 8))
    story.append(Paragraph("User Manual", ParagraphStyle(
        "CoverManual", parent=styles["Title"], fontSize=22, leading=28,
    )))
    story.append(Spacer(1, 0.5 * inch))
    story.append(HRFlowable(width="60%", thickness=2, color=BLUE))
    story.append(Spacer(1, 0.3 * inch))
    story.append(Paragraph(
        "Intelligent Accounting Automation for Canadian CPA Firms",
        styles["Subtitle"],
    ))
    story.append(Paragraph("Version 1.0 &mdash; 2026", ParagraphStyle(
        "CoverVer", parent=styles["Subtitle"], fontSize=11,
    )))
    story.append(Spacer(1, 1.5 * inch))
    story.append(Paragraph(
        "Quebec-compliant &bull; GST/QST &bull; CAS Audit &bull; "
        "Bilingual FR/EN &bull; AI-Powered",
        ParagraphStyle("CoverTags", parent=styles["Body"],
                       alignment=TA_CENTER, fontSize=10, textColor=BLUE),
    ))
    story.append(PageBreak())


def build_toc(story, styles):
    """Table of contents page."""
    story.append(Paragraph("Table of Contents", styles["H1"]))
    story.append(sp())
    toc_entries = [
        ("1", "Introduction", "3"),
        ("2", "Installation", "7"),
        ("3", "User Management", "14"),
        ("4", "Daily Workflow", "18"),
        ("5", "Quebec Tax Compliance", "28"),
        ("6", "Client Portal", "38"),
        ("7", "Month-End and Billing", "42"),
        ("8", "CPA Audit Module", "46"),
        ("9", "Administration", "55"),
        ("10", "Troubleshooting", "59"),
        ("11", "Glossary", "63"),
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


# ── Section 1: Introduction ────────────────────────────────────
def build_section_1_introduction(story, styles):
    """Section 1 — What OtoCPA is, architecture, requirements, AI."""
    story.append(Paragraph("1. Introduction", styles["H1"]))
    story.append(sp())

    story.append(Paragraph("1.1 What Is OtoCPA?", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA is an intelligent accounting automation platform "
        "designed for Canadian CPA firms, with deep specialisation in "
        "Quebec tax law (GST/QST), bilingual French/English operation, "
        "and full CAS audit support. It automates document intake, AI-powered "
        "data extraction, fraud detection, bank reconciliation, tax filing, "
        "and CPA audit workflows &mdash; reducing manual bookkeeping effort "
        "by up to 80%.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("1.2 Key Capabilities", styles["H2"]))
    story.extend(bullet_list([
        "Multi-channel document intake: WhatsApp, Telegram, email, "
        "folder watcher, client portal, manual upload",
        "AI-powered OCR extraction with handwriting support",
        "13-rule deterministic fraud detection engine",
        "Economic substance classification (CapEx, prepaids, loans, "
        "personal expenses, tax remittances)",
        "Full GST/QST/HST tax engine with ITC/ITR tracking",
        "Bank statement import with smart fuzzy matching",
        "QuickBooks Online integration for posting",
        "CPA audit module covering CAS 315, 320, 330, 500, 530, 550, "
        "560, 570, 580, 700, and CSQC 1",
        "Revenu Quebec FPZ-500 pre-fill and Quick Method support",
        "Complete payroll validation: QPP, QPIP, EI, HSF, CNESST, RL-1/T4",
        "Bilingual interface (French and English) with one-click toggle",
        "Client portal with Cloudflare tunnel for secure remote access",
        "Role-based access control: Owner, Manager, Employee, Client",
    ], styles))
    story.append(sp())

    _build_section_1_architecture(story, styles)
    _build_section_1_requirements(story, styles)
    _build_section_1_ai_providers(story, styles)
    story.append(PageBreak())


def _build_section_1_architecture(story, styles):
    """Sub-section: 3-layer architecture."""
    story.append(Paragraph("1.3 Three-Layer Architecture", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA uses a three-layer processing pipeline to maximise "
        "accuracy while minimising AI costs:",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Layer 1\nDeterministic", "Tax engine, fraud engine, substance\n"
         "engine, payroll validator, bank parser", "Zero AI cost, instant,\n"
         "100% reproducible"],
        ["Layer 2\nRoutine AI", "Document classification, vendor\n"
         "extraction, GL mapping", "Budget AI provider\n(e.g. DeepSeek)"],
        ["Layer 3\nPremium AI", "Complex anomaly explanation,\n"
         "compliance narrative, working papers", "Premium AI provider\n"
         "(e.g. Claude)"],
    ]
    story.append(make_table(
        ["Layer", "Functions", "Cost Profile"],
        rows,
        col_widths=[1.3 * inch, 2.8 * inch, 2.4 * inch],
    ))
    story.append(sp())
    story.append(tip_box(
        "Layer 1 handles over 70% of processing with zero AI calls. "
        "This keeps operating costs low while maintaining deterministic "
        "accuracy for tax calculations and fraud detection.",
        styles,
    ))
    story.append(sp())


def _build_section_1_requirements(story, styles):
    """Sub-section: system requirements."""
    story.append(Paragraph("1.4 System Requirements", styles["H2"]))
    rows = [
        ["Operating System", "Windows 10/11 (64-bit) or macOS 12+"],
        ["Python", "3.11 or higher"],
        ["RAM", "4 GB minimum, 8 GB recommended"],
        ["Disk Space", "500 MB for application + database growth"],
        ["Browser", "Chrome, Edge, Firefox, or Safari (current version)"],
        ["Network", "Internet for AI providers and Cloudflare tunnel"],
        ["Email (optional)", "SMTP account (Gmail, Outlook, or custom)"],
    ]
    story.append(make_table(
        ["Component", "Requirement"],
        rows,
        col_widths=[2.0 * inch, 4.5 * inch],
    ))
    story.append(sp())


def _build_section_1_ai_providers(story, styles):
    """Sub-section: AI providers."""
    story.append(Paragraph("1.5 AI Providers", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA uses two AI provider tiers configured during setup:",
        styles["Body"],
    ))
    rows = [
        ["Standard (Routine)", "DeepSeek", "Document classification, vendor\n"
         "extraction, GL account suggestion"],
        ["Premium (Complex)", "Claude via OpenRouter", "Anomaly explanation,\n"
         "compliance narrative, working paper\n"
         "generation, substance classification"],
    ]
    story.append(make_table(
        ["Tier", "Recommended Provider", "Tasks"],
        rows,
        col_widths=[1.5 * inch, 1.8 * inch, 3.2 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Both providers use the OpenAI-compatible API format. You can "
        "substitute any provider that supports this format, including "
        "self-hosted models. API keys are configured during the setup "
        "wizard (Step 4) and stored encrypted in the configuration file.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(warning_box(
        "AI extraction is not 100% accurate. All AI-processed documents "
        "pass through the review queue where a human reviewer can verify "
        "and correct any errors before posting to QuickBooks.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("1.6 Supported Document Formats", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA processes documents in multiple formats through its "
        "OCR engine. Format detection uses magic bytes (file signature), "
        "not file extensions, for reliable identification.",
        styles["Body"],
    ))
    story.append(sp())
    fmt_rows = [
        ["PDF", "Native text extraction via pdfplumber.\n"
         "Falls back to vision AI if text &lt; 20 words.", "Primary format"],
        ["JPEG / JPG", "Vision AI extraction.\nHandwriting auto-detected.", "Common for photos"],
        ["PNG", "Vision AI extraction.\nSupports transparency.", "Screenshots, scans"],
        ["TIFF", "Vision AI extraction.\nMulti-page supported.", "Professional scanners"],
        ["WebP", "Vision AI extraction.\nCompact web format.", "Web downloads"],
        ["HEIC", "Auto-converted to JPEG (quality 92)\nthen processed via vision AI.", "iPhone photos"],
    ]
    story.append(make_table(
        ["Format", "Processing Method", "Typical Source"],
        fmt_rows,
        col_widths=[1.2 * inch, 3.3 * inch, 2.0 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("1.7 Handwriting Support", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA automatically detects handwritten documents using "
        "multiple heuristics and routes them through a specialised AI "
        "prompt for improved accuracy:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Text extraction &lt; 10 words from pdfplumber adds +0.4 to "
        "handwriting probability score",
        "Image pixel variance &gt; 2000 adds +0.2 (ink variation pattern)",
        "Average word length &lt; 4.0 adds +0.1 (abbreviations common "
        "in handwriting)",
        "Score &gt; 0.5 triggers the handwritten receipt prompt template",
        "Confidence threshold for handwritten documents: 0.70 minimum",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "For best results with handwritten receipts, photograph the document "
        "in good lighting with the text filling most of the frame. Avoid "
        "shadows and skewed angles.",
        styles,
    ))

    story.append(sp())
    story.append(Paragraph("1.8 Security Architecture", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA implements multiple security layers to protect "
        "sensitive financial data:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Authentication:</b> Session-based with bcrypt password hashing "
        "(12 rounds). Sessions expire after 12 hours (configurable).",
        "<b>Rate limiting:</b> 5 failed login attempts per 15 minutes per "
        "IP triggers HTTP 429 lockout.",
        "<b>Role-based access:</b> Four roles (Owner/Manager/Employee/Client) "
        "with hierarchical permissions enforced on every route.",
        "<b>Audit logging:</b> All fraud overrides, posting approvals, and "
        "administrative actions are permanently logged.",
        "<b>API key encryption:</b> Provider API keys are stored encrypted "
        "in the configuration file.",
        "<b>Secure cookies:</b> HttpOnly flag set on session tokens. Secure "
        "flag auto-enabled when HTTPS is detected.",
        "<b>Database integrity:</b> SQLite triggers enforce immutability of "
        "signed-off working papers and finalized reconciliations.",
        "<b>Optimistic locking:</b> Version checking prevents stale "
        "approvals (Trap 6).",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "OtoCPA stores all data locally on your server. No client data "
        "is sent to OtoCPA servers. AI providers receive only the "
        "document content needed for extraction &mdash; never client names, "
        "account numbers, or other identifying information beyond what "
        "appears on the document itself.",
        styles["Body"],
    ))


# ── Section 2: Installation ────────────────────────────────────
def build_section_2_installation(story, styles):
    """Section 2 — Installation and setup wizard."""
    story.append(Paragraph("2. Installation", styles["H1"]))
    story.append(sp())

    _build_section_2_install_bat(story, styles)
    _build_section_2_wizard(story, styles)
    _build_section_2_first_login(story, styles)
    _build_section_2_config(story, styles)
    story.append(PageBreak())


def _build_section_2_install_bat(story, styles):
    """INSTALL.bat walkthrough."""
    story.append(Paragraph("2.1 Windows Installation (INSTALL.bat)", styles["H2"]))
    story.append(Paragraph(
        "The INSTALL.bat script automates the entire installation process. "
        "Double-click the file and grant administrator access when prompted.",
        styles["Body"],
    ))
    story.append(sp())
    story.extend(numbered_list([
        "<b>Admin elevation</b> &mdash; Requests administrator privileges "
        "to install the Windows service.",
        "<b>Python check</b> &mdash; Verifies Python 3.11+ is installed. "
        "If not found, automatically downloads and installs Python 3.11.9.",
        "<b>Install dependencies</b> &mdash; Runs <font face='Courier'>"
        "pip install -r requirements.txt</font> to install all packages.",
        "<b>Database migration</b> &mdash; Creates the SQLite database and "
        "all required tables via <font face='Courier'>migrate_db.py</font>.",
        "<b>Install Windows service</b> &mdash; Registers OtoCPA as a "
        "persistent background service.",
        "<b>Start service</b> &mdash; Launches the service immediately.",
        "<b>Create desktop shortcuts</b> &mdash; Creates \"OtoCPA\" "
        "(dashboard) and \"OtoCPA Setup\" (wizard) shortcuts.",
        "<b>Launch setup wizard</b> &mdash; Opens the 20-step setup wizard "
        "in your browser at <font face='Courier'>http://127.0.0.1:8790/</font>.",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "The entire installation typically takes 3-5 minutes. A log file is "
        "saved to C:\\OtoCPA\\install.log for troubleshooting.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("2.1.1 macOS Installation", styles["H3"]))
    story.append(Paragraph(
        "On macOS, open Terminal and run: <font face='Courier'>cd ~/Desktop/"
        "OtoCPA &amp;&amp; bash INSTALL_MAC.sh</font>. The script installs "
        "dependencies, creates a launchd service, and opens the wizard.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("2.1.2 Second Machine Installation", styles["H3"]))
    story.append(Paragraph(
        "To install on additional workstations, copy your "
        "<font face='Courier'>otocpa.config.json</font> to a USB drive "
        "and run:",
        styles["Body"],
    ))
    story.append(Paragraph(
        "python scripts/install_second_machine.py --config \"E:\\otocpa.config.json\"",
        styles["Code"],
    ))
    story.append(Paragraph(
        "The recommended multi-machine setup is Option B: run OtoCPA on "
        "one server with <font face='Courier'>host: 0.0.0.0</font>, then "
        "access it from other machines via browser at the server's IP address.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_2_wizard(story, styles):
    """20-step setup wizard guide."""
    story.append(Paragraph("2.2 Setup Wizard (20 Steps)", styles["H2"]))
    story.append(Paragraph(
        "The setup wizard runs on port 8790 and guides you through every "
        "configuration option. A sidebar shows progress. All steps are "
        "bilingual (French/English) with a language toggle.",
        styles["Body"],
    ))
    story.append(sp())

    wizard_steps = [
        ["0", "Welcome", "Checklist of required info: GST#, QST#, "
         "license key, admin password, professional email"],
        ["1", "Firm Information", "Company name, address, province, "
         "phone, website, GST/QST registration numbers"],
        ["2", "Administrator", "Create owner account with full name, "
         "username, email, and password (8+ chars, uppercase, digit)"],
        ["3", "License Key", "Enter and validate your LLAI- license key; "
         "displays tier, max clients/users, expiry"],
        ["4", "AI Providers", "Configure standard (DeepSeek) and premium "
         "(Claude/OpenRouter) AI providers with API URLs and keys"],
        ["5", "Email (SMTP)", "SMTP setup with Gmail/Outlook presets; "
         "test email button to verify delivery"],
        ["6", "Client Portal", "View local portal URL (port 8788); "
         "optional Cloudflare tunnel for remote access"],
        ["7", "WhatsApp", "Twilio integration: Account SID, Auth Token, "
         "WhatsApp number (~$0.005/message)"],
        ["8", "Telegram", "Bot creation via @BotFather; enter bot token "
         "and name to enable Telegram intake"],
        ["9", "Microsoft 365", "Service account for auto-reading emails, "
         "Outlook calendar sync, Teams digest"],
        ["10", "QuickBooks Online", "Realm ID, Client ID, Client Secret "
         "for posting transactions and syncing vendors"],
        ["11", "Folder Watcher", "Set inbox folder path (default: "
         "C:/OtoCPA/Inbox/) for USB scanner or cloud sync"],
        ["12", "Daily Digest", "Configure email summaries: send time, "
         "recipients, language (FR/EN)"],
        ["13", "Backup", "Backup folder, frequency (daily/weekly/login), "
         "retention count, OneDrive toggle"],
        ["14", "Notifications", "Per-event alerts: new doc, fraud, "
         "pending >X days, deadline, license, errors; channel selection"],
        ["15", "Security", "Session timeout, max login attempts, lockout "
         "duration, force HTTPS"],
        ["16", "Staff Members", "Add managers and employees with temporary "
         "passwords; displays staff table"],
        ["17", "Clients", "Add accounting clients with code, email, "
         "language, filing frequency, assigned accountant; CSV import"],
        ["18", "Review &amp; Confirm", "Summary of all configured items "
         "with status indicators (Configured/Not configured)"],
        ["19", "Complete", "Dashboard URL, portal URL, staff credentials "
         "table, PDF download of access instructions with QR codes"],
    ]
    story.append(make_table(
        ["Step", "Name", "Description"],
        wizard_steps,
        col_widths=[0.5 * inch, 1.2 * inch, 4.8 * inch],
    ))
    story.append(sp())
    story.append(warning_box(
        "Save your staff credentials from Step 19. Temporary passwords are "
        "shown only once. Download the PDF for safekeeping.",
        styles,
    ))
    story.append(sp())


def _build_section_2_first_login(story, styles):
    """First login instructions."""
    story.append(Paragraph("2.3 First Login", styles["H2"]))
    story.extend(numbered_list([
        "Open your browser to <font face='Courier'>http://127.0.0.1:8787/"
        "</font> (or click the desktop shortcut).",
        "Enter the administrator username and password created in Step 2.",
        "The system detects no staff or clients and redirects to onboarding.",
        "Follow the 3-step onboarding: add staff, add clients, review.",
        "Once complete, you land on the main review dashboard.",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "The dashboard is rate-limited to 5 failed login attempts per 15 "
        "minutes per IP address. After exceeding this limit, you receive "
        "an HTTP 429 response and must wait before retrying.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_2_config(story, styles):
    """Configuration file reference."""
    story.append(Paragraph("2.4 Configuration File Reference", styles["H2"]))
    story.append(Paragraph(
        "All settings are stored in <font face='Courier'>"
        "otocpa.config.json</font> at the project root. Key sections:",
        styles["Body"],
    ))
    config_rows = [
        ["host / port", "Network binding (default 0.0.0.0:8787)"],
        ["session_hours", "Session duration before re-login (default 12)"],
        ["ai_router.routine_provider", "Standard AI provider URL, model, key"],
        ["ai_router.premium_provider", "Premium AI provider URL, model, key"],
        ["email_digest", "SMTP settings, recipients, schedule"],
        ["security.bcrypt_rounds", "Password hashing strength (default 12)"],
        ["client_portal.port", "Portal port (default 8788)"],
        ["client_portal.max_upload_mb", "Upload size limit (default 20 MB)"],
        ["ingest.port", "Ingest service port (default 8789)"],
        ["database_path", "SQLite database location"],
    ]
    story.append(make_table(
        ["Setting", "Description"],
        config_rows,
        col_widths=[2.5 * inch, 4.0 * inch],
    ))
    story.append(sp())
    story.append(warning_box(
        "Never share your config file publicly &mdash; it contains "
        "encrypted API keys and SMTP credentials. Use the provisioning "
        "script to create clean copies for distribution.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("2.5 Multi-Machine Deployment Options", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA supports three deployment models for firms with "
        "multiple workstations:",
        styles["Body"],
    ))
    story.append(sp())
    deploy_rows = [
        ["Option A\nShared Network\nDrive", "Place .db file on a network\nshare. "
         "Configure database_path\nto \\\\SERVER\\share\\otocpa.db",
         "Simple setup", "SQLite supports only\none writer at a time"],
        ["Option B\nServer + Browser\n(Recommended)", "Run OtoCPA on one server\n"
         "with host: 0.0.0.0. Other\nmachines access via browser.",
         "Single database, no\nsync issues, zero\nclient installation",
         "Server must stay\nonline"],
        ["Option C\nSeparate DBs\nwith Sync", "Each machine has its own\ndatabase. "
         "Sync via Settings\n> Backup > Export/Import.",
         "Fully independent", "Manual sync required;\nrisk of conflicts"],
    ]
    story.append(make_table(
        ["Option", "Setup", "Advantages", "Limitations"],
        deploy_rows,
        col_widths=[1.2 * inch, 2.1 * inch, 1.6 * inch, 1.6 * inch],
    ))
    story.append(sp())
    story.append(tip_box(
        "Option B is recommended for most firms. It requires no installation "
        "on client machines &mdash; any device with a web browser can access "
        "the dashboard at the server's IP address.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("2.6 License Transfer Between Machines", styles["H2"]))
    story.extend(numbered_list([
        "On the old machine: go to Settings &gt; License &gt; Deactivate.",
        "Copy <font face='Courier'>otocpa.config.json</font> to the "
        "new machine via USB or network share.",
        "On the new machine: the license auto-activates on first launch.",
        "If issues persist, contact support@otocpa.com for a server-side "
        "reset of the machine activation.",
    ], styles))
    story.append(sp())

    story.append(Paragraph("2.7 Provisioning New Clients", styles["H2"]))
    story.append(Paragraph(
        "Firm administrators can provision new clients with a single command "
        "that generates a license key, builds an installer ZIP, and "
        "optionally emails the package to the client:",
        styles["Body"],
    ))
    story.append(Paragraph(
        'python scripts/provision_client.py --firm "Acme Inc" '
        '--tier professionnel --months 12 --email client@acme.com '
        '--contact "Jane Doe"',
        styles["Code"],
    ))
    story.append(Paragraph(
        "The script logs all provisioned clients to "
        "<font face='Courier'>clients.csv</font> with license key, tier, "
        "expiry date, and provisioning date for record-keeping.",
        styles["Body"],
    ))


# ── Section 3: User Management ─────────────────────────────────
def build_section_3_user_management(story, styles):
    """Section 3 — Roles, accounts, portfolios, passwords."""
    story.append(Paragraph("3. User Management", styles["H1"]))
    story.append(sp())

    _build_section_3_roles(story, styles)
    _build_section_3_accounts(story, styles)
    _build_section_3_portfolios(story, styles)
    _build_section_3_passwords(story, styles)
    story.append(PageBreak())


def _build_section_3_roles(story, styles):
    """Roles table."""
    story.append(Paragraph("3.1 Roles and Permissions", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA uses four roles with hierarchical permissions. Each "
        "user is assigned exactly one role at creation time.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Owner", "Full system access. Can manage users, view all\n"
         "clients, configure license, run admin tools,\n"
         "access audit module, generate invoices.", "Firm partner or\nprincipal"],
        ["Manager", "View all clients, assign work, approve postings,\n"
         "manage portfolios, access bank import,\n"
         "reconciliation, period close, communications.", "Senior accountant\nor team lead"],
        ["Employee", "View assigned clients only. Can claim and\n"
         "review documents, update fields, place holds.\n"
         "Cannot approve postings or manage team.", "Staff bookkeeper\nor junior"],
        ["Client", "Access client portal only. Can upload documents,\n"
         "view submission history. No dashboard access.\n"
         "Cannot see other clients' data.", "External client\nof the firm"],
    ]
    story.append(make_table(
        ["Role", "Permissions", "Typical User"],
        rows,
        col_widths=[1.0 * inch, 3.5 * inch, 2.0 * inch],
    ))
    story.append(sp())
    story.append(tip_box(
        "Owner-only features include: user management, license activation, "
        "analytics dashboard, audit module, system troubleshooting, "
        "cache management, vendor memory reset, and remote administration.",
        styles,
    ))
    story.append(sp())


def _build_section_3_accounts(story, styles):
    """Creating user accounts."""
    story.append(Paragraph("3.2 Creating User Accounts", styles["H2"]))
    story.append(Paragraph(
        "Only the Owner role can create new dashboard users.",
        styles["Body"],
    ))
    story.extend(numbered_list([
        "Navigate to <b>Users</b> in the admin sidebar.",
        "Click <b>Add User</b>.",
        "Enter: display name, username (unique), role (Manager or Employee).",
        "Set a temporary password (or let the wizard generate one).",
        "The new user appears in the users table. Share the credentials "
        "securely &mdash; the temporary password is shown only once.",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Client accounts are created separately through the client portal "
        "configuration (see Section 6). Client users do not appear in the "
        "dashboard user list.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_3_portfolios(story, styles):
    """Portfolio management."""
    story.append(Paragraph("3.3 Portfolio Management", styles["H2"]))
    story.append(Paragraph(
        "Portfolios control which clients each staff member can see. "
        "Managers and Owners access the portfolio screen from the sidebar.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Assign:</b> Link a client code to an accountant. The accountant "
        "sees that client's documents in their queue.",
        "<b>Remove:</b> Unlink a client from an accountant. Documents remain "
        "but are no longer visible to that staff member.",
        "<b>Move:</b> Transfer a client from one accountant to another in a "
        "single action. Useful for staff transitions.",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Owners and Managers always see all clients regardless of portfolio "
        "assignments. Employees see only their assigned clients.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_3_passwords(story, styles):
    """Password management."""
    story.append(Paragraph("3.4 Password Management", styles["H2"]))
    story.extend(bullet_list([
        "Passwords are hashed with bcrypt (12 rounds by default).",
        "Legacy SHA-256 passwords are auto-upgraded on next login.",
        "Owners can reset any user's password from the Users page.",
        "Users can change their own password from the Change Password page.",
        "Minimum password requirements: 8 characters, at least one "
        "uppercase letter, at least one digit.",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "There is no password recovery mechanism. If the Owner forgets "
        "their password, the database must be manually edited or the "
        "setup wizard re-run.",
        styles,
    ))


# ── Section 4: Daily Workflow ──────────────────────────────────
def build_section_4_daily_workflow(story, styles):
    """Section 4 — Intake, review, fraud, approvals, bank."""
    story.append(Paragraph("4. Daily Workflow", styles["H1"]))
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
    """Document intake methods."""
    story.append(Paragraph("4.1 Document Intake Methods", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA accepts documents from six channels. All channels "
        "feed into the same processing pipeline: OCR extraction, fraud "
        "detection, substance classification, and review queue placement.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Client Portal", "Upload via browser at port 8788.\n"
         "Supports PDF, JPEG, PNG, TIFF, WebP, HEIC.\n"
         "Max 20 MB per file."],
        ["WhatsApp", "Send photo/PDF to the firm's Twilio\n"
         "WhatsApp number. Auto-routed by sender\n"
         "phone number to client code."],
        ["Telegram", "Send document to the firm's Telegram\n"
         "bot. Messages processed via OpenClaw\n"
         "bridge at /ingest/openclaw."],
        ["Email", "Forward invoices to the ingest email.\n"
         "MIME attachments extracted automatically.\n"
         "Ingest service runs on port 8789."],
        ["Folder Watcher", "Drop files into the watched inbox folder\n"
         "(default: C:/OtoCPA/Inbox/). Ideal for\n"
         "USB scanners and cloud sync folders."],
        ["Manual Upload", "Upload directly from the dashboard\n"
         "document queue. Drag-and-drop supported."],
    ]
    story.append(make_table(
        ["Channel", "How It Works"],
        rows,
        col_widths=[1.5 * inch, 5.0 * inch],
    ))
    story.append(sp())
    story.append(tip_box(
        "For USB document scanners, point the scanner's output folder to "
        "the OtoCPA Inbox. Documents are picked up automatically "
        "within seconds.",
        styles,
    ))
    story.append(sp())


def _build_section_4_review_queue(story, styles):
    """Review queue and document statuses."""
    story.append(Paragraph("4.2 Review Queue", styles["H2"]))
    story.append(Paragraph(
        "All ingested documents appear in the review queue on the dashboard "
        "home page. Documents are automatically classified and scored for "
        "confidence.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.2.1 Document Statuses", styles["H3"]))
    rows = [
        ["NeedsReview", "Low confidence, fraud flag, substance flag,\n"
         "or large amount (&ge;$25,000). Requires human review."],
        ["Ready", "High confidence (&ge;0.85), all fields populated,\n"
         "no blocking flags. Can be posted."],
        ["Exception", "Missing critical field (vendor or client code).\n"
         "Must be resolved before processing."],
        ["OnHold", "Manually placed on hold by reviewer with\n"
         "a reason note. Stays until returned to Ready."],
        ["Posted", "Successfully posted to QuickBooks Online.\n"
         "Immutable &mdash; corrections require new entries."],
        ["Ignored", "Excluded from processing. Can be restored."],
    ]
    story.append(make_table(
        ["Status", "Description"],
        rows,
        col_widths=[1.3 * inch, 5.2 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("4.2.2 Queue Filters", styles["H3"]))
    story.extend(bullet_list([
        "<b>Status filter:</b> NeedsReview, Ready, Exception, Posted, "
        "OnHold, Ignored",
        "<b>Search:</b> Keyword search across vendor, amount, notes",
        "<b>Client code:</b> Filter to a single client",
        "<b>Queue mode:</b> All visible, My queue (assigned to me), "
        "Unassigned",
        "<b>Include ignored:</b> Toggle to show or hide ignored documents",
    ], styles))
    story.append(sp())

    story.append(Paragraph("4.2.3 Document Detail View", styles["H3"]))
    story.append(Paragraph(
        "Clicking a document opens the detail view showing: vendor name, "
        "client code, document type, amount, date, GL account, tax code, "
        "category, confidence score, raw OCR result, learning history, "
        "learning suggestions, posting readiness, and vendor memory matches. "
        "The PDF viewer is embedded for side-by-side comparison.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.2.4 Auto-Approval Logic", styles["H3"]))
    story.append(Paragraph(
        "Documents with confidence &ge; 0.85, all required fields, and no "
        "blocking flags are automatically marked as Ready. The following "
        "conditions block auto-approval:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "High or critical fraud flags (confidence capped at 0.60)",
        "Substance flags requiring human review",
        "Large amounts &ge; $25,000 (confidence capped at 0.75)",
        "Large credits &lt; -$5,000 (confidence capped at 0.65)",
        "Invalid date format or zero total amount",
        "Missing required fields: vendor, total, date",
        "Mixed tax invoices (confidence capped at 0.50)",
    ], styles))
    story.append(sp())


def _build_section_4_fraud(story, styles):
    """Fraud detection rules."""
    story.append(Paragraph("4.3 Fraud Detection", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA runs 13 deterministic fraud rules on every document. "
        "No AI is used for detection &mdash; AI only explains flagged items. "
        "Each rule produces a flag with a severity level.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["1", "Vendor Amount Anomaly", "Amount &gt; 2 std dev from vendor mean", "HIGH"],
        ["2", "Vendor Timing Anomaly", "Invoice day-of-month &gt; 14 days from norm", "MEDIUM"],
        ["3", "Duplicate (Same Vendor)", "Same vendor + same amount within 30 days", "HIGH"],
        ["4", "Duplicate (Cross Vendor)", "Different vendor + same amount within 7 days", "MEDIUM"],
        ["5", "Weekend Transaction", "Saturday/Sunday with amount &gt; $200", "LOW"],
        ["6", "Holiday Transaction", "Quebec statutory holiday, amount &gt; $200", "LOW"],
        ["7", "Round Number", "Exact round amount from irregular vendor", "LOW"],
        ["8", "New Vendor Large Amount", "First invoice from vendor over $2,000", "MEDIUM"],
        ["9", "Bank Account Change", "Vendor bank details changed between invoices", "CRITICAL"],
        ["10", "Invoice After Payment", "Invoice date after matching payment date", "HIGH"],
        ["11", "Tax Registration Issue", "Charges GST/QST but historically exempt", "HIGH"],
        ["12", "Category Shift", "Vendor category contradicts 80%+ history", "MEDIUM"],
        ["13", "Payee Mismatch", "Bank payee differs from invoice vendor", "HIGH"],
    ]
    story.append(make_table(
        ["#", "Rule", "Trigger Condition", "Severity"],
        rows,
        col_widths=[0.3 * inch, 1.6 * inch, 3.1 * inch, 1.0 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("4.3.1 Fraud Override Workflow", styles["H3"]))
    story.append(Paragraph(
        "Critical and High severity fraud flags require Manager or Owner "
        "override before posting. The override workflow requires:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Explicit acknowledgment checkbox",
        "Detailed justification (minimum 10 characters)",
        "All overrides are audit-logged with timestamp, username, "
        "document ID, fraud flags, and override reason",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Fraud overrides create a permanent audit trail. Ensure "
        "justifications are thorough and accurate, as they may be "
        "reviewed during external audits.",
        styles,
    ))
    story.append(sp())


def _build_section_4_substance(story, styles):
    """Substance flags."""
    story.append(Paragraph("4.4 Substance Classification", styles["H2"]))
    story.append(Paragraph(
        "The substance engine identifies non-operating items that require "
        "special GL treatment. Detection uses bilingual keyword matching "
        "with AI fallback when confidence is low.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["CapEx", "Equipment, vehicles, computers,\nrenovations, HVAC", "1500\n(Fixed Assets)", "0.70"],
        ["Prepaid", "Insurance, advance rent,\nannual subscriptions", "1300\n(Prepaids)", "0.70"],
        ["Loan", "Borrowing, mortgages,\ncredit lines, capital leases", "2500\n(Long-term)", "0.70"],
        ["Tax Remittance", "GST/QST, payroll deductions,\nCNESST, HSF", "2200-2215\n(Tax liabilities)", "0.70"],
        ["Personal Expense", "Grocery, clothing, Netflix,\nvacation, gym", "5400\n(Personal)", "0.70"],
        ["Shareholder", "Withdrawals, related-party\ntransactions, loans", "2600\n(Shareholder)", "0.70"],
    ]
    story.append(make_table(
        ["Category", "Examples", "GL Suggestion", "Conf. Cap"],
        rows,
        col_widths=[1.2 * inch, 2.0 * inch, 1.3 * inch, 0.9 * inch],
    ))
    story.append(sp())
    story.append(tip_box(
        "When a substance flag is raised, the GL account suggestion changes "
        "automatically. Review the suggested GL account on the document "
        "detail page before approving.",
        styles,
    ))
    story.append(sp())


def _build_section_4_uncertainty(story, styles):
    """Uncertainty reasons."""
    story.append(Paragraph("4.5 Uncertainty Tracking", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA tracks 21+ failure modes with structured uncertainty "
        "reasons. Each reason includes bilingual descriptions and "
        "evidence requirements. Posting decisions are based on confidence:",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["&ge; 0.80 (all fields)", "SAFE_TO_POST", "Auto-approve if no\nunresolved reasons"],
        ["0.60 &ndash; 0.79", "PARTIAL_POST_WITH_FLAGS", "Allowed with manual\nreview and flags"],
        ["&lt; 0.60", "BLOCK_PENDING_REVIEW", "Posting blocked until\nresolution"],
    ]
    story.append(make_table(
        ["Confidence", "Recommendation", "Action"],
        rows,
        col_widths=[1.5 * inch, 2.5 * inch, 2.5 * inch],
    ))
    story.append(sp())


def _build_section_4_approvals(story, styles):
    """Approval and posting workflow."""
    story.append(Paragraph("4.6 Approval and Posting", styles["H2"]))
    story.append(Paragraph(
        "The posting workflow moves documents through four states:",
        styles["Body"],
    ))
    story.extend(numbered_list([
        "<b>Draft</b> &mdash; Initial posting job created from reviewed "
        "document. Tax codes validated against GL account and province.",
        "<b>Ready to Post</b> &mdash; All validations passed. Math "
        "verification confirms subtotal + taxes = total (hallucination guard).",
        "<b>Approved for Posting</b> &mdash; Manager or Owner approves "
        "the posting job. Fraud flag check enforced.",
        "<b>Posted</b> &mdash; Transaction submitted to QuickBooks Online. "
        "Returns posting_id and external_id. Document becomes immutable.",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Failed postings can be retried from the queue. The retry also "
        "re-checks fraud flags to prevent bypass.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_4_bank(story, styles):
    """Bank import and reconciliation."""
    story.append(Paragraph("4.7 Bank Import and Matching", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA imports bank statements in CSV and PDF formats from "
        "major Quebec banks: Desjardins, National Bank (Banque Nationale), "
        "BMO, TD, and RBC. Bank detection is automatic.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.7.1 Smart Matching", styles["H3"]))
    story.append(Paragraph(
        "Each bank transaction is matched against existing invoices using "
        "three criteria:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Vendor similarity:</b> Fuzzy matching &ge; 80% (with accent "
        "stripping and Quebec business suffix handling: inc, ltd, ltee, "
        "enr, senc)",
        "<b>Amount tolerance:</b> Within 2% of invoice amount",
        "<b>Date window:</b> Within 7 days of expected payment date",
    ], styles))
    story.append(sp())

    story.append(Paragraph("4.7.2 Split Payments", styles["H3"]))
    story.append(Paragraph(
        "When a single bank transaction covers multiple invoices, use the "
        "split payment feature. The system detects potential splits and "
        "allows manual allocation across invoices.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.7.3 Reversal Detection", styles["H3"]))
    story.append(Paragraph(
        "The bank matcher detects reversals using: vendor similarity &ge; 80%, "
        "opposite signs or reversal keywords (annulation, reversal, correction), "
        "amount within 1%, and date proximity within 5 business days.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.7.4 Cross-Currency Matching", styles["H3"]))
    story.append(Paragraph(
        "For USD transactions matched against CAD invoices, the bank matcher "
        "applies Bank of Canada FX rates with a 2% tolerance window.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_4_journal(story, styles):
    """Manual journal entries."""
    story.append(Paragraph("4.8 Manual Journal Entries", styles["H2"]))
    story.append(Paragraph(
        "Manual journal entries (MJEs) can be created by Managers and Owners "
        "for adjustments not captured by document processing.",
        styles["Body"],
    ))
    story.append(sp())
    story.extend(bullet_list([
        "<b>Create:</b> Client code, period, date, debit/credit accounts, "
        "amount, description.",
        "<b>Conflict detection:</b> Auto-detects when an MJE conflicts with "
        "an automated posting correction (Trap 7). Conflicting entries are "
        "quarantined for review.",
        "<b>Phantom tax detection:</b> Flags ITC claims without GST/QST "
        "registration. Status set to phantom_tax_blocked.",
        "<b>Post:</b> Move from draft to posted status.",
        "<b>Reverse:</b> Create a reversing entry for a posted or draft MJE.",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Manual journal entries are subject to optimistic locking (Trap 6). "
        "If another user modifies the same document between your read and "
        "your approval, the system rejects the stale version and requires "
        "you to refresh.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("4.9 Learning and Vendor Memory", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA continuously learns from reviewer corrections to "
        "improve future accuracy. The learning system has three components:",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.9.1 Learning History", styles["H3"]))
    story.append(Paragraph(
        "Every correction made during document review is recorded in the "
        "learning history. This includes changes to: vendor name, GL account, "
        "tax code, category, amount, and date. The history is visible on "
        "the document detail page under the \"Learning History\" section.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.9.2 Learning Suggestions", styles["H3"]))
    story.append(Paragraph(
        "Based on past corrections, the system suggests values for new "
        "documents. Suggestions appear on the document detail page and "
        "include confidence scores. Higher confidence (more consistent "
        "past approvals) means more reliable suggestions.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("4.9.3 Vendor Memory", styles["H3"]))
    story.append(Paragraph(
        "Vendor memory stores learned patterns per vendor and client "
        "combination:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Preferred GL account for each vendor",
        "Typical tax code assignment",
        "Expected amount range (for anomaly detection)",
        "Usual invoice timing (day of month)",
        "Category preferences",
        "Confidence increases with each consistent approval",
        "Reset available from Admin &gt; Vendor Memory if needed",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "The first 5 transactions from any vendor require full manual "
        "review. After 5 consistent approvals, the system builds enough "
        "confidence for higher auto-approval scores and more reliable "
        "fraud detection baselines.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("4.10 Document Fields Reference", styles["H2"]))
    story.append(Paragraph(
        "Each document in the review queue contains the following fields, "
        "extracted by AI and editable by reviewers:",
        styles["Body"],
    ))
    story.append(sp())
    field_rows = [
        ["Vendor", "Company or person name from invoice", "Required"],
        ["Client Code", "Accounting client this belongs to", "Required"],
        ["Document Type", "invoice, receipt, credit_note, utility_bill,\n"
         "bank_transaction, credit_card_statement", "Auto-detected"],
        ["Amount", "Total invoice amount (negative for credits)", "Required"],
        ["Document Date", "Invoice or receipt date", "Required"],
        ["GL Account", "General ledger account code", "Suggested by AI"],
        ["Tax Code", "T, Z, E, M, I, HST, HST_ATL, GST_ONLY,\nVAT, NONE",
         "Suggested by AI"],
        ["Category", "Expense category for reporting", "Suggested by AI"],
        ["Review Status", "NeedsReview, Ready, Exception, OnHold,\nPosted, Ignored",
         "Auto-assigned"],
        ["Confidence", "0.00 to 1.00 effective confidence score", "Calculated"],
        ["Fraud Flags", "JSON array of triggered fraud rules", "Auto-detected"],
        ["Substance Flags", "CapEx, prepaid, loan, tax, personal, etc.", "Auto-detected"],
        ["Raw Result", "Complete JSON from OCR extraction", "Read-only"],
    ]
    story.append(make_table(
        ["Field", "Description", "Source"],
        field_rows,
        col_widths=[1.3 * inch, 3.2 * inch, 1.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("4.11 Communications", styles["H2"]))
    story.append(Paragraph(
        "Managers and Owners can draft and send client messages directly "
        "from the dashboard (Manager/Owner). Navigate to <b>Communications</b>.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Draft:</b> AI-generates a context-aware message using vendor, "
        "amount, and client code. Supports French and English.",
        "<b>Edit:</b> Review and modify the draft before sending.",
        "<b>Send:</b> Delivers the message via SMTP to the client's email.",
        "<b>History:</b> View all sent and draft messages with timestamps.",
    ], styles))
    story.append(sp())

    story.append(Paragraph("4.12 Analytics Dashboard", styles["H2"]))
    story.append(Paragraph(
        "The analytics page (Owner only) provides operational insights:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Staff productivity:</b> Documents processed, average review time, "
        "hold rate, approval rate per team member",
        "<b>Client complexity:</b> Document count, approval rate, exception "
        "rate per client",
        "<b>Monthly trends:</b> Volume graphs, status distribution over time",
        "<b>Fraud summary:</b> Incident count by fraud rule type",
        "<b>Deadlines at risk:</b> Upcoming filing dates that may be missed",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Use these metrics to identify bottlenecks, reallocate staff, and "
        "monitor which clients generate the most exceptions.",
        styles["Body"],
    ))


# ── Section 5: Quebec Tax ──────────────────────────────────────
def build_section_5_quebec_tax(story, styles):
    """Section 5 — Full Quebec tax compliance."""
    story.append(Paragraph("5. Quebec Tax Compliance", styles["H1"]))
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
    """GST/QST/HST rates by province."""
    story.append(Paragraph("5.1 Tax Rates by Province", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA calculates taxes using exact Decimal arithmetic "
        "(ROUND_HALF_UP to $0.01). All rates are deterministic with "
        "zero AI involvement.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Quebec (QC)", "5.000%", "9.975%", "&mdash;", "14.975%"],
        ["Ontario (ON)", "&mdash;", "&mdash;", "13.000%", "13.000%"],
        ["New Brunswick (NB)", "&mdash;", "&mdash;", "15.000%", "15.000%"],
        ["Nova Scotia (NS)", "&mdash;", "&mdash;", "15.000%", "15.000%"],
        ["Newfoundland (NL)", "&mdash;", "&mdash;", "15.000%", "15.000%"],
        ["Prince Edward Island (PE)", "&mdash;", "&mdash;", "15.000%", "15.000%"],
        ["British Columbia (BC)", "5.000%", "&mdash;", "&mdash;", "5% + 7% PST"],
        ["Manitoba (MB)", "5.000%", "&mdash;", "&mdash;", "5% + 7% PST"],
        ["Saskatchewan (SK)", "5.000%", "&mdash;", "&mdash;", "5% + 6% PST"],
        ["Alberta (AB)", "5.000%", "&mdash;", "&mdash;", "5.000%"],
        ["Territories (NT/NU/YT)", "5.000%", "&mdash;", "&mdash;", "5.000%"],
    ]
    story.append(make_table(
        ["Province", "GST", "QST", "HST", "Effective Rate"],
        rows,
        col_widths=[1.8 * inch, 0.8 * inch, 0.8 * inch, 0.8 * inch, 1.3 * inch],
    ))
    story.append(sp())
    story.append(tip_box(
        "In Quebec, GST and QST are calculated in parallel on the pre-tax "
        "amount (not cascaded). This is different from the old system where "
        "QST was calculated on GST-inclusive amounts.",
        styles,
    ))
    story.append(sp())


def _build_section_5_tax_codes(story, styles):
    """Tax codes with examples."""
    story.append(Paragraph("5.2 Tax Codes", styles["H2"]))
    story.append(Paragraph(
        "Each document is assigned a tax code that determines tax treatment "
        "and ITC/ITR eligibility:",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["T", "Taxable", "GST 5% + QST 9.975%", "100% / 100%",
         "Office supplies from\nQuebec vendor"],
        ["Z", "Zero-rated", "0% tax", "100% ITC on inputs",
         "Basic groceries,\nprescriptions, exports"],
        ["E", "Exempt", "0% tax", "No ITC",
         "Residential rent,\nfinancial services, health"],
        ["M", "Meals", "GST + QST (50% claimable)", "50% / 50%",
         "Business meals,\nentertainment"],
        ["I", "Insurance", "QC 9% non-recoverable", "No ITC",
         "Property/casualty\ninsurance premiums"],
        ["HST", "HST Ontario", "13%", "100%",
         "Purchase from\nOntario vendor"],
        ["HST_ATL", "HST Atlantic", "15%", "100%",
         "Purchase from\nNB, NS, NL, PE"],
        ["GST_ONLY", "GST Only", "5%", "100%",
         "Purchase from AB,\nNT, NU, YT"],
        ["VAT", "Foreign VAT", "Varies", "Not recoverable",
         "International purchase\nwith foreign tax"],
        ["NONE", "No Tax", "0%", "N/A",
         "Government fees,\nbank charges"],
    ]
    story.append(make_table(
        ["Code", "Name", "Tax Rate", "ITC/ITR", "Example"],
        rows,
        col_widths=[0.6 * inch, 1.0 * inch, 1.4 * inch, 1.2 * inch, 1.6 * inch],
    ))
    story.append(sp())
    story.append(warning_box(
        "Tax code I (Insurance) is Quebec-specific. Insurance premiums in "
        "Quebec carry a 9% provincial tax that is NOT recoverable as an "
        "ITR, unlike regular QST.",
        styles,
    ))
    story.append(sp())


def _build_section_5_itc_itr(story, styles):
    """ITC/ITR explanation."""
    story.append(Paragraph("5.3 Input Tax Credits (ITC) and Input Tax Refunds (ITR)",
                           styles["H2"]))
    story.append(Paragraph(
        "Businesses registered for GST/QST can recover taxes paid on "
        "business expenses through Input Tax Credits (ITC for GST) and "
        "Input Tax Refunds (ITR for QST).",
        styles["Body"],
    ))
    story.append(sp())
    story.extend(bullet_list([
        "<b>Full recovery (T, HST, GST_ONLY):</b> 100% of GST/QST/HST "
        "paid is claimable",
        "<b>Partial recovery (M - Meals):</b> Only 50% of GST and QST "
        "is claimable for meals and entertainment",
        "<b>Zero-rated (Z):</b> No tax charged, but ITC on inputs is "
        "still claimable (e.g., exporter buying supplies)",
        "<b>Exempt (E):</b> No tax charged AND no ITC recovery on inputs",
        "<b>Insurance (I):</b> Quebec 9% premium tax is never recoverable",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "OtoCPA tracks ITC/ITR amounts per document and aggregates them "
        "in the filing summary for GST/QST returns.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_5_filing(story, styles):
    """Filing summary."""
    story.append(Paragraph("5.4 Filing Summary", styles["H2"]))
    story.append(Paragraph(
        "The filing summary aggregates all posted documents for a period "
        "and calculates net GST and QST payable or refundable:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Total GST collected on sales",
        "Total ITC (GST paid on purchases)",
        "Net GST = Collected - ITC",
        "Total QST collected on sales",
        "Total ITR (QST paid on purchases)",
        "Net QST = Collected - ITR",
        "Meal adjustments (50% restriction applied)",
    ], styles))
    story.append(sp())


def _build_section_5_revenu_quebec(story, styles):
    """Revenu Quebec pre-fill."""
    story.append(Paragraph("5.5 Revenu Quebec FPZ-500 Pre-fill", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA generates pre-fill data for the FPZ-500 "
        "(Quebec GST/QST return) based on posted documents for the period. "
        "Navigate to <b>Revenu Quebec</b> in the sidebar (Manager/Owner).",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Select client and period",
        "Review calculated amounts (sales, ITC, ITR, adjustments)",
        "Download PDF summary for reference during online filing",
        "Configure client between standard and Quick Method reporting",
    ], styles))
    story.append(sp())


def _build_section_5_quick_method(story, styles):
    """Quick Method."""
    story.append(Paragraph("5.6 Quick Method of Accounting", styles["H2"]))
    story.append(Paragraph(
        "Small businesses may elect the Quick Method, which simplifies "
        "GST/QST remittance to a percentage of taxable sales (including tax). "
        "OtoCPA supports Quick Method configuration per client.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Configure via <b>Revenu Quebec &gt; Set Config</b>",
        "Select entity type: retail or services",
        "The system applies the Quick Method rate instead of detailed "
        "ITC/ITR calculation",
        "Quick Method clients do not claim individual ITCs",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "The Quick Method is available to businesses with annual taxable "
        "sales (including tax) of $400,000 or less. It reduces bookkeeping "
        "complexity significantly for eligible clients.",
        styles,
    ))
    story.append(sp())


def _build_section_5_deadlines(story, styles):
    """Filing calendar and deadlines."""
    story.append(Paragraph("5.7 Filing Calendar", styles["H2"]))
    story.append(Paragraph(
        "The filing calendar tracks GST/QST filing deadlines by client. "
        "Navigate to <b>Calendar</b> in the sidebar.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Monthly", "Last day of the month\nfollowing the reporting period",
         "Large businesses\n(annual sales &gt; $6M)"],
        ["Quarterly", "Last day of the month\nfollowing the quarter end",
         "Medium businesses\n($1.5M &ndash; $6M annual)"],
        ["Annual", "3 months after\nfiscal year-end",
         "Small businesses\n(&lt; $1.5M annual)"],
    ]
    story.append(make_table(
        ["Frequency", "Deadline", "Typical Filer"],
        rows,
        col_widths=[1.3 * inch, 2.5 * inch, 2.2 * inch],
    ))
    story.append(sp())
    story.extend(bullet_list([
        "Configure per-client frequency and fiscal year-end",
        "Automatic deadline generation based on filing config",
        "Mark as filed with filed_by user and timestamp",
        "Notification alerts 14 days before deadline (configurable)",
    ], styles))
    story.append(sp())


def _build_section_5_payroll(story, styles):
    """Payroll compliance."""
    story.append(Paragraph("5.8 Payroll Compliance (Quebec)", styles["H2"]))
    story.append(Paragraph(
        "The payroll engine validates Quebec-specific payroll deductions "
        "using deterministic rules. All arithmetic uses Python Decimal.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("5.8.1 Payroll Deduction Rates (2024-2025)", styles["H3"]))
    rows = [
        ["QPP (Quebec Pension Plan)", "Employee: 6.40%\nEmployee2: 4.00%",
         "Quebec residents only\n(replaces CPP)"],
        ["CPP (Canada Pension Plan)", "Employee: 5.95%\nEmployee2: 4.00%",
         "All other provinces"],
        ["EI (Employment Insurance)", "Quebec: 1.32%\nOther: 1.66%",
         "Reduced rate for Quebec\n(QPIP offset)"],
        ["QPIP (Parental Insurance)", "Employee: 0.494%\nEmployer: 0.692%",
         "Quebec only"],
    ]
    story.append(make_table(
        ["Deduction", "Rate", "Notes"],
        rows,
        col_widths=[2.0 * inch, 1.8 * inch, 2.2 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("5.8.2 HSF (Health Services Fund) Tiers", styles["H3"]))
    hsf_rows = [
        ["&le; $1,000,000", "1.25%"],
        ["$1M &ndash; $2M", "1.25% &ndash; 1.65% (progressive)"],
        ["$2M &ndash; $3M", "1.65% &ndash; 2.00%"],
        ["$3M &ndash; $5M", "2.00% &ndash; 2.50%"],
        ["$5M &ndash; $7M", "2.50% &ndash; 3.70%"],
        ["&gt; $7,000,000", "4.26%"],
    ]
    story.append(make_table(
        ["Total Payroll", "HSF Rate"],
        hsf_rows,
        col_widths=[2.5 * inch, 2.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("5.8.3 CNESST Premiums", styles["H3"]))
    story.append(Paragraph(
        "CNESST (workplace safety) premiums vary by industry classification "
        "code. Example rates:",
        styles["Body"],
    ))
    cnesst_rows = [
        ["54010", "Office / Professional", "0.54%"],
        ["23010", "Construction", "5.85%"],
        ["52010", "Retail", "1.22%"],
        ["61010", "Transportation", "3.44%"],
        ["62010", "Restaurant / Food Service", "2.10%"],
    ]
    story.append(make_table(
        ["Code", "Industry", "Rate"],
        cnesst_rows,
        col_widths=[1.0 * inch, 3.0 * inch, 1.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("5.8.4 RL-1 / T4 Reconciliation", styles["H3"]))
    story.append(Paragraph(
        "Quebec employers must file both RL-1 (provincial) and T4 (federal) "
        "slips. OtoCPA validates the mapping between boxes:",
        styles["Body"],
    ))
    rl1_rows = [
        ["Box A &mdash; Employment income", "Box 14 &mdash; Employment income"],
        ["Box C &mdash; QPP employee", "Box 16 &mdash; CPP employee"],
        ["Box F &mdash; EI premium", "Box 18 &mdash; EI premium"],
        ["Box H &mdash; QPIP employee", "Box 55 &mdash; PPIP insurable"],
    ]
    story.append(make_table(
        ["RL-1 (Quebec)", "T4 (Federal)"],
        rl1_rows,
        col_widths=[3.0 * inch, 3.0 * inch],
    ))
    story.append(sp())


def _build_section_5_customs(story, styles):
    """Customs and imports."""
    story.append(Paragraph("5.9 Customs and Imports", styles["H2"]))
    story.append(Paragraph(
        "The customs engine handles CBSA (Canada Border Services Agency) "
        "import tax calculations per Customs Act Section 45.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("5.9.1 Customs Value Determination", styles["H3"]))
    story.extend(bullet_list([
        "Discount shown on commercial invoice + unconditional + not "
        "post-import: use discounted price",
        "Conditional discount (volume, loyalty): use undiscounted price",
        "Post-import discount: use undiscounted price",
        "No discount: invoice amount is customs value",
    ], styles))
    story.append(sp())

    story.append(Paragraph("5.9.2 Import Tax Calculation", styles["H3"]))
    story.append(Paragraph(
        "GST on imports: (Customs Value + Duties + Excise) x 5%. "
        "This GST is recoverable as ITC. For Quebec importers, QST is "
        "calculated on: (Customs Value + Duties + GST) x 9.975%.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("5.9.3 FX Rate Validation", styles["H3"]))
    story.append(Paragraph(
        "OtoCPA validates foreign exchange rates against Bank of Canada "
        "daily rates. Manual rates deviating more than 1% from the BoC rate "
        "are flagged for review.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("5.9.4 Double Taxation Prevention", styles["H3"]))
    story.append(Paragraph(
        "The customs engine detects scenarios where both import GST/QST and "
        "domestic GST/QST might be charged, preventing double taxation.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_5_mixed(story, styles):
    """Mixed invoices."""
    story.append(Paragraph("5.10 Mixed Tax Invoices", styles["H2"]))
    story.append(Paragraph(
        "Some invoices contain both taxable and exempt items (e.g., medical "
        "supplies with food). The tax code resolver detects mixed invoices "
        "using bilingual keyword matching with AI fallback.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Strong keyword match: confidence 0.85 (e.g., \"partial exempt\")",
        "Secondary detection: both exempt and taxable indicators present "
        "(confidence 0.65)",
        "AI fallback when keyword confidence &lt; 0.70",
        "Mixed invoices block auto-approval and require manual tax allocation",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Mixed tax invoices require line-by-line tax code assignment. Use "
        "the line item view to assign T, E, or Z codes to individual lines "
        "before posting.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("5.11 Line Item Tax Regime", styles["H2"]))
    story.append(Paragraph(
        "The line item engine extracts individual lines from invoices and "
        "assigns per-line tax regimes based on the place of supply rules "
        "in the Excise Tax Act (ETA Schedule IX).",
        styles["Body"],
    ))
    story.append(sp())
    supply_rows = [
        ["Tangible Goods", "Delivery destination\n(buyer province)", "Shipping address\ndetermines tax"],
        ["Services", "Where predominantly\nperformed", "Service location or\nbuyer province"],
        ["Real Property", "Where property\nis situated", "Always the\nproperty province"],
        ["Intangibles", "Recipient's province\n(buyer)", "Software, licenses,\nsubscriptions"],
        ["Shipping", "Follows principal supply\nor destination", "Part of a\nlarger contract"],
    ]
    story.append(make_table(
        ["Supply Type", "Place of Supply", "Notes"],
        supply_rows,
        col_widths=[1.3 * inch, 2.0 * inch, 2.2 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "The engine detects supply type from keywords: \"shipping\", "
        "\"freight\" = shipping; \"service\", \"labour\", \"installation\" "
        "= service; default = tangible goods.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("5.12 Invoice Reconciliation", styles["H2"]))
    story.append(Paragraph(
        "The reconciliation validator checks that computed line totals "
        "match the invoice total. Tolerance thresholds:",
        styles["Body"],
    ))
    recon_rows = [
        ["&le; $0.02", "Exact match", "Acceptable &mdash; proceed to post"],
        ["&le; $1.00\n(FX)", "FX rounding", "Acceptable for foreign\ncurrency invoices"],
        ["&le; $1.00\n(tax)", "Tax ambiguity", "Flag for review;\ntax-included detection"],
        ["&le; $5.00", "Missing lines", "Check for shipping, handling,\nor environmental fees"],
        ["&le; $50.00", "Vendor markup", "Verify with vendor;\npossible admin fee"],
        ["&gt; $50.00", "Unresolvable", "BLOCK POSTING &mdash;\nmanual review required"],
    ]
    story.append(make_table(
        ["Gap Amount", "Classification", "Action"],
        recon_rows,
        col_widths=[1.2 * inch, 1.5 * inch, 2.8 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("5.13 Tax Calculation Examples", styles["H2"]))
    story.append(Paragraph(
        "The following examples illustrate how OtoCPA calculates taxes "
        "for common scenarios:",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph("Example 1: Standard Quebec Purchase (Tax Code T)",
                           styles["H3"]))
    story.append(Paragraph(
        "Office supplies from a Quebec vendor, pre-tax amount $100.00:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "GST = $100.00 x 5.000% = $5.00",
        "QST = $100.00 x 9.975% = $9.98 (parallel, not on GST amount)",
        "Total = $100.00 + $5.00 + $9.98 = $114.98",
        "ITC claimable: $5.00 (GST)",
        "ITR claimable: $9.98 (QST)",
    ], styles))
    story.append(sp())
    story.append(Paragraph("Example 2: Business Meal (Tax Code M)",
                           styles["H3"]))
    story.append(Paragraph(
        "Restaurant meal for business meeting, pre-tax amount $80.00:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "GST = $80.00 x 5.000% = $4.00",
        "QST = $80.00 x 9.975% = $7.98",
        "Total = $80.00 + $4.00 + $7.98 = $91.98",
        "ITC claimable: $4.00 x 50% = $2.00 (50% restriction)",
        "ITR claimable: $7.98 x 50% = $3.99 (50% restriction)",
    ], styles))
    story.append(sp())
    story.append(Paragraph("Example 3: Ontario Purchase (Tax Code HST)",
                           styles["H3"]))
    story.append(Paragraph(
        "Computer equipment from an Ontario vendor, pre-tax amount $500.00:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "HST = $500.00 x 13.000% = $65.00",
        "Total = $500.00 + $65.00 = $565.00",
        "ITC claimable: $65.00 (full HST recovery)",
    ], styles))
    story.append(sp())
    story.append(Paragraph("Example 4: Import from the US", styles["H3"]))
    story.append(Paragraph(
        "Machinery imported from the US, customs value CAD $10,000, "
        "duties $500, no excise:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "GST base = $10,000 + $500 = $10,500",
        "GST = $10,500 x 5.000% = $525.00 (recoverable as ITC)",
        "QST base = $10,000 + $500 + $525 = $11,025",
        "QST = $11,025 x 9.975% = $1,099.74 (recoverable as ITR)",
        "Total landed cost = $10,000 + $500 + $525 + $1,099.74 = $12,124.74",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "Note that QST on imports is calculated on the customs value PLUS "
        "duties PLUS GST. This is different from domestic purchases where "
        "GST and QST are calculated in parallel on the pre-tax amount.",
        styles,
    ))


# ── Section 6: Client Portal ──────────────────────────────────
def build_section_6_client_portal(story, styles):
    """Section 6 — Portal, Cloudflare, QR, WhatsApp/Telegram."""
    story.append(Paragraph("6. Client Portal", styles["H1"]))
    story.append(sp())

    _build_section_6_overview(story, styles)
    _build_section_6_credentials(story, styles)
    _build_section_6_submission(story, styles)
    _build_section_6_cloudflare(story, styles)
    _build_section_6_qr(story, styles)
    _build_section_6_messaging(story, styles)
    story.append(PageBreak())


def _build_section_6_overview(story, styles):
    """Portal overview."""
    story.append(Paragraph("6.1 Portal Overview", styles["H2"]))
    story.append(Paragraph(
        "The client portal runs on port 8788 and provides a simple upload "
        "interface for clients to submit documents. Clients do not need "
        "OtoCPA installed &mdash; they access the portal via browser.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Supported formats: PDF, JPEG, PNG, TIFF, WebP, HEIC",
        "Maximum upload size: 20 MB (configurable)",
        "Documents are automatically routed to the client's review queue",
        "Clients can view their submission history",
    ], styles))
    story.append(sp())


def _build_section_6_credentials(story, styles):
    """Creating portal credentials."""
    story.append(Paragraph("6.2 Client Accounts", styles["H2"]))
    story.append(Paragraph(
        "Client accounts are created during the setup wizard (Step 17) or "
        "via the Clients page on the dashboard. Each client receives:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Unique client code (up to 10 uppercase characters)",
        "Contact email address",
        "Preferred language (French or English)",
        "Filing frequency (monthly, quarterly, or annual)",
        "Assigned accountant for portfolio management",
    ], styles))
    story.append(sp())


def _build_section_6_submission(story, styles):
    """Document submission methods."""
    story.append(Paragraph("6.3 Document Submission Methods", styles["H2"]))
    rows = [
        ["Browser Upload", "Navigate to portal URL, log in, drag-and-drop files",
         "Most common; no setup\nrequired"],
        ["WhatsApp", "Send photos/PDFs to the firm's Twilio number",
         "Convenient for mobile;\nrequires Twilio setup"],
        ["Telegram", "Send documents to the firm's Telegram bot",
         "Free messaging;\nrequires bot creation"],
        ["Email", "Forward invoices to the ingest email address",
         "Works with any email\nclient; port 8789"],
        ["Folder Drop", "Place files in shared OneDrive/Dropbox folder",
         "Good for batch scanning;\nauto-pickup"],
    ]
    story.append(make_table(
        ["Method", "How To", "Notes"],
        rows,
        col_widths=[1.2 * inch, 3.0 * inch, 2.0 * inch],
    ))
    story.append(sp())


def _build_section_6_cloudflare(story, styles):
    """Cloudflare tunnel setup."""
    story.append(Paragraph("6.4 Cloudflare Tunnel (Remote Access)", styles["H2"]))
    story.append(Paragraph(
        "To make the client portal accessible over the internet (not just "
        "local network), configure a Cloudflare tunnel during the setup "
        "wizard (Step 6).",
        styles["Body"],
    ))
    story.extend(numbered_list([
        "Create a free Cloudflare account at cloudflare.com",
        "Add your domain to Cloudflare DNS",
        "Install cloudflared on the OtoCPA server",
        "Create a tunnel: <font face='Courier'>cloudflared tunnel create "
        "otocpa</font>",
        "Configure the tunnel to point to localhost:8788",
        "Start the tunnel: <font face='Courier'>cloudflared tunnel run "
        "otocpa</font>",
        "Update DNS to point your subdomain to the tunnel",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "Cloudflare tunnels are free and provide HTTPS encryption, DDoS "
        "protection, and global CDN. No port forwarding or static IP is "
        "required. When HTTPS is active, OtoCPA auto-enables secure "
        "cookies.",
        styles,
    ))
    story.append(sp())


def _build_section_6_qr(story, styles):
    """QR codes."""
    story.append(Paragraph("6.5 QR Codes", styles["H2"]))
    story.append(Paragraph(
        "Generate QR codes for each client that link directly to their "
        "upload page. Navigate to <b>QR</b> in the sidebar.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Download individual client QR codes as PNG images",
        "Download all client QR codes as a multi-page PDF",
        "Print and hand to clients for easy portal access",
        "QR codes encode the portal URL with the client code pre-filled",
    ], styles))
    story.append(sp())


def _build_section_6_messaging(story, styles):
    """WhatsApp/Telegram via OpenClaw."""
    story.append(Paragraph("6.6 WhatsApp and Telegram via OpenClaw", styles["H2"]))
    story.append(Paragraph(
        "WhatsApp and Telegram messages are processed through the OpenClaw "
        "bridge. The bridge accepts JSON payloads at "
        "<font face='Courier'>/ingest/openclaw</font> (no authentication "
        "required for the ingest endpoint).",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "WhatsApp uses Twilio as the messaging provider (~$0.005/message)",
        "Telegram uses a custom bot created via @BotFather (free)",
        "Messages are routed to client codes by sender phone number or "
        "Telegram user ID",
        "Unknown senders receive HTTP 404 (unknown_sender)",
        "Successful ingestion returns document_id and status",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Configure WhatsApp in Setup Wizard Step 7 and Telegram in Step 8.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("6.7 Microsoft 365 Integration", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA integrates with Microsoft 365 for email and calendar "
        "automation. Configure in Setup Wizard Step 9.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph("Features:", styles["BodyBold"]))
    story.extend(bullet_list([
        "<b>Auto-read emails:</b> Service account monitors a shared mailbox "
        "for incoming invoices and automatically ingests attachments.",
        "<b>Invoice processing:</b> PDF and image attachments are extracted "
        "and processed through the standard OCR pipeline.",
        "<b>Outlook calendar sync:</b> Filing deadlines and period close "
        "dates synced to a shared Outlook calendar.",
        "<b>Teams digest:</b> Daily summary posted to a Teams channel with "
        "queue status, fraud alerts, and upcoming deadlines.",
    ], styles))
    story.append(sp())
    story.append(Paragraph("Azure AD Setup:", styles["BodyBold"]))
    story.extend(numbered_list([
        "Go to Azure Active Directory &gt; App registrations",
        "Create a new registration for OtoCPA",
        "Grant Mail.Read, Mail.ReadWrite, and Calendars.ReadWrite permissions",
        "Create a client secret (note: secrets expire; set a reminder)",
        "Enter the Tenant ID, Client ID, and Client Secret in Step 9",
        "Test connectivity from the setup wizard",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "The Microsoft 365 service account should be a dedicated mailbox "
        "(e.g., invoices@yourfirm.com), not a personal account. This "
        "ensures continuous operation and avoids MFA issues.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("6.8 QuickBooks Online Integration", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA posts approved transactions to QuickBooks Online. "
        "Configure in Setup Wizard Step 10.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph("Capabilities:", styles["BodyBold"]))
    story.extend(bullet_list([
        "<b>Post transactions:</b> Approved documents create bills, "
        "expenses, or journal entries in QBO.",
        "<b>Sync vendors:</b> Vendor master data is synchronized between "
        "OtoCPA and QBO.",
        "<b>Update accounts:</b> Chart of accounts synced from QBO for "
        "accurate GL mapping.",
        "<b>Verify postings:</b> The QBO verify tool confirms that posted "
        "transactions appear correctly in QBO.",
    ], styles))
    story.append(sp())
    story.append(Paragraph("Setup:", styles["BodyBold"]))
    story.extend(numbered_list([
        "Go to developer.intuit.com and create an app",
        "Set the redirect URI to your OtoCPA URL",
        "Note the Realm ID (Company ID) from QBO",
        "Enter Realm ID, Client ID, and Client Secret in Step 10",
        "Authorize the connection via OAuth flow",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "QBO tokens expire periodically. OtoCPA automatically refreshes "
        "tokens, but if posting fails with an authentication error, "
        "re-authorize from the setup wizard.",
        styles,
    ))


# ── Section 7: Month-End and Billing ──────────────────────────
def build_section_7_monthend(story, styles):
    """Section 7 — Period close, time tracking, invoicing, recon."""
    story.append(Paragraph("7. Month-End and Billing", styles["H1"]))
    story.append(sp())

    _build_section_7_checklist(story, styles)
    _build_section_7_period_lock(story, styles)
    _build_section_7_time_tracking(story, styles)
    _build_section_7_invoicing(story, styles)
    _build_section_7_reconciliation(story, styles)
    story.append(PageBreak())


def _build_section_7_checklist(story, styles):
    """Period close checklist."""
    story.append(Paragraph("7.1 Period Close Checklist", styles["H2"]))
    story.append(Paragraph(
        "Navigate to <b>Period Close</b> in the sidebar (Manager/Owner). "
        "The checklist tracks all month-end tasks:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Review all NeedsReview documents for the period",
        "Complete bank reconciliation",
        "Verify GST/QST filing amounts",
        "Check for outstanding payroll items",
        "Review and post manual journal entries",
        "Verify all document assignments are complete",
        "Generate period close PDF report",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Each checklist item has a responsible user assignment and due date. "
        "Mark items as open or closed as you progress through month-end.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_7_period_lock(story, styles):
    """Period locking."""
    story.append(Paragraph("7.2 Locking Periods", styles["H2"]))
    story.append(Paragraph(
        "Once month-end is complete, lock the period to prevent further "
        "document updates. Locked periods:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Prevent document modifications for the locked date range",
        "Block new postings to the locked period",
        "Trigger a filing snapshot (amendment engine) to preserve the "
        "state at time of filing",
        "Any corrections to locked periods raise amendment flags (Trap 1) "
        "rather than modifying the original filing",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Period locking is irreversible through the UI. Once locked, "
        "corrections must go through the amendment workflow. This ensures "
        "the audit trail is preserved.",
        styles,
    ))
    story.append(sp())


def _build_section_7_time_tracking(story, styles):
    """Time tracking."""
    story.append(Paragraph("7.3 Time Tracking", styles["H2"]))
    story.append(Paragraph(
        "Track billable hours per document or client via the time tracking "
        "feature (Manager/Owner).",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Start timer:</b> POST /time/start with document_id or "
        "client_code (optional)",
        "<b>Stop timer:</b> POST /time/stop with entry_id. Returns "
        "duration in minutes.",
        "Time entries are linked to documents and client codes",
        "Used for invoice generation at month-end",
    ], styles))
    story.append(sp())


def _build_section_7_invoicing(story, styles):
    """Invoice generation."""
    story.append(Paragraph("7.4 Invoice Generation", styles["H2"]))
    story.append(Paragraph(
        "Generate professional invoices from time entries. Navigate to "
        "<b>Invoice</b> in the sidebar.",
        styles["Body"],
    ))
    story.extend(numbered_list([
        "Select client code and billing period (start/end dates)",
        "Enter hourly rate, firm name, client name",
        "Enter GST# and QST# for tax calculation",
        "System calculates: billable hours, subtotal, GST (5%), "
        "QST (9.975%), total",
        "Generate and download invoice PDF",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Invoice naming convention: <font face='Courier'>"
        "invoice_{client}_{start}_{end}_{number}.pdf</font>",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_7_reconciliation(story, styles):
    """Bank reconciliation."""
    story.append(Paragraph("7.5 Bank Reconciliation", styles["H2"]))
    story.append(Paragraph(
        "The bank reconciliation module provides two-sided statement "
        "reconciliation. Navigate to <b>Reconciliation</b> in the sidebar.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("7.5.1 Creating a Reconciliation", styles["H3"]))
    story.extend(numbered_list([
        "Click <b>New Reconciliation</b>",
        "Enter: client code, account name, account number",
        "Enter: period end date, statement balance, GL balance",
        "System auto-populates outstanding items from unmatched documents",
    ], styles))
    story.append(sp())

    story.append(Paragraph("7.5.2 Reconciliation Items", styles["H3"]))
    rows = [
        ["Deposits in Transit", "Amounts in GL but not yet on\n"
         "bank statement", "Added to statement balance"],
        ["Outstanding Cheques", "Amounts on statement but not\n"
         "yet in GL", "Subtracted from statement\nbalance"],
        ["Bank Errors", "Errors on the bank statement", "Adjust statement balance"],
        ["Book Errors", "Errors in GL records", "Adjust GL balance"],
    ]
    story.append(make_table(
        ["Item Type", "Description", "Effect"],
        rows,
        col_widths=[1.5 * inch, 2.5 * inch, 2.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("7.5.3 Reconciliation Formula", styles["H3"]))
    story.append(Paragraph(
        "Adjusted Bank Balance = Statement Balance + Deposits in Transit "
        "- Outstanding Cheques +/- Bank Errors",
        styles["Code"],
    ))
    story.append(Paragraph(
        "Adjusted Book Balance = GL Balance +/- Book Errors",
        styles["Code"],
    ))
    story.append(Paragraph(
        "Both adjusted balances must match within $0.01 to finalize. "
        "Finalized reconciliations are immutable and protected by database "
        "triggers.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Download the reconciliation report as a bilingual PDF from the "
        "detail page.",
        styles["Body"],
    ))


# ── Section 8: CPA Audit Module ───────────────────────────────
def build_section_8_audit(story, styles):
    """Section 8 — Full CAS audit module."""
    story.append(Paragraph("8. CPA Audit Module", styles["H1"]))
    story.append(sp())
    story.append(Paragraph(
        "The audit module is available to Entreprise-tier license holders "
        "and provides comprehensive support for CPA engagements following "
        "Canadian Auditing Standards (CAS). All features are accessible "
        "to Owner and Manager roles.",
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
    """Engagement types and management."""
    story.append(Paragraph("8.1 Engagement Types", styles["H2"]))
    rows = [
        ["Audit", "Full-scope audit per CAS. Provides\nreasonable assurance.",
         "CAS 200-810"],
        ["Review", "Limited assurance engagement.\nInquiry and analytical procedures.",
         "CSRE 2400"],
        ["Compilation", "No assurance. Preparation of\nfinancial statements only.",
         "CSRS 4200"],
    ]
    story.append(make_table(
        ["Type", "Description", "Standards"],
        rows,
        col_widths=[1.2 * inch, 3.3 * inch, 1.5 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Create engagements from <b>Engagements</b> in the sidebar. "
        "Each engagement tracks: client code, period, type, partner, "
        "manager, staff, planned hours, budget, and fee. Going concern "
        "assessment runs automatically on creation.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_working_papers(story, styles):
    """Working papers."""
    story.append(Paragraph("8.2 Working Papers (CAS Documentation)", styles["H2"]))
    story.append(Paragraph(
        "Working papers document the evidence gathered and conclusions "
        "reached during the engagement. Navigate to <b>Working Papers</b>.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Auto-create working papers from the chart of accounts (200+ "
        "Quebec standard accounts)",
        "Each paper tracks: balance per books, balance confirmed, "
        "prepared by, reviewed by, sign-off date",
        "Add items with tick marks: tested, confirmed, exception, "
        "not_applicable",
        "Link documents as evidence to working paper items",
        "Download lead sheet PDFs with bilingual headers and exception "
        "highlighting",
        "Auto-materiality check flags significant accounts",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Signed-off working papers are immutable (P0-2 requirement). "
        "Once signed off, no modifications are allowed. This is enforced "
        "by SQLite database triggers. Sign-offs must be within 24 hours "
        "of the last modification.",
        styles,
    ))
    story.append(sp())


def _build_section_8_materiality(story, styles):
    """Materiality (CAS 320)."""
    story.append(Paragraph("8.3 Materiality (CAS 320)", styles["H2"]))
    story.append(Paragraph(
        "Calculate and document materiality for the engagement.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Pre-tax Income", "5%", "Most common basis for\nfor-profit entities"],
        ["Total Assets", "0.5%", "Useful for asset-heavy\nentities (real estate)"],
        ["Revenue", "2%", "Used for not-for-profit\nor early-stage entities"],
    ]
    story.append(make_table(
        ["Basis", "Rate", "When to Use"],
        rows,
        col_widths=[1.5 * inch, 0.8 * inch, 3.5 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "<b>Performance Materiality</b> = 75% of Planning Materiality. "
        "This lower threshold is used for individual account testing.",
        styles["Body"],
    ))
    story.append(Paragraph(
        "<b>Clearly Trivial Threshold</b> = 5% of Planning Materiality. "
        "Misstatements below this amount are accumulated but not individually "
        "investigated.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_risk(story, styles):
    """Risk assessment (CAS 315)."""
    story.append(Paragraph("8.4 Risk Assessment (CAS 315)", styles["H2"]))
    story.append(Paragraph(
        "The risk matrix evaluates inherent risk and control risk at the "
        "assertion level for each significant account.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("Assertions evaluated:", styles["BodyBold"]))
    story.extend(bullet_list([
        "<b>Completeness</b> &mdash; All transactions recorded",
        "<b>Accuracy</b> &mdash; Amounts correctly stated",
        "<b>Existence</b> &mdash; Assets/liabilities exist at period end",
        "<b>Cutoff</b> &mdash; Transactions in correct period",
        "<b>Classification</b> &mdash; Correct GL account",
        "<b>Rights &amp; Obligations</b> &mdash; Entity has rights to assets",
        "<b>Presentation</b> &mdash; Proper disclosure and classification",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "<b>Significant Risk:</b> Flagged when inherent risk is High AND "
        "control risk is Medium or High. Significant risks require special "
        "audit procedures.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_controls(story, styles):
    """Control testing (CAS 330)."""
    story.append(Paragraph("8.5 Control Testing (CAS 330)", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA includes a library of 15 standard controls for Quebec "
        "CPA firms:",
        styles["Body"],
    ))
    story.append(sp())
    control_rows = [
        ["1", "AP Authorization", "Invoice approval before payment"],
        ["2", "Bank Reconciliation", "Monthly bank statement matching"],
        ["3", "Payroll Authorization", "New hires and rate changes"],
        ["4", "Revenue Completeness", "All sales recorded"],
        ["5", "Physical Inventory", "Annual count and reconciliation"],
        ["6", "Access Controls", "System login and permission review"],
        ["7", "Journal Entry Approval", "MJE review and authorization"],
        ["8", "Vendor Master Changes", "New vendor and changes approval"],
        ["9", "Fixed Asset Additions", "CapEx approval and capitalization"],
        ["10", "Credit Card Reconciliation", "Monthly statement matching"],
        ["11", "GST/QST Remittance", "Tax filing and payment verification"],
        ["12", "RL-1/T4 Reconciliation", "Payroll slip accuracy"],
        ["13", "Bank Signing Authority", "Authorized signatories review"],
        ["14", "Petty Cash", "Count and reconciliation"],
        ["15", "Document Retention", "7-year retention compliance"],
    ]
    story.append(make_table(
        ["#", "Control", "Objective"],
        control_rows,
        col_widths=[0.4 * inch, 2.2 * inch, 3.4 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "Test types: walkthrough, reperformance, observation, inquiry. "
        "Conclusions: effective, ineffective, partially_effective. Track "
        "sample size and exceptions found.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_sampling(story, styles):
    """Statistical sampling (CAS 530)."""
    story.append(Paragraph("8.6 Statistical Sampling (CAS 530)", styles["H2"]))
    story.append(Paragraph(
        "The sampling tool selects items for testing using reproducible "
        "random sampling (seeded by working paper ID). Navigate to "
        "<b>Sampling</b> in the audit sidebar.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Specify client, period, account code, and sample size",
        "Items are randomly selected from the population",
        "Mark each item: tested, exception, not_applicable",
        "Sample results are linked to working papers",
        "Reproducible results (same seed = same sample)",
    ], styles))
    story.append(sp())


def _build_section_8_going_concern(story, styles):
    """Going concern (CAS 570)."""
    story.append(Paragraph("8.7 Going Concern (CAS 570)", styles["H2"]))
    story.append(Paragraph(
        "Going concern assessment runs automatically when an engagement is "
        "created or updated. The system detects indicators such as:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Recurring operating losses",
        "Negative working capital",
        "Inability to pay creditors on time",
        "Loss of key customers or suppliers",
        "Legal or regulatory events threatening viability",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Results are stored in the going_concern_assessments table and "
        "linked to the engagement. Flagged concerns trigger additional "
        "disclosure requirements.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_subsequent(story, styles):
    """Subsequent events (CAS 560)."""
    story.append(Paragraph("8.8 Subsequent Events (CAS 560)", styles["H2"]))
    story.append(Paragraph(
        "The amendment engine tracks events between the period end and "
        "the report date. When a correction is made to a filed period:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "The original filing snapshot is preserved (never rewritten)",
        "An amendment flag is raised for the filed period",
        "Corrections are directed to the correction period",
        "The full amendment timeline is available for audit review",
        "\"What was believed at time T\" queries support event analysis",
    ], styles))
    story.append(sp())


def _build_section_8_rep_letter(story, styles):
    """Management rep letter (CAS 580)."""
    story.append(Paragraph("8.9 Management Representation Letter (CAS 580)",
                           styles["H2"]))
    story.append(Paragraph(
        "Generate bilingual (FR/EN) management representation letters. "
        "Owner-only feature accessible from the audit sidebar.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph("Standard representations include:", styles["BodyBold"]))
    story.extend(numbered_list([
        "Financial statements are fairly presented per applicable standards",
        "All transactions have been recorded",
        "Related party disclosures are complete",
        "Post-balance-sheet events have been disclosed",
        "Fraud involving management or control employees has been disclosed",
        "Minutes and significant agreements are complete",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Letter status progresses: draft &rarr; signed &rarr; refused. "
        "Track management name, title, and signature date.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_related_parties(story, styles):
    """Related parties (CAS 550)."""
    story.append(Paragraph("8.10 Related Parties (CAS 550)", styles["H2"]))
    story.append(Paragraph(
        "Identify and track related parties and their transactions.",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Owner", "Individual shareholder or proprietor"],
        ["Family Member", "Spouse, children, or close relatives of owner"],
        ["Affiliated Company", "Entity under common control or ownership"],
        ["Key Management", "CEO, CFO, or other senior executives"],
        ["Board Member", "Member of the board of directors"],
    ]
    story.append(make_table(
        ["Relationship Type", "Description"],
        rows,
        col_widths=[2.0 * inch, 4.0 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "For each related party transaction, record: amount, measurement "
        "basis (carrying_amount, exchange_amount, cost), and whether "
        "disclosure is required. Generate disclosure text for financial "
        "statement notes.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_opinion(story, styles):
    """Audit opinion (CAS 700)."""
    story.append(Paragraph("8.11 Audit Opinion (CAS 700)", styles["H2"]))
    story.append(Paragraph(
        "Issue the engagement report from the engagement detail page. "
        "The system performs pre-issuance checks:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "All checklist blocking items must be resolved",
        "Working papers must be signed off",
        "Materiality must be documented",
        "Risk matrix must be complete",
        "Management representation letter must be signed",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "The report PDF is generated with engagement details, period, "
        "team assignments, and findings. Status changes to completed.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_assertions(story, styles):
    """Assertion coverage (CAS 500)."""
    story.append(Paragraph("8.12 Assertion Coverage (CAS 500)", styles["H2"]))
    story.append(Paragraph(
        "Working papers track assertion coverage at the account level. "
        "The seven assertions (completeness, accuracy, existence, cutoff, "
        "classification, rights &amp; obligations, presentation) are linked "
        "to working paper items via the Save Assertions action.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph(
        "The assertion coverage view shows which assertions have been "
        "tested for each significant account, helping ensure complete "
        "audit coverage.",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_8_quality(story, styles):
    """Quality control (CSQC 1)."""
    story.append(Paragraph("8.13 Quality Control (CSQC 1)", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA supports quality control through several mechanisms "
        "aligned with CSQC 1 requirements:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Engagement team:</b> Partner, manager, and staff assignments "
        "with segregation of duties",
        "<b>Working paper review:</b> Separate prepared_by and reviewed_by "
        "fields enforce second-person review",
        "<b>Immutability:</b> Signed-off papers cannot be modified, "
        "preserving the audit trail",
        "<b>Audit evidence linking:</b> Three-way matching (PO, invoice, "
        "payment) ensures complete evidence chains",
        "<b>Planned vs. actual hours:</b> Budget monitoring at the "
        "engagement level",
        "<b>7-year retention:</b> Document retention control in the "
        "standard controls library",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "These features support the firm's quality control policies as "
        "required by CSQC 1 and provincial CPA standards.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("8.14 Financial Statements", styles["H2"]))
    story.append(Paragraph(
        "Generate financial statements from posted documents. Navigate to "
        "<b>Financial Statements</b> in the audit sidebar.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Balance Sheet:</b> Assets (1000-1999), Liabilities (2000-2999), "
        "Equity (3000-3999) per Quebec plan comptable",
        "<b>Income Statement:</b> Revenue (4000-4999), Expenses (5000-5999)",
        "<b>Trial Balance:</b> All GL accounts with debit/credit totals",
        "Statements are generated from the trial balance, which aggregates "
        "posted documents by GL account",
        "Download as PDF with bilingual headers",
    ], styles))
    story.append(sp())

    story.append(Paragraph("8.15 Chart of Accounts (Plan Comptable)", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA includes a pre-loaded chart of 200+ accounts following "
        "the Quebec standard plan comptable. Key account ranges:",
        styles["Body"],
    ))
    story.append(sp())
    coa_rows = [
        ["1000-1099", "Cash and equivalents", "Bank accounts, petty cash"],
        ["1100-1199", "Accounts receivable", "Trade receivables, allowances"],
        ["1200-1299", "Inventory", "Goods for resale, WIP"],
        ["1300-1399", "Prepaid expenses", "Insurance, rent, subscriptions"],
        ["1400-1499", "Deposits", "Security deposits, utility deposits"],
        ["1500-1999", "Fixed assets", "Equipment, vehicles, buildings, leasehold"],
        ["2000-2099", "Accounts payable", "Trade payables"],
        ["2100-2199", "Accrued liabilities", "Wages, interest, utilities"],
        ["2200-2299", "Tax liabilities", "GST, QST, source deductions"],
        ["2300-2499", "Current loans", "Line of credit, current portion"],
        ["2500-2999", "Long-term liabilities", "Mortgages, term loans, leases"],
        ["3000-3999", "Equity", "Capital, retained earnings, drawings"],
        ["4000-4999", "Revenue", "Sales, service income, other income"],
        ["5000-5999", "Expenses", "Operating expenses by category"],
    ]
    story.append(make_table(
        ["Range", "Category", "Examples"],
        coa_rows,
        col_widths=[1.0 * inch, 2.0 * inch, 3.0 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "The chart of accounts also includes CRA T2 line mappings and "
        "Revenu Quebec CO-17 expense line mappings for tax return "
        "preparation.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("8.16 Three-Way Matching (Audit Evidence)", styles["H2"]))
    story.append(Paragraph(
        "The audit evidence system supports three-way matching to verify "
        "transaction completeness:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Purchase Order (PO):</b> Authorization to purchase",
        "<b>Invoice:</b> Vendor's bill for goods or services",
        "<b>Payment:</b> Bank transaction or cheque clearing the invoice",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "The system tracks match status: missing (no links), partial "
        "(1-2 documents linked), complete (all three matched). Amount "
        "tolerance is applied &mdash; PO amount must approximately equal "
        "invoice amount must approximately equal payment amount.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("8.17 Correction Chain and Amendment Tracking",
                           styles["H2"]))
    story.append(Paragraph(
        "OtoCPA maintains a correction chain graph that tracks all "
        "document corrections, credit memo decompositions, and duplicate "
        "clusters. This ensures one economic event produces exactly one "
        "correction.",
        styles["Body"],
    ))
    story.append(sp())
    story.append(Paragraph("Key trap protections:", styles["BodyBold"]))
    trap_rows = [
        ["Trap 1", "Filed Period\nAmendment", "Corrections to filed periods raise\n"
         "amendment flags. Original filing\nis preserved (never rewritten)."],
        ["Trap 2", "Credit Memo\nDecomposition", "Credit memos decomposed with\n"
         "confidence levels: explicit (0.95),\nlinked (0.80), unlinked (0.45)."],
        ["Trap 3", "Overlap\nAnomaly", "Detects when new vendor overlaps\n"
         "with original vendor's work scope.\nFlags for review."],
        ["Trap 5", "Duplicate\nClustering", "Groups 3+ variants of same\n"
         "document into one cluster.\nPrevents n-way corrections."],
        ["Trap 6", "Stale Version\nDetection", "Optimistic locking rejects\n"
         "approvals on stale document\nversions. Forces refresh."],
        ["Trap 7", "Manual Journal\nCollision", "Detects when MJE conflicts\n"
         "with automated correction.\nQuarantines colliding entry."],
        ["Trap 8", "Rollback\nProtection", "Explicit audited rollback with\n"
         "reimport block until safe\nto re-process."],
    ]
    story.append(make_table(
        ["Trap", "Name", "Protection"],
        trap_rows,
        col_widths=[0.7 * inch, 1.3 * inch, 3.5 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "The full correction chain is traversable from root document to "
        "leaf, providing complete audit lineage for any transaction.",
        styles["Body"],
    ))


# ── Section 9: Administration ──────────────────────────────────
def build_section_9_administration(story, styles):
    """Section 9 — License, admin tools, backups."""
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
    """License tiers."""
    story.append(Paragraph("9.1 License Tiers", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA licenses are signed with HMAC-SHA256 and encoded in "
        "the LLAI- key format. Four tiers are available:",
        styles["Body"],
    ))
    story.append(sp())
    rows = [
        ["Essentiel", "10", "3",
         "Basic review, basic posting"],
        ["Professionnel", "30", "5",
         "AI router, bank parser, fraud\ndetection, Revenu Quebec,\ntime tracking, month-end"],
        ["Cabinet", "75", "15",
         "Analytics, Microsoft 365,\nfiling calendar, client\ncommunications"],
        ["Entreprise", "Unlimited", "Unlimited",
         "Audit module, financial\nstatements, sampling,\nAPI access"],
    ]
    story.append(make_table(
        ["Tier", "Max Clients", "Max Users", "Features"],
        rows,
        col_widths=[1.2 * inch, 0.9 * inch, 0.9 * inch, 3.0 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "View your license status, expiry date, and machine activations "
        "from <b>License</b> in the admin sidebar (Owner only).",
        styles["Body"],
    ))
    story.append(sp())


def _build_section_9_troubleshoot(story, styles):
    """Troubleshoot page."""
    story.append(Paragraph("9.2 System Diagnostics", styles["H2"]))
    story.append(Paragraph(
        "The troubleshoot page (<b>Troubleshoot</b> in admin sidebar) "
        "displays real-time system status:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Folder watcher status (running/stopped)",
        "OpenClaw bridge status (connected/disconnected)",
        "Cloudflare tunnel status (active/inactive)",
        "Database path and file size",
        "Service uptime and port bindings",
        "AI provider connectivity check",
    ], styles))
    story.append(sp())


def _build_section_9_autofix(story, styles):
    """Autofix script."""
    story.append(Paragraph("9.3 Autofix Script", styles["H2"]))
    story.append(Paragraph(
        "The autofix script automatically detects and repairs common "
        "database issues. Run from the Troubleshoot page or via command "
        "line: <font face='Courier'>python scripts/autofix.py</font>",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Repairs missing database columns (additive migrations)",
        "Fixes corrupted session tokens",
        "Re-creates missing database indexes",
        "Validates foreign key integrity",
        "Reports issues found and actions taken",
    ], styles))
    story.append(sp())


def _build_section_9_backups(story, styles):
    """Backup management."""
    story.append(Paragraph("9.4 Backups", styles["H2"]))
    story.append(Paragraph(
        "Configure backups during setup (Step 13) or from the Troubleshoot "
        "page. Options:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Backup folder:</b> Default C:/OtoCPA/Backups/",
        "<b>Frequency:</b> Daily, Weekly, or Every Login",
        "<b>Retention:</b> Number of backup copies to keep",
        "<b>OneDrive sync:</b> Optional cloud backup",
        "<b>Manual backup:</b> Download database from Troubleshoot page",
    ], styles))
    story.append(sp())
    story.append(tip_box(
        "The SQLite database file contains all documents, users, audit "
        "data, and configuration. A single backup file preserves "
        "everything. Test restores periodically.",
        styles,
    ))
    story.append(sp())


def _build_section_9_vendor_memory(story, styles):
    """Vendor memory management."""
    story.append(Paragraph("9.5 Vendor Memory and Aliases", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA learns from corrections to build vendor memory &mdash; "
        "a pattern database that improves accuracy over time.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Vendor memory:</b> Tracks approved GL accounts, tax codes, "
        "and categories per vendor/client combination",
        "<b>Confidence scores:</b> Memory confidence increases with "
        "consistent approvals",
        "<b>Reset:</b> Clear vendor memory for a specific vendor/client "
        "from Admin &gt; Vendor Memory",
        "<b>Vendor aliases:</b> Map alternative vendor names to canonical "
        "names (e.g., \"Desjardins\" and \"Mouvement Desjardins\")",
    ], styles))
    story.append(sp())


def _build_section_9_cache(story, styles):
    """AI cache."""
    story.append(Paragraph("9.6 AI Cache", styles["H2"]))
    story.append(Paragraph(
        "The AI router caches responses to reduce API costs. View cache "
        "statistics and clear the cache from <b>Admin &gt; Cache</b>.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "View cache hit rate and storage size",
        "Clear cache to force fresh AI responses",
        "Useful after changing AI providers or models",
    ], styles))
    story.append(sp())


def _build_section_9_updates(story, styles):
    """Software updates."""
    story.append(Paragraph("9.7 Software Updates", styles["H2"]))
    story.append(Paragraph(
        "Check for and install updates from <b>Admin &gt; Updates</b>.",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Check:</b> Queries the remote version server",
        "<b>Install:</b> Downloads and applies the update in the "
        "background. Service restarts automatically.",
    ], styles))
    story.append(sp())


def _build_section_9_remote(story, styles):
    """Remote management."""
    story.append(Paragraph("9.8 Remote Management", styles["H2"]))
    story.append(Paragraph(
        "For multi-machine deployments, use the Remote Management page "
        "(Owner only) to manage remote OtoCPA instances:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Restart:</b> Restart the remote OtoCPA service",
        "<b>Backup:</b> Trigger a remote database backup",
        "<b>Update:</b> Push software update to the remote instance",
        "<b>Autofix:</b> Run the autofix script remotely",
    ], styles))
    story.append(sp())

    story.append(Paragraph("9.9 Daily Digest Configuration", styles["H2"]))
    story.append(Paragraph(
        "Configure automated email summaries from Setup Wizard Step 12 or "
        "the config file. The daily digest includes:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Number of new documents received since last digest",
        "Documents awaiting review (NeedsReview count)",
        "Documents on hold with reasons",
        "Fraud alerts requiring attention",
        "Upcoming filing deadlines within 14 days",
        "Staff productivity summary (if Owner)",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "Configure send time, recipient list (comma-separated emails), "
        "and language (FR or EN). Requires SMTP configuration (Step 5).",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("9.10 Notification Configuration", styles["H2"]))
    story.append(Paragraph(
        "Fine-tune which events trigger notifications and through which "
        "channel. Configure in Setup Wizard Step 14.",
        styles["Body"],
    ))
    story.append(sp())
    notif_rows = [
        ["New document received", "Email, Desktop, Both, None"],
        ["Fraud detected", "Email, Desktop, Both, None"],
        ["Document pending > X days", "Email, Desktop, Both, None"],
        ["GST/QST deadline (14 days)", "Email, Desktop, Both, None"],
        ["License expires (30 days)", "Email, Desktop, Both, None"],
        ["System errors", "Email, Desktop, Both, None"],
    ]
    story.append(make_table(
        ["Event", "Channel Options"],
        notif_rows,
        col_widths=[3.0 * inch, 3.0 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("9.11 Security Best Practices", styles["H2"]))
    story.append(Paragraph(
        "Recommended security configuration for production environments:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Session timeout:</b> Set to 1 hour or 4 hours for active "
        "environments. Never disable timeout on shared machines.",
        "<b>Max login attempts:</b> Keep at 5 (default). Increase only "
        "if users frequently mistype passwords.",
        "<b>Lockout duration:</b> 15 minutes (default) balances security "
        "with usability.",
        "<b>Force HTTPS:</b> Always enable when using Cloudflare tunnel. "
        "Ensures cookies have the Secure flag.",
        "<b>Strong passwords:</b> Enforce minimum 8 characters, uppercase, "
        "and digit requirements for all users.",
        "<b>API key rotation:</b> Rotate AI provider API keys quarterly. "
        "Update in the config file and restart the service.",
        "<b>Database backups:</b> Daily frequency with at least 7 copies "
        "retained. Enable OneDrive sync for off-site protection.",
        "<b>Audit log review:</b> Periodically review fraud override logs "
        "and posting approvals for anomalies.",
    ], styles))
    story.append(sp())

    story.append(Paragraph("9.12 Database Maintenance", styles["H2"]))
    story.append(Paragraph(
        "The SQLite database grows over time as documents accumulate. "
        "Maintenance tips:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Monitor database size from the Troubleshoot page",
        "Archive completed periods by exporting data and creating a fresh "
        "database for the new fiscal year",
        "Run VACUUM periodically: <font face='Courier'>sqlite3 "
        "data/otocpa_agent.db \"VACUUM;\"</font>",
        "Ensure adequate disk space (minimum 500 MB free recommended)",
        "SQLite supports databases up to 281 TB; in practice, performance "
        "may degrade above 1-2 GB without optimization",
    ], styles))


# ── Section 10: Troubleshooting ────────────────────────────────
def build_section_10_troubleshooting(story, styles):
    """Section 10 — Common issues and solutions."""
    story.append(Paragraph("10. Troubleshooting", styles["H1"]))
    story.append(sp())

    issues = [
        ("10.1 Login Fails", [
            ("Symptom", "Cannot log in, or HTTP 429 Too Many Requests."),
            ("Cause", "Rate limiting: 5 failed attempts per 15 minutes per IP."),
            ("Fix", "Wait 15 minutes and try again. Verify username and "
             "password. Owner can reset passwords from the Users page. "
             "Check that the service is running on port 8787."),
        ]),
        ("10.2 PDF Viewer Not Displaying", [
            ("Symptom", "Document PDF shows blank or fails to load."),
            ("Cause", "Browser PDF viewer incompatibility or corrupted file."),
            ("Fix", "Try a different browser (Chrome recommended). Download "
             "the PDF and open in a standalone PDF reader. Check that the "
             "file exists in the data directory."),
        ]),
        ("10.3 QuickBooks Online Errors", [
            ("Symptom", "Posting fails with QBO authentication error."),
            ("Cause", "Expired or invalid QBO credentials."),
            ("Fix", "Re-authenticate QBO from Setup Wizard Step 10. Verify "
             "Realm ID, Client ID, and Client Secret. Check that the QBO "
             "app is in production mode (not sandbox)."),
        ]),
        ("10.4 AI Extraction Wrong", [
            ("Symptom", "Vendor name, amount, or date extracted incorrectly."),
            ("Cause", "Low OCR confidence on poor-quality scans or handwriting."),
            ("Fix", "Correct the fields in the document detail view. The "
             "correction feeds vendor memory for future accuracy. For "
             "persistent issues with a vendor, check vendor aliases. "
             "Consider re-scanning at higher resolution."),
        ]),
        ("10.5 Email Ingest Not Working", [
            ("Symptom", "Forwarded emails are not appearing in the queue."),
            ("Cause", "Ingest service not running or SMTP misconfigured."),
            ("Fix", "Verify ingest service is running on port 8789. Check "
             "SMTP settings in config. Test email delivery from the setup "
             "wizard. Review firewall rules for port 8789."),
        ]),
        ("10.6 Cloudflare Tunnel Down", [
            ("Symptom", "Client portal inaccessible from outside the network."),
            ("Cause", "cloudflared service stopped or DNS misconfiguration."),
            ("Fix", "Restart cloudflared: <font face='Courier'>cloudflared "
             "tunnel run otocpa</font>. Verify DNS points to the tunnel. "
             "Check Cloudflare dashboard for tunnel status."),
        ]),
        ("10.7 Substance Flags Wrong", [
            ("Symptom", "Document flagged as CapEx but is actually a repair."),
            ("Cause", "Keyword-based detection triggered on partial match."),
            ("Fix", "Override the substance classification on the document "
             "detail page. The GL account will update accordingly. The "
             "system uses negative keywords (e.g., 'maintenance' negates "
             "CapEx) but edge cases exist."),
        ]),
        ("10.8 Fraud False Positives", [
            ("Symptom", "Legitimate transaction flagged as fraud."),
            ("Cause", "Statistical rules trigger on unusual but valid patterns."),
            ("Fix", "Use the fraud override workflow (Manager/Owner). Provide "
             "detailed justification. The override is audit-logged. Consider "
             "that new vendors with large first invoices will always trigger "
             "Rule 8 until 5+ transactions build history."),
        ]),
        ("10.9 Vendor Memory Reset", [
            ("Symptom", "Incorrect GL suggestions persist after corrections."),
            ("Cause", "Outdated vendor memory from early, incorrect approvals."),
            ("Fix", "Navigate to Admin &gt; Vendor Memory. Select the vendor "
             "and client code. Click Reset. This clears learned patterns and "
             "forces the system to re-learn from future approvals."),
        ]),
        ("10.10 Performance Issues", [
            ("Symptom", "Dashboard loads slowly or times out."),
            ("Cause", "Large database, insufficient RAM, or network issues."),
            ("Fix", "Clear the AI cache (Admin &gt; Cache). Archive old "
             "documents from completed periods. Ensure at least 4 GB RAM. "
             "Check that no other service is competing for port 8787. "
             "Consider the server-only deployment (Option B) for multi-machine."),
        ]),
    ]

    for heading, items in issues:
        story.append(Paragraph(heading, styles["H2"]))
        for label, text in items:
            story.append(Paragraph(f"<b>{label}:</b> {text}", styles["Body"]))
        story.append(sp())

    story.append(Paragraph("10.11 Error Codes Reference", styles["H2"]))
    story.append(Paragraph(
        "Common HTTP error codes returned by OtoCPA and their meaning:",
        styles["Body"],
    ))
    story.append(sp())
    err_rows = [
        ["200", "OK", "Request successful"],
        ["302", "Redirect", "After form submission, redirects to\nnext page"],
        ["400", "Bad Request", "Invalid input: missing required field,\n"
         "invalid JSON, or malformed data"],
        ["401", "Unauthorized", "Session expired or not logged in.\n"
         "Redirects to login page."],
        ["403", "Forbidden", "Insufficient role permissions.\n"
         "Owner/Manager feature accessed by Employee."],
        ["404", "Not Found", "Document ID does not exist or\n"
         "unknown sender in OpenClaw ingest."],
        ["429", "Too Many Requests", "Rate limit exceeded. Wait 15 minutes\n"
         "before retrying login."],
        ["500", "Server Error", "Unexpected error. Check install.log\n"
         "and restart the service."],
    ]
    story.append(make_table(
        ["Code", "Status", "Description"],
        err_rows,
        col_widths=[0.6 * inch, 1.4 * inch, 4.0 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("10.12 Database Recovery", styles["H2"]))
    story.append(Paragraph(
        "If the SQLite database becomes corrupted, follow these steps:",
        styles["Body"],
    ))
    story.extend(numbered_list([
        "Stop the OtoCPA service.",
        "Locate the latest backup in C:/OtoCPA/Backups/ (or your "
        "configured backup folder).",
        "Copy the backup file to <font face='Courier'>"
        "data/otocpa_agent.db</font>, replacing the corrupted file.",
        "Run <font face='Courier'>python scripts/migrate_db.py</font> to "
        "apply any pending migrations.",
        "Run <font face='Courier'>python scripts/autofix.py</font> to "
        "verify integrity.",
        "Restart the service.",
    ], styles))
    story.append(sp())
    story.append(warning_box(
        "Data entered since the last backup will be lost. Increase backup "
        "frequency if data loss is a concern. Consider daily backups with "
        "OneDrive sync for maximum protection.",
        styles,
    ))
    story.append(sp())

    story.append(Paragraph("10.13 Port Conflicts", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA uses three ports by default. If a port is already in "
        "use by another application:",
        styles["Body"],
    ))
    port_rows = [
        ["8787", "Dashboard", "Change 'port' in config"],
        ["8788", "Client Portal", "Change 'client_portal.port' in config"],
        ["8789", "Ingest Service", "Change 'ingest.port' in config"],
        ["8790", "Setup Wizard", "Only runs during initial setup"],
    ]
    story.append(make_table(
        ["Port", "Service", "How to Change"],
        port_rows,
        col_widths=[0.8 * inch, 1.5 * inch, 3.5 * inch],
    ))
    story.append(sp())
    story.append(Paragraph(
        "On Windows, check port usage with: <font face='Courier'>"
        "netstat -ano | findstr :8787</font>. Kill the conflicting "
        "process or change the OtoCPA port in the config file.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("10.14 Upgrading Python", styles["H2"]))
    story.append(Paragraph(
        "OtoCPA requires Python 3.11 or higher. To upgrade:",
        styles["Body"],
    ))
    story.extend(numbered_list([
        "Download the latest Python 3.11+ from python.org.",
        "Install with \"Add Python to PATH\" checked.",
        "Stop the OtoCPA service.",
        "Run: <font face='Courier'>pip install -r requirements.txt</font>",
        "Restart the service.",
        "Verify: <font face='Courier'>python --version</font>",
    ], styles))
    story.append(sp())

    story.append(PageBreak())


# ── Section 11: Glossary ───────────────────────────────────────
def build_section_11_glossary(story, styles):
    """Section 11 — 40+ terms FR/EN side by side."""
    story.append(Paragraph("11. Glossary", styles["H1"]))
    story.append(sp())
    story.append(Paragraph(
        "OtoCPA is fully bilingual. This glossary provides French and "
        "English equivalents for key terms used throughout the application.",
        styles["Body"],
    ))
    story.append(sp())

    _build_glossary_general(story, styles)
    _build_glossary_tax(story, styles)
    _build_glossary_audit(story, styles)
    _build_glossary_payroll(story, styles)


def _build_glossary_general(story, styles):
    """General accounting terms."""
    story.append(Paragraph("11.1 General Accounting Terms", styles["H2"]))
    rows = [
        ["Accounts Payable", "Comptes fournisseurs",
         "Amounts owed to vendors"],
        ["Accounts Receivable", "Comptes clients",
         "Amounts owed by customers"],
        ["Balance Sheet", "Bilan",
         "Statement of financial position"],
        ["Bank Reconciliation", "Rapprochement bancaire",
         "Matching bank and GL records"],
        ["Chart of Accounts", "Plan comptable",
         "List of GL accounts"],
        ["Credit Note", "Note de credit",
         "Reversal or reduction of invoice"],
        ["Fiscal Year-End", "Fin d'exercice",
         "End of accounting period"],
        ["General Ledger", "Grand livre",
         "Main accounting record"],
        ["Income Statement", "Etat des resultats",
         "Profit and loss statement"],
        ["Invoice", "Facture",
         "Bill for goods or services"],
        ["Journal Entry", "Ecriture de journal",
         "Manual accounting entry"],
        ["Posting", "Inscription",
         "Recording to GL/QBO"],
        ["Receipt", "Recu",
         "Proof of payment"],
        ["Trial Balance", "Balance de verification",
         "TB for all accounts"],
        ["Vendor", "Fournisseur",
         "Supplier of goods/services"],
    ]
    story.append(make_table(
        ["English", "French", "Definition"],
        rows,
        col_widths=[1.8 * inch, 2.0 * inch, 2.5 * inch],
    ))
    story.append(sp())


def _build_glossary_tax(story, styles):
    """Tax and compliance terms."""
    story.append(Paragraph("11.2 Tax and Compliance Terms", styles["H2"]))
    rows = [
        ["GST", "TPS (Taxe sur les produits\net services)",
         "Federal 5% goods and\nservices tax"],
        ["QST", "TVQ (Taxe de vente\ndu Quebec)",
         "Quebec 9.975% provincial\nsales tax"],
        ["HST", "TVH (Taxe de vente\nharmonisee)",
         "Harmonized sales tax\n(ON, Atlantic)"],
        ["ITC", "CTI (Credit de taxe\nsur les intrants)",
         "GST/HST recovery on\nbusiness purchases"],
        ["ITR", "RTI (Remboursement\nde la taxe sur les intrants)",
         "QST recovery on\nbusiness purchases"],
        ["Quick Method", "Methode rapide",
         "Simplified GST/QST\ncalculation for small business"],
        ["FPZ-500", "FPZ-500",
         "Quebec GST/QST\nreturn form"],
        ["Exempt Supply", "Fourniture exoneree",
         "Supply with no tax\nand no ITC"],
        ["Zero-rated Supply", "Fourniture detaxee",
         "Supply at 0% tax\nbut ITC claimable"],
        ["Mixed Supply", "Fourniture mixte",
         "Invoice with both taxable\nand exempt items"],
        ["Place of Supply", "Lieu de fourniture",
         "Province determining\ntax regime"],
        ["Customs Value", "Valeur en douane",
         "CBSA import\nvaluation basis"],
    ]
    story.append(make_table(
        ["English", "French", "Definition"],
        rows,
        col_widths=[1.8 * inch, 2.0 * inch, 2.5 * inch],
    ))
    story.append(sp())


def _build_glossary_audit(story, styles):
    """Audit terms."""
    story.append(Paragraph("11.3 Audit and CAS Terms", styles["H2"]))
    rows = [
        ["CAS", "NCA (Normes canadiennes\nd'audit)",
         "Canadian Auditing\nStandards"],
        ["Materiality", "Importance relative",
         "CAS 320 threshold for\nsignificant misstatement"],
        ["Engagement", "Mission",
         "Audit/review/compilation\nassignment"],
        ["Working Paper", "Dossier de travail",
         "Audit documentation\nfor each account"],
        ["Lead Sheet", "Feuille sommaire",
         "Summary working paper\nfor an account group"],
        ["Tick Mark", "Coche de verification",
         "Symbol indicating\ntest performed"],
        ["Assertion", "Assertion",
         "Management claim about\nfinancial statements"],
        ["Risk Assessment", "Evaluation des risques",
         "CAS 315 risk\nidentification"],
        ["Control Test", "Test de controle",
         "CAS 330 testing of\ninternal controls"],
        ["Sampling", "Echantillonnage",
         "CAS 530 statistical\nsample selection"],
        ["Going Concern", "Continuite d'exploitation",
         "CAS 570 viability\nassessment"],
        ["Rep Letter", "Lettre de declaration",
         "CAS 580 management\nrepresentation letter"],
        ["Related Party", "Partie liee",
         "CAS 550 connected\nentity or individual"],
        ["Audit Opinion", "Opinion d'audit",
         "CAS 700 auditor's\nconclusion"],
        ["CSQC 1", "NCCQ 1",
         "Quality control standard\nfor CPA firms"],
    ]
    story.append(make_table(
        ["English", "French", "Definition"],
        rows,
        col_widths=[1.8 * inch, 2.0 * inch, 2.5 * inch],
    ))
    story.append(sp())


def _build_glossary_payroll(story, styles):
    """Payroll and Quebec-specific terms."""
    story.append(Paragraph("11.4 Payroll and Quebec-Specific Terms", styles["H2"]))
    rows = [
        ["QPP", "RRQ (Regime de rentes\ndu Quebec)",
         "Quebec Pension Plan"],
        ["CPP", "RPC (Regime de pensions\ndu Canada)",
         "Canada Pension Plan\n(outside Quebec)"],
        ["EI", "AE (Assurance-emploi)",
         "Employment Insurance"],
        ["QPIP", "RQAP (Regime quebecois\nd'assurance parentale)",
         "Quebec Parental\nInsurance Plan"],
        ["HSF", "FSS (Fonds des services\nde sante)",
         "Health Services Fund\n(employer contribution)"],
        ["CNESST", "CNESST",
         "Workplace health and\nsafety commission"],
        ["RL-1", "Releve 1",
         "Quebec employment\nincome slip"],
        ["T4", "T4",
         "Federal employment\nincome slip"],
        ["DAS", "DAS (Deductions a\nla source)",
         "Source deductions\n(payroll remittance)"],
        ["Revenu Quebec", "Revenu Quebec",
         "Quebec provincial\nrevenue agency"],
        ["CRA", "ARC (Agence du revenu\ndu Canada)",
         "Canada Revenue Agency"],
    ]
    story.append(make_table(
        ["English", "French", "Definition"],
        rows,
        col_widths=[1.8 * inch, 2.0 * inch, 2.5 * inch],
    ))
    story.append(sp())

    story.append(sp())

    story.append(Paragraph("11.5 System and Technical Terms", styles["H2"]))
    rows = [
        ["OCR", "ROC (Reconnaissance\noptique de caracteres)",
         "Optical character\nrecognition from images"],
        ["API", "API (Interface de\nprogrammation)",
         "Application programming\ninterface"],
        ["SMTP", "SMTP",
         "Email sending protocol"],
        ["Cloudflare Tunnel", "Tunnel Cloudflare",
         "Secure remote access\nwithout port forwarding"],
        ["SQLite", "SQLite",
         "Embedded database\nengine"],
        ["OpenClaw", "OpenClaw",
         "WhatsApp/Telegram\nmessaging bridge"],
        ["Folder Watcher", "Surveillance de dossier",
         "Auto-detect new files\nin inbox folder"],
        ["AI Router", "Routeur IA",
         "Routes tasks to budget\nor premium AI provider"],
        ["Vendor Memory", "Memoire fournisseur",
         "Learned patterns for\neach vendor"],
        ["Auto-Approval", "Approbation automatique",
         "Documents approved\nwithout human review"],
        ["Confidence Score", "Score de confiance",
         "0.00-1.00 extraction\nreliability measure"],
        ["Optimistic Locking", "Verrouillage optimiste",
         "Version check prevents\nstale data writes"],
        ["Amendment Flag", "Drapeau de modification",
         "Marks filed period\nneeding correction"],
        ["Filing Snapshot", "Instantane de production",
         "Frozen state at time\nof tax filing"],
    ]
    story.append(make_table(
        ["English", "French", "Definition"],
        rows,
        col_widths=[1.8 * inch, 2.0 * inch, 2.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph(
        "This glossary covers the primary terms used in OtoCPA. "
        "The application interface displays all terms in both languages "
        "and can be toggled with one click from any page.",
        styles["Body"],
    ))
    story.append(sp())

    # ── Appendix: Keyboard Shortcuts and Quick Reference ──
    story.append(PageBreak())
    story.append(Paragraph("Appendix A: Quick Reference Card", styles["H1"]))
    story.append(sp())
    story.append(Paragraph(
        "This quick reference card summarises the most common operations "
        "in OtoCPA for everyday use.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph("A.1 Dashboard URLs", styles["H2"]))
    url_rows = [
        ["Main Dashboard", "http://127.0.0.1:8787/"],
        ["Client Portal", "http://127.0.0.1:8788/"],
        ["Setup Wizard", "http://127.0.0.1:8790/"],
        ["Login Page", "http://127.0.0.1:8787/login"],
        ["Analytics", "http://127.0.0.1:8787/analytics"],
        ["Reconciliation", "http://127.0.0.1:8787/reconciliation"],
        ["Filing Calendar", "http://127.0.0.1:8787/calendar"],
        ["Working Papers", "http://127.0.0.1:8787/working_papers"],
        ["Engagements", "http://127.0.0.1:8787/engagements"],
        ["Troubleshoot", "http://127.0.0.1:8787/troubleshoot"],
    ]
    story.append(make_table(
        ["Feature", "URL"],
        url_rows,
        col_widths=[2.0 * inch, 4.0 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("A.2 Document Workflow Quick Guide", styles["H2"]))
    story.extend(numbered_list([
        "Document arrives via any intake channel (portal, email, WhatsApp, "
        "Telegram, folder watcher, or manual upload).",
        "OCR engine extracts data: vendor, amount, date, tax code, GL account.",
        "Fraud engine runs 13 deterministic rules on the document.",
        "Substance engine classifies non-operating items (CapEx, prepaids, etc.).",
        "Review policy calculates effective confidence score.",
        "If confidence &ge; 0.85 and no blocks: status = Ready (auto-approved).",
        "If confidence &lt; 0.85 or blocks exist: status = NeedsReview.",
        "Reviewer verifies and corrects fields on the document detail page.",
        "Reviewer changes status to Ready (or OnHold with reason).",
        "Manager/Owner builds a posting job from Ready documents.",
        "Math verification confirms subtotal + taxes = total.",
        "Fraud flag check runs again at approval time.",
        "Manager/Owner approves the posting job.",
        "Transaction posted to QuickBooks Online. Status = Posted.",
        "Corrections feed vendor memory for future accuracy improvement.",
    ], styles))
    story.append(sp())

    story.append(Paragraph("A.3 Tax Code Quick Reference", styles["H2"]))
    tax_quick = [
        ["T", "Standard Quebec", "GST 5% + QST 9.975%", "Full ITC + ITR"],
        ["Z", "Zero-rated", "0%", "ITC on inputs"],
        ["E", "Exempt", "0%", "No ITC"],
        ["M", "Meals (50%)", "GST + QST", "50% ITC + ITR"],
        ["I", "Insurance (QC)", "9% non-recoverable", "No ITC"],
        ["HST", "Ontario", "13%", "Full ITC"],
        ["HST_ATL", "Atlantic", "15%", "Full ITC"],
        ["GST_ONLY", "AB/Territories", "5%", "Full ITC"],
        ["VAT", "Foreign", "Varies", "No recovery"],
        ["NONE", "No tax", "0%", "N/A"],
    ]
    story.append(make_table(
        ["Code", "Usage", "Rate", "Recovery"],
        tax_quick,
        col_widths=[0.8 * inch, 1.5 * inch, 1.8 * inch, 1.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("A.4 Fraud Rule Quick Reference", styles["H2"]))
    fraud_quick = [
        ["1-2", "Vendor anomalies", "Amount/timing deviation from history", "Requires 5+ prior transactions"],
        ["3-4", "Duplicates", "Same/cross-vendor duplicate amounts", "30-day / 7-day windows"],
        ["5-6", "Weekend/Holiday", "Transactions on non-business days", "Amount &gt; $200 threshold"],
        ["7", "Round number", "Perfectly round amount", "From irregular vendor"],
        ["8", "New vendor large", "First invoice over $2,000", "Clears after 5 transactions"],
        ["9", "Bank change", "Payment details changed", "CRITICAL severity"],
        ["10", "After payment", "Invoice dated after payment", "HIGH severity"],
        ["11", "Tax contradiction", "GST/QST from exempt vendor", "HIGH severity"],
        ["12-13", "Category/Payee", "Historical pattern mismatch", "MEDIUM/HIGH severity"],
    ]
    story.append(make_table(
        ["Rules", "Name", "Trigger", "Notes"],
        fraud_quick,
        col_widths=[0.6 * inch, 1.3 * inch, 2.4 * inch, 2.0 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("A.5 CAS Standards Quick Reference", styles["H2"]))
    cas_quick = [
        ["CAS 315", "Risk Assessment", "Inherent + control risk per assertion"],
        ["CAS 320", "Materiality", "Planning, performance, clearly trivial"],
        ["CAS 330", "Control Testing", "15 standard controls, 4 test types"],
        ["CAS 500", "Assertion Coverage", "7 assertions per account"],
        ["CAS 530", "Sampling", "Statistical sampling with seed"],
        ["CAS 550", "Related Parties", "5 relationship types, RPT tracking"],
        ["CAS 560", "Subsequent Events", "Amendment timeline tracking"],
        ["CAS 570", "Going Concern", "Auto-detection on engagement create"],
        ["CAS 580", "Rep Letter", "Bilingual template, 6 representations"],
        ["CAS 700", "Audit Opinion", "Pre-issuance checklist verification"],
        ["CSQC 1", "Quality Control", "Team assignment, review, immutability"],
    ]
    story.append(make_table(
        ["Standard", "Topic", "OtoCPA Implementation"],
        cas_quick,
        col_widths=[0.9 * inch, 1.5 * inch, 3.5 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("A.6 License Tier Comparison", styles["H2"]))
    tier_rows = [
        ["Feature", "Essentiel", "Professionnel", "Cabinet", "Entreprise"],
        ["Max Clients", "10", "30", "75", "Unlimited"],
        ["Max Users", "3", "5", "15", "Unlimited"],
        ["Basic Review", "Yes", "Yes", "Yes", "Yes"],
        ["AI Router", "No", "Yes", "Yes", "Yes"],
        ["Fraud Detection", "No", "Yes", "Yes", "Yes"],
        ["Bank Parser", "No", "Yes", "Yes", "Yes"],
        ["Revenu Quebec", "No", "Yes", "Yes", "Yes"],
        ["Time Tracking", "No", "Yes", "Yes", "Yes"],
        ["Analytics", "No", "No", "Yes", "Yes"],
        ["Microsoft 365", "No", "No", "Yes", "Yes"],
        ["Filing Calendar", "No", "No", "Yes", "Yes"],
        ["Communications", "No", "No", "Yes", "Yes"],
        ["Audit Module", "No", "No", "No", "Yes"],
        ["Financial Statements", "No", "No", "No", "Yes"],
        ["Sampling", "No", "No", "No", "Yes"],
        ["API Access", "No", "No", "No", "Yes"],
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

    story.append(Paragraph("A.7 Substance Flag Quick Reference", styles["H2"]))
    sub_quick = [
        ["CapEx", "Equipment, vehicles,\ncomputers, renovations",
         "GL 1500", "\"Maintenance\" or \"repair\"\nnegates CapEx detection"],
        ["Prepaid", "Insurance, advance rent,\nannual subscriptions",
         "GL 1300", "\"Assurance qualite\" is NOT\ninsurance (false positive)"],
        ["Loan", "Mortgages, credit lines,\ncapital leases",
         "GL 2500", "\"Pret-a-porter\" is NOT\na loan (false positive)"],
        ["Tax Remittance", "GST/QST, payroll\ndeductions, CNESST",
         "GL 2200-2215", "Source deductions and\ngovernment remittances"],
        ["Personal", "Grocery, clothing,\nNetflix, gym, vacation",
         "GL 5400", "\"Personnel RH\" is HR\nstaffing, not personal"],
        ["Shareholder", "Withdrawals, related-party\nloans, dividends",
         "GL 2600", "Shareholder transactions\nrequire CAS 550 review"],
    ]
    story.append(make_table(
        ["Category", "Examples", "GL", "False Positive Notes"],
        sub_quick,
        col_widths=[1.0 * inch, 1.6 * inch, 0.8 * inch, 2.4 * inch],
    ))
    story.append(sp())

    story.append(Paragraph("A.8 Reconciliation Checklist", styles["H2"]))
    story.append(Paragraph(
        "Use this checklist before finalizing a bank reconciliation:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "Statement balance matches the bank statement PDF exactly",
        "GL balance matches the trial balance for the account",
        "All deposits in transit have been verified against deposit slips",
        "All outstanding cheques have been confirmed as not yet cleared",
        "Bank errors have supporting documentation",
        "Book errors have been corrected with journal entries",
        "Adjusted bank balance = Adjusted book balance (within $0.01)",
        "Reconciliation downloaded as PDF for the audit file",
    ], styles))
    story.append(sp())

    story.append(Paragraph("A.9 Support and Contact Information", styles["H2"]))
    story.append(Paragraph(
        "If you encounter issues not covered in this manual, the following "
        "resources are available:",
        styles["Body"],
    ))
    story.extend(bullet_list([
        "<b>Email support:</b> support@otocpa.com",
        "<b>Installation guide:</b> docs/README_INSTALL.txt (included in "
        "installation package)",
        "<b>Second machine guide:</b> docs/SECOND_MACHINE_INSTALL.md",
        "<b>Autofix tool:</b> Run <font face='Courier'>python scripts/"
        "autofix.py</font> for automatic diagnostics and repair",
        "<b>Troubleshoot page:</b> Access from the admin sidebar for "
        "real-time system status",
        "<b>License issues:</b> Contact support@otocpa.com for "
        "license transfers and activation resets",
    ], styles))
    story.append(sp())
    story.append(Paragraph(
        "When contacting support, please include: your license tier, "
        "the error message or screenshot, the steps to reproduce the issue, "
        "and the contents of C:\\OtoCPA\\install.log if applicable.",
        styles["Body"],
    ))
    story.append(sp())

    story.append(Paragraph(
        "&mdash; End of OtoCPA User Manual &mdash;",
        ParagraphStyle("EndMark", parent=styles["Body"],
                       alignment=TA_CENTER, textColor=BLUE,
                       fontSize=11, spaceBefore=24),
    ))


# ═══════════════════════════════════════════════════════════════
#  PAGE TEMPLATE AND MAIN
# ═══════════════════════════════════════════════════════════════

def _on_page(canvas, doc):
    """Draw header line and page number on each page."""
    canvas.saveState()
    # Header line
    canvas.setStrokeColor(BLUE)
    canvas.setLineWidth(1)
    canvas.line(
        0.75 * inch, letter[1] - 0.6 * inch,
        letter[0] - 0.75 * inch, letter[1] - 0.6 * inch,
    )
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(BLUE)
    canvas.drawString(0.75 * inch, letter[1] - 0.55 * inch,
                      "OtoCPA User Manual")
    # Page number
    canvas.setFillColor(DARK_GREY)
    canvas.drawCentredString(
        letter[0] / 2, 0.5 * inch,
        f"Page {doc.page}",
    )
    canvas.restoreState()


def _on_first_page(canvas, doc):
    """Cover page — no header/footer."""
    pass


def main():
    """Build the complete PDF manual."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(
        str(OUT_PATH),
        pagesize=letter,
        topMargin=0.85 * inch,
        bottomMargin=0.85 * inch,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        title="OtoCPA User Manual",
        author="OtoCPA",
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
    print(f"PDF generated: {OUT_PATH}")
    print(f"File size: {OUT_PATH.stat().st_size / 1024:.0f} KB")

    # Report page count
    try:
        from reportlab.lib.utils import open_for_read
        import re
        with open(str(OUT_PATH), "rb") as f:
            data = f.read()
        pages = len(re.findall(rb"/Type\s*/Page[^s]", data))
        print(f"Page count: {pages}")
    except Exception:
        print("Page count: check PDF viewer")


if __name__ == "__main__":
    main()
