"""
LedgerLink AI — User Manual Generator (English)
Generates docs/LedgerLink_User_Manual_EN.pdf using ReportLab.
Run: python scripts/generate_manual_en.py
"""

import os
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, KeepTogether
)
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY

# ── Brand colours ──────────────────────────────────────────────────────────────
BLUE   = colors.HexColor("#1F3864")
AMBER  = colors.HexColor("#FFF3CD")
AMBER_BORDER = colors.HexColor("#FFC107")
TEAL   = colors.HexColor("#D1ECF1")
TEAL_BORDER  = colors.HexColor("#17A2B8")
LIGHT_GREY   = colors.HexColor("#F5F5F5")
MID_GREY     = colors.HexColor("#CCCCCC")
WHITE  = colors.white
BLACK  = colors.black

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "docs", "LedgerLink_User_Manual_EN.pdf")


# ── Style factory ──────────────────────────────────────────────────────────────
def make_styles():
    base = getSampleStyleSheet()

    styles = {}

    styles["Normal"] = ParagraphStyle(
        "Normal", parent=base["Normal"],
        fontSize=10, leading=15, spaceAfter=6, textColor=BLACK
    )
    styles["Body"] = ParagraphStyle(
        "Body", parent=styles["Normal"],
        alignment=TA_JUSTIFY, spaceAfter=8
    )
    styles["H1"] = ParagraphStyle(
        "H1", fontSize=26, leading=32, textColor=BLUE,
        fontName="Helvetica-Bold", spaceAfter=12, spaceBefore=6
    )
    styles["H2"] = ParagraphStyle(
        "H2", fontSize=16, leading=20, textColor=BLUE,
        fontName="Helvetica-Bold", spaceAfter=8, spaceBefore=14
    )
    styles["H3"] = ParagraphStyle(
        "H3", fontSize=13, leading=17, textColor=BLUE,
        fontName="Helvetica-Bold", spaceAfter=6, spaceBefore=10
    )
    styles["H4"] = ParagraphStyle(
        "H4", fontSize=11, leading=15, textColor=BLUE,
        fontName="Helvetica-Bold", spaceAfter=4, spaceBefore=8
    )
    styles["Bullet"] = ParagraphStyle(
        "Bullet", parent=styles["Body"],
        leftIndent=18, bulletIndent=6, spaceAfter=4
    )
    styles["Code"] = ParagraphStyle(
        "Code", fontName="Courier", fontSize=9, leading=13,
        leftIndent=12, spaceAfter=6, backColor=LIGHT_GREY
    )
    styles["TOCEntry1"] = ParagraphStyle(
        "TOCEntry1", fontSize=11, leading=16, textColor=BLUE,
        fontName="Helvetica-Bold", leftIndent=0
    )
    styles["TOCEntry2"] = ParagraphStyle(
        "TOCEntry2", fontSize=10, leading=14, textColor=BLACK,
        leftIndent=18
    )
    styles["Caption"] = ParagraphStyle(
        "Caption", fontSize=8, leading=11, textColor=colors.HexColor("#555555"),
        alignment=TA_CENTER, spaceAfter=4
    )
    styles["CoverTitle"] = ParagraphStyle(
        "CoverTitle", fontSize=36, leading=44, textColor=WHITE,
        fontName="Helvetica-Bold", alignment=TA_CENTER
    )
    styles["CoverSub"] = ParagraphStyle(
        "CoverSub", fontSize=16, leading=22, textColor=WHITE,
        fontName="Helvetica", alignment=TA_CENTER
    )
    styles["CoverVersion"] = ParagraphStyle(
        "CoverVersion", fontSize=11, leading=15, textColor=colors.HexColor("#AACBFF"),
        alignment=TA_CENTER
    )
    return styles


# ── Reusable flowable helpers ──────────────────────────────────────────────────
def warning_box(text, styles):
    """Amber box for critical steps."""
    data = [[Paragraph("<b>&#9888; Important</b><br/>" + text, styles["Body"])]]
    t = Table(data, colWidths=[6.5 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), AMBER),
        ("BOX", (0, 0), (-1, -1), 1.5, AMBER_BORDER),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    return [t, Spacer(1, 8)]


def tip_box(text, styles):
    """Teal box for best-practice tips."""
    data = [[Paragraph("<b>&#128161; Tip</b><br/>" + text, styles["Body"])]]
    t = Table(data, colWidths=[6.5 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), TEAL),
        ("BOX", (0, 0), (-1, -1), 1.5, TEAL_BORDER),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    return [t, Spacer(1, 8)]


def section_rule(styles):
    return [HRFlowable(width="100%", thickness=1, color=BLUE, spaceAfter=6)]


def std_table(headers, rows, col_widths=None):
    """Standard blue-header table."""
    col_widths = col_widths or []
    data = [headers] + rows
    t = Table(data, colWidths=col_widths or None, repeatRows=1)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), BLUE),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GREY]),
        ("FONTSIZE", (0, 1), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, MID_GREY),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]
    t.setStyle(TableStyle(style))
    return t


def bullets(items, styles):
    return [Paragraph("&#8226; " + item, styles["Bullet"]) for item in items]


# ── Page template (header/footer) ─────────────────────────────────────────────
def on_page(canvas, doc):
    canvas.saveState()
    w, h = LETTER
    # Header bar
    canvas.setFillColor(BLUE)
    canvas.rect(0, h - 0.55 * inch, w, 0.55 * inch, fill=1, stroke=0)
    canvas.setFillColor(WHITE)
    canvas.setFont("Helvetica-Bold", 9)
    canvas.drawString(0.5 * inch, h - 0.35 * inch, "LedgerLink AI — User Manual")
    canvas.setFont("Helvetica", 8)
    canvas.drawRightString(w - 0.5 * inch, h - 0.35 * inch, "Confidential — CPA Edition 2026")
    # Footer
    canvas.setFillColor(BLUE)
    canvas.rect(0, 0, w, 0.4 * inch, fill=1, stroke=0)
    canvas.setFillColor(WHITE)
    canvas.setFont("Helvetica", 8)
    canvas.drawString(0.5 * inch, 0.13 * inch, "© 2026 LedgerLink AI Inc. All rights reserved.")
    canvas.drawRightString(w - 0.5 * inch, 0.13 * inch, f"Page {doc.page}")
    canvas.restoreState()


def on_page_first(canvas, doc):
    """Cover page — no header/footer chrome."""
    pass



# ── Section 0: Cover page ──────────────────────────────────────────────────────
def build_cover_page(story, styles):
    # Full-page blue background via a table
    cover_data = [[
        Paragraph("LedgerLink AI", styles["CoverTitle"]),
    ]]
    cover = Table(cover_data, colWidths=[7.5 * inch], rowHeights=[1.2 * inch])
    cover.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BLUE),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(Spacer(1, 1.5 * inch))
    story.append(cover)
    story.append(Spacer(1, 0.3 * inch))

    sub_data = [[Paragraph("User Manual", styles["CoverSub"])]]
    sub = Table(sub_data, colWidths=[7.5 * inch])
    sub.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BLUE),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(sub)
    story.append(Spacer(1, 0.2 * inch))

    ver_data = [[Paragraph("English Edition · Version 3.0 · March 2026<br/>For CPA Firms and Accounting Professionals", styles["CoverVersion"])]]
    ver = Table(ver_data, colWidths=[7.5 * inch])
    ver.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BLUE),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(ver)
    story.append(Spacer(1, 1 * inch))

    info_rows = [
        ["Product", "LedgerLink AI — Accounting Automation Platform"],
        ["Edition", "Professional / Cabinet / Entreprise"],
        ["Language", "English (FR version available separately)"],
        ["Jurisdiction", "Quebec, Canada — GST/QST Compliant"],
        ["Support", "support@ledgerlink.ai"],
        ["Confidentiality", "For licensed users only. Do not distribute."],
    ]
    t = Table(info_rows, colWidths=[1.8 * inch, 5.7 * inch])
    t.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [LIGHT_GREY, WHITE]),
        ("GRID", (0, 0), (-1, -1), 0.5, MID_GREY),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    story.append(t)
    story.append(PageBreak())

    # ── Legal / copyright page ──
    story.append(Spacer(1, 1.5 * inch))
    story.append(Paragraph("Legal Notice", styles["H2"]))
    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph(
        "Copyright &copy; 2024-2026 LedgerLink AI Inc. All rights reserved.", styles["Body"]))
    story.append(Paragraph(
        "This manual is provided exclusively for licensed users of LedgerLink AI. "
        "No part of this document may be reproduced, distributed, or transmitted in "
        "any form without the prior written permission of LedgerLink AI Inc.", styles["Body"]))
    story.append(Paragraph(
        "LedgerLink AI is a registered trademark of LedgerLink AI Inc. QuickBooks and "
        "QuickBooks Online are trademarks of Intuit Inc. Microsoft 365 and SharePoint "
        "are trademarks of Microsoft Corporation. Cloudflare is a trademark of "
        "Cloudflare Inc. All other trademarks are the property of their respective "
        "owners.", styles["Body"]))
    story.append(Spacer(1, 0.3 * inch))
    story.append(Paragraph("Disclaimer", styles["H3"]))
    story.append(Paragraph(
        "This software is a tool to assist accounting professionals. It does not "
        "replace professional judgement. The accountant or auditor remains responsible "
        "for all decisions made using LedgerLink AI, including but not limited to: "
        "tax filing, audit opinions, financial statement preparation, and compliance "
        "with Canadian Auditing Standards (CAS), Accounting Standards for Private "
        "Enterprises (ASPE), and International Financial Reporting Standards (IFRS).", styles["Body"]))
    story.append(Paragraph(
        "LedgerLink AI Inc. is not a licensed accounting firm and does not provide "
        "accounting, audit, tax, or legal services. The AI-generated suggestions, "
        "classifications, and calculations in this software are advisory in nature "
        "and must be validated by a qualified professional before reliance.", styles["Body"]))
    story.append(Spacer(1, 0.3 * inch))
    story.append(Paragraph("Document History", styles["H3"]))
    doc_hist_rows = [
        ["Version", "Date", "Author", "Changes"],
        ["1.0", "October 2024", "LedgerLink AI", "Initial release — Essentiel and Professionnel tiers"],
        ["2.0", "January 2025", "LedgerLink AI", "Added Cabinet tier, audit module, bank reconciliation"],
        ["2.5", "June 2025", "LedgerLink AI", "Added Entreprise tier, CAS compliance, OpenClaw bridge"],
        ["3.0", "March 2026", "LedgerLink AI", "Full CAS coverage, payroll engine, QR codes, enhanced glossary"],
    ]
    story.append(std_table(doc_hist_rows[0], doc_hist_rows[1:], [0.8*inch, 1.2*inch, 1.5*inch, 3.0*inch]))
    story.append(Spacer(1, 0.3 * inch))
    story.append(Paragraph("Contact Information", styles["H3"]))
    contact_rows = [
        ["Channel", "Details"],
        ["Email support", "support@ledgerlink.ai"],
        ["Documentation", "docs.ledgerlink.ai"],
        ["Phone (Quebec)", "+1 (514) 555-LINK"],
        ["Hours", "Monday-Friday, 8:00-17:00 EST"],
        ["Emergency (P1)", "After-hours pager for enterprise tier clients"],
    ]
    story.append(std_table(contact_rows[0], contact_rows[1:], [2.0*inch, 4.5*inch]))
    story.append(PageBreak())


# ── Section 0b: Table of Contents ─────────────────────────────────────────────
def build_toc(story, styles):
    story.append(Paragraph("Table of Contents", styles["H1"]))
    story.extend(section_rule(styles))
    story.append(Spacer(1, 0.1 * inch))

    toc_entries = [
        ("1", "Introduction", ""),
        ("1.1", "What Is LedgerLink AI?", ""),
        ("1.2", "Three-Layer Architecture", ""),
        ("1.3", "System Requirements", ""),
        ("1.4", "Security Architecture", ""),
        ("1.5", "Data Flow Diagram", ""),
        ("2", "Installation & First Login", ""),
        ("2.1", "Running setup.exe", ""),
        ("2.2", "Setup Wizard — 6 Steps", ""),
        ("2.3", "First Login", ""),
        ("2.4", "API Key Configuration", ""),
        ("2.5", "Configuration File", ""),
        ("3", "User Management", ""),
        ("3.1", "Creating User Accounts", ""),
        ("3.2", "Roles & Permissions", ""),
        ("3.3", "Client Portfolios", ""),
        ("3.4", "Password Policies", ""),
        ("4", "Daily Workflow", ""),
        ("4.1", "Document Intake Methods", ""),
        ("4.2", "The Review Queue", ""),
        ("4.3", "Fraud Flags & AI Warnings", ""),
        ("4.4", "Approvals, Holds & Bank Import", ""),
        ("4.5", "Substance Engine Flags", ""),
        ("4.6", "Learning Memory & Vendor Intelligence", ""),
        ("5", "Quebec Tax Compliance", ""),
        ("5.1", "GST/QST Rates", ""),
        ("5.2", "Tax Codes T, Z, E, M, I", ""),
        ("5.3", "ITC/ITR Recovery", ""),
        ("5.4", "Filing Summary & Revenu Québec Pre-Fill", ""),
        ("5.5", "Quick Method & Deadline Calendar", ""),
        ("5.6", "HST & Provincial Sales Tax — All Provinces", ""),
        ("5.7", "Quebec Payroll Compliance", ""),
        ("6", "Client Portal", ""),
        ("6.1", "Creating Client Accounts", ""),
        ("6.2", "Document Submission Methods", ""),
        ("6.3", "Cloudflare Tunnel", ""),
        ("6.4", "Client Communications", ""),
        ("6.5", "QR Codes for Client Onboarding", ""),
        ("6.6", "WhatsApp & Telegram via OpenClaw", ""),
        ("7", "Month-End & Billing", ""),
        ("7.1", "Month-End Checklist", ""),
        ("7.2", "Locking Periods", ""),
        ("7.3", "Time Tracking", ""),
        ("7.4", "Invoice Generation", ""),
        ("7.5", "Bank Reconciliation — Two-Sided Statement", ""),
        ("8", "CPA Audit Module", ""),
        ("8.1", "Engagements", ""),
        ("8.2", "Working Papers", ""),
        ("8.3", "Three-Way Matching", ""),
        ("8.4", "Statistical Sampling (CAS 530)", ""),
        ("8.5", "Trial Balance & Financial Statements", ""),
        ("8.6", "Analytical Procedures (CAS 520)", ""),
        ("8.7", "Materiality (CAS 320)", ""),
        ("8.8", "Risk Assessment (CAS 315)", ""),
        ("8.9", "Control Testing (CAS 330)", ""),
        ("8.10", "Going Concern (CAS 570)", ""),
        ("8.11", "Subsequent Events (CAS 560)", ""),
        ("8.12", "Management Rep Letter (CAS 580)", ""),
        ("8.13", "Related Parties (CAS 550)", ""),
        ("8.14", "Audit Opinion (CAS 700)", ""),
        ("8.15", "Quality Control (CSQC 1)", ""),
        ("9", "Administration", ""),
        ("9.1", "License Management & Tiers", ""),
        ("9.2", "Troubleshoot Page & Autofix", ""),
        ("9.3", "Backups & Updates", ""),
        ("9.4", "Vendor Memory Management", ""),
        ("9.5", "AI Cache Management", ""),
        ("10", "Troubleshooting", ""),
        ("10.1", "Login Failures", ""),
        ("10.2", "PDF Viewer Issues", ""),
        ("10.3", "QuickBooks Posting Failures", ""),
        ("10.4", "AI Extraction Wrong", ""),
        ("10.5", "Email Intake Not Working", ""),
        ("10.6", "Substance Flags — Wrong GL", ""),
        ("10.7", "Fraud Engine — False Positives", ""),
        ("10.8", "Vendor Memory Reset", ""),
        ("10.9", "Running Autofix", ""),
        ("10.10", "Cloudflare Tunnel Issues", ""),
        ("11", "Glossary (FR/EN)", ""),
    ]

    for num, title, _pg in toc_entries:
        is_top = len(num) == 1
        indent = 0 if is_top else 0.3 * inch
        font = "Helvetica-Bold" if is_top else "Helvetica"
        size = 11 if is_top else 10
        text = f"<font name='{font}' size='{size}'>{num}&nbsp;&nbsp;&nbsp;{title}</font>"
        p = Paragraph(text, ParagraphStyle(
            f"TOC_{num}", parent=styles["Normal"],
            leftIndent=indent, spaceAfter=2 if not is_top else 6,
            spaceBefore=4 if is_top else 0,
        ))
        story.append(p)

    story.append(PageBreak())



# ── Section 1: Introduction ────────────────────────────────────────────────────
def build_section_1_introduction(story, styles):
    story.append(Paragraph("Section 1 — Introduction", styles["H1"]))
    story.extend(section_rule(styles))

    story.append(Paragraph("1.1  What Is LedgerLink AI?", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI is a full-stack accounting automation platform designed for "
        "Canadian CPA firms operating in Quebec. It ingests source documents "
        "(invoices, receipts, bank statements) from email, SharePoint, or file-system "
        "folders, extracts structured data using OCR and large language models, applies "
        "deterministic GST/QST tax logic, and posts clean transactions directly to "
        "QuickBooks Online — all under human-in-the-loop review control.", styles["Body"]))
    story.append(Paragraph(
        "The platform is multi-tenant, bilingual (French/English), and role-aware. "
        "A single installation can support dozens of client files simultaneously, each "
        "with its own GL chart of accounts, tax registrations, and filing calendar. "
        "Every AI decision is logged, explainable, and auditable.", styles["Body"]))

    story.extend(tip_box(
        "LedgerLink AI is not a general-purpose bookkeeping tool. It is purpose-built "
        "for CPA firms that handle GST/QST-registered business clients in Quebec, "
        "Ontario, and other Canadian provinces.", styles))

    story.append(Paragraph("Key capabilities at a glance:", styles["H4"]))
    story.extend(bullets([
        "Automatic document ingestion from Microsoft 365, SharePoint, and local folders",
        "AI-powered extraction of vendor, date, amount, and tax amounts",
        "Deterministic GST/QST calculation with ITC/ITR tracking",
        "Fraud detection engine with 9 rule categories",
        "Bank statement import and three-way matching",
        "QuickBooks Online posting with full audit trail",
        "CPA audit module: working papers, trial balance, statistical sampling",
        "Quebec Revenu Québec pre-fill and Quick Method support",
        "Client portal for secure document submission",
        "Time tracking and invoice generation",
        "Bilingual UI — French default for Quebec workflow",
    ], styles))

    story.append(Paragraph("1.2  Three-Layer Architecture", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI is organized into three logical layers that separate concerns "
        "and allow each component to be upgraded independently:", styles["Body"]))

    arch_rows = [
        ["Layer", "Components", "Role"],
        ["Ingestion Layer",
         "OCR Engine, Folder Watcher, SharePoint Processor, Graph Mail",
         "Accepts raw documents from any channel; extracts text and metadata; "
         "deduplicates using SHA-256 fingerprinting."],
        ["Intelligence Layer",
         "AI Router (DeepSeek / OpenRouter), Rules Engine, Fraud Engine, "
         "Tax Engine, Hallucination Guard, Learning Memory",
         "Classifies documents, assigns GL codes and tax codes, detects anomalies, "
         "learns from corrections, and validates AI output against hard rules."],
        ["Action Layer",
         "Review Dashboard (port 8787), Posting Builder, QBO Online Adapter, "
         "Period Close, Audit Engine, Invoice Generator",
         "Presents decisions to human reviewers; posts approved transactions to "
         "QuickBooks; manages month-end close, invoicing, and audit engagements."],
    ]
    t = std_table(arch_rows[0], arch_rows[1:], [1.2*inch, 2.2*inch, 3.1*inch])
    story.append(t)
    story.append(Spacer(1, 10))

    story.append(Paragraph(
        "The three layers communicate through a SQLite database "
        "(data/ledgerlink_agent.db) and a shared configuration file "
        "(ledgerlink.config.json). No external message broker is required for "
        "single-server deployments.", styles["Body"]))

    story.extend(warning_box(
        "The SQLite database must reside on a local SSD. Placing it on a network "
        "share or cloud-synced folder (OneDrive, Dropbox) will cause lock contention "
        "and data corruption under concurrent access.", styles))

    story.append(Paragraph("AI Provider Routing", styles["H3"]))
    story.append(Paragraph(
        "LedgerLink AI uses a dual-provider AI strategy to balance cost and quality:", styles["Body"]))
    ai_rows = [
        ["Task Type", "Default Provider", "Examples"],
        ["Routine", "DeepSeek (low cost)", "Classify, Extract, Suggest GL"],
        ["Complex", "OpenRouter (premium)", "Explain anomaly, Escalation, Compliance, Working paper narrative"],
    ]
    story.append(std_table(ai_rows[0], ai_rows[1:], [1.5*inch, 2.2*inch, 2.8*inch]))
    story.append(Spacer(1, 8))
    story.extend(tip_box(
        "You can override the provider per task in ledgerlink.config.json under the "
        "ai_router section. Premium tasks consume credits faster — review the audit_log "
        "table to monitor usage.", styles))

    story.append(Paragraph("1.3  System Requirements", styles["H2"]))
    req_rows = [
        ["Component", "Minimum", "Recommended"],
        ["OS", "Windows 10 64-bit", "Windows 11 Pro 64-bit"],
        ["CPU", "4 cores / 2.5 GHz", "8 cores / 3.5 GHz"],
        ["RAM", "8 GB", "16 GB"],
        ["Disk", "20 GB SSD free", "100 GB NVMe SSD"],
        ["Python", "3.10", "3.12"],
        ["Network", "10 Mbps upload", "100 Mbps (for SharePoint sync)"],
        ["QuickBooks", "QBO subscription (any plan)", "QBO Plus or Advanced"],
        ["Microsoft 365", "Optional (for Graph mail/SharePoint)", "Business Standard or higher"],
        ["Browser", "Chrome 110+ or Edge 110+", "Chrome 120+ (for inline PDF viewer)"],
    ]
    story.append(std_table(req_rows[0], req_rows[1:], [1.5*inch, 2.3*inch, 2.7*inch]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Port Usage Summary", styles["H3"]))
    port_rows = [
        ["Port", "Service", "Who Accesses It"],
        ["8787", "Main Review Dashboard", "Accountants (owner, manager, employee)"],
        ["8788", "Client Portal", "End clients submitting documents"],
        ["8789", "Ingest API", "Folder watcher, external automation"],
    ]
    story.append(std_table(port_rows[0], port_rows[1:], [0.8*inch, 2.5*inch, 3.2*inch]))
    story.append(Spacer(1, 10))
    story.extend(warning_box(
        "Do not expose ports 8787 or 8789 directly to the internet. Use a Cloudflare "
        "Tunnel or reverse proxy with TLS termination. Port 8788 (client portal) can be "
        "safely tunnelled via Cloudflare for external client access.", styles))

    story.append(Paragraph("Supported Document Formats", styles["H3"]))
    fmt_rows = [
        ["Format", "Extension", "Extraction Method", "Notes"],
        ["PDF (native text)", ".pdf", "pdfplumber text extraction", "Fastest and most accurate; preferred format"],
        ["PDF (scanned)", ".pdf", "Claude Vision API fallback", "Falls back to Vision when text < 20 words"],
        ["JPEG / JPG", ".jpg, .jpeg", "Claude Vision API", "Common for photographed receipts"],
        ["PNG", ".png", "Claude Vision API", "Screenshots and scanned images"],
        ["HEIC", ".heic", "Claude Vision API", "iPhone photo format — auto-converted"],
        ["TIFF", ".tiff, .tif", "Claude Vision API", "Multi-page scanner output"],
        ["WebP", ".webp", "Claude Vision API", "Web-optimised image format"],
    ]
    story.append(std_table(fmt_rows[0], fmt_rows[1:], [1.3*inch, 1.0*inch, 2.0*inch, 2.2*inch]))
    story.append(Spacer(1, 8))
    story.extend(bullets([
        "Maximum file size: 20 MB per document (configurable in ledgerlink.config.json)",
        "Multi-page PDFs are processed page-by-page; totals reconciled across pages",
        "Password-protected PDFs are rejected and routed to the exception queue",
        "Handwritten documents are automatically detected and routed to Vision API",
        "Low-confidence extractions (below 0.70) are flagged as NeedsReview",
    ], styles))

    story.append(Paragraph("1.4  Security Architecture", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI is designed with security as a core principle. The system "
        "handles sensitive financial data and must meet the confidentiality "
        "requirements of CPA professional standards.", styles["Body"]))

    sec_rows = [
        ["Layer", "Mechanism", "Details"],
        ["Authentication", "bcrypt password hashing", "12 rounds minimum; legacy SHA-256 auto-detected and forced to reset"],
        ["Session management", "JWT tokens", "12-hour TTL; stored server-side; invalidated on logout or password change"],
        ["Authorization", "Role-based access control", "4 roles (owner/manager/employee/client) with strict permission boundaries"],
        ["Data isolation", "Portfolio-based scoping", "Employees see only assigned clients; clients see only own documents"],
        ["Transport", "Cloudflare Tunnel (TLS)", "No direct port exposure; automatic certificate management"],
        ["API security", "Bearer token authentication", "Port 8789 ingest API requires pre-shared bearer token"],
        ["Audit trail", "Immutable audit_log table", "Every user action logged with timestamp, user, action, and details"],
        ["File integrity", "SHA-256 fingerprinting", "Every ingested document hashed; duplicates rejected"],
        ["AI data isolation", "No training on client data", "AI providers process documents but do not retain or train on them"],
    ]
    story.append(std_table(sec_rows[0], sec_rows[1:], [1.3*inch, 1.8*inch, 3.4*inch]))
    story.append(Spacer(1, 8))

    story.extend(warning_box(
        "LedgerLink AI stores financial data in a local SQLite database. Ensure the "
        "server has full-disk encryption (BitLocker) enabled and that physical access "
        "is restricted. The database file contains client names, vendor names, invoice "
        "amounts, and tax information.", styles))

    story.append(Paragraph("1.5  Data Flow Diagram", styles["H2"]))
    story.append(Paragraph(
        "The following table summarises the end-to-end data flow from document intake "
        "to QuickBooks posting:", styles["Body"]))
    flow_rows = [
        ["Stage", "Input", "Processing", "Output"],
        ["1. Intake", "PDF/image from email, SharePoint, portal, or folder",
         "Fingerprint check, format detection", "Raw document in data/ directory"],
        ["2. OCR", "Raw document file",
         "pdfplumber or Vision API extraction", "Extracted text and metadata"],
        ["3. Classification", "Extracted text",
         "AI classification (invoice/receipt/credit note/bank/other)", "Document type and confidence score"],
        ["4. Extraction", "Extracted text + document type",
         "AI field extraction (vendor, date, amount, taxes)", "Structured data with confidence scores"],
        ["5. Validation", "Structured data",
         "Hallucination guard, tax engine, fraud engine, substance engine",
         "Validated data with flags and warnings"],
        ["6. Queue insertion", "Validated data",
         "Write to documents table with NeedsReview status", "Document visible in review queue"],
        ["7. Human review", "Document in review queue",
         "Reviewer verifies, corrects, approves or holds", "Document status updated"],
        ["8. Posting", "Approved document",
         "Posting builder creates QBO journal entry; QBO adapter posts", "Transaction in QuickBooks Online"],
    ]
    story.append(std_table(flow_rows[0], flow_rows[1:], [0.8*inch, 1.5*inch, 2.0*inch, 2.2*inch]))
    story.append(Spacer(1, 8))
    story.append(PageBreak())



# ── Section 2: Installation ────────────────────────────────────────────────────
def build_section_2_installation(story, styles):
    story.append(Paragraph("Section 2 — Installation &amp; First Login", styles["H1"]))
    story.extend(section_rule(styles))

    story.append(Paragraph("2.1  Running setup.exe", styles["H2"]))
    story.append(Paragraph(
        "The LedgerLink AI installer is distributed as a single Windows executable "
        "(LedgerLink-Setup-3.x.x.exe). It is built with PyInstaller and bundles the "
        "Python runtime, all dependencies, and the default configuration template. "
        "No internet connection is required during installation.", styles["Body"]))
    story.extend(warning_box(
        "Run setup.exe as Administrator. Right-click the file and select "
        "'Run as administrator'. The installer writes to Program Files and registers "
        "a Windows service — both require elevated privileges.", styles))

    install_steps = [
        ["Step", "Screen / Action", "Notes"],
        ["1", "Welcome screen — click Next", "Verify version number matches your license"],
        ["2", "License Agreement — Accept", "EULA must be accepted to continue"],
        ["3", "Installation directory", "Default: C:\Program Files\LedgerLinkAI"],
        ["4", "Component selection", "Core (required), Audit Module, Client Portal"],
        ["5", "Shortcut creation", "Desktop shortcut + Start Menu entry"],
        ["6", "Install", "Copies files, installs Python packages via pip"],
        ["7", "Service registration", "Creates LedgerLinkAI Windows service (auto-start)"],
        ["8", "Finish — Launch Now?", "Opens browser to http://127.0.0.1:8787"],
    ]
    story.append(std_table(install_steps[0], install_steps[1:], [0.5*inch, 2.2*inch, 3.8*inch]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("2.2  Setup Wizard — 6 Steps", styles["H2"]))
    story.append(Paragraph(
        "On first launch, LedgerLink AI displays the Setup Wizard "
        "(scripts/setup_wizard.py). The wizard collects the minimum configuration "
        "needed to process documents. You can re-run it at any time via "
        "Administration → Run Setup Wizard.", styles["Body"]))

    wizard_steps = [
        ["Step", "Field", "Example / Notes"],
        ["1", "Tenant label", "Gestion Beaumont Inc. — friendly name shown in UI"],
        ["2", "Tenant domain", "gesbeau.onmicrosoft.com or custom domain"],
        ["3", "Mailbox address", "comptabilite@gesbeau.com — delegated Graph access"],
        ["4", "SharePoint site URL", "https://gesbeau.sharepoint.com/sites/Accounting"],
        ["5", "Folder paths", "Inbox / Processing / Review / Archive (SharePoint paths)"],
        ["6", "Language preference", "AUTO (detect per user), EN, or FR"],
    ]
    story.append(std_table(wizard_steps[0], wizard_steps[1:], [0.5*inch, 1.8*inch, 4.2*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Additional Wizard Settings", styles["H3"]))
    story.extend(bullets([
        "Province default — sets the default tax province (QC, ON, BC, etc.)",
        "MSAL token caching — stores Microsoft authentication tokens per tenant",
        "Email connectivity test — validates Graph API access to the mailbox",
        "SharePoint connectivity test — lists root folder contents to confirm access",
        "After wizard completion, settings are saved to ledgerlink.config.json",
    ], styles))
    story.extend(tip_box(
        "If you manage multiple clients with separate Microsoft 365 tenants, run the "
        "wizard once per tenant. Each tenant gets its own MSAL token cache entry "
        "(.o365_profile/ directory).", styles))

    story.append(Paragraph("2.3  First Login", styles["H2"]))
    story.append(Paragraph(
        "After installation, an initial owner account is created automatically. "
        "Default credentials are printed to the installer log and shown on screen. "
        "Change the password immediately using Administration → User Management → "
        "Reset Password.", styles["Body"]))
    story.extend(warning_box(
        "The default owner password is a random 12-character string generated at "
        "install time. It is stored in installer.log in the install directory. "
        "Delete this file after recording the password.", styles))

    login_steps = [
        "Open http://127.0.0.1:8787 in Chrome or Edge",
        "Enter the username (default: admin) and the installer-generated password",
        "Click Log In — a bcrypt verification is performed (may take 0.5 s)",
        "You are redirected to the dashboard home page",
        "Go to Administration → Change Password and set a strong password",
        "Optionally enable two-factor authentication (TOTP) via Administration → Security",
    ]
    for i, step in enumerate(login_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Session Management", styles["H3"]))
    story.append(Paragraph(
        "Sessions use JWT tokens with a 12-hour TTL stored in the dashboard_sessions "
        "table. Tokens are invalidated on logout or password change. If you need "
        "longer sessions for overnight batch runs, contact support for configuration "
        "guidance.", styles["Body"]))

    story.append(Paragraph("2.4  API Key Configuration", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI requires API keys for its AI providers and optional integrations. "
        "Keys are stored in ledgerlink.config.json and/or the .env file. Never commit "
        "these to version control.", styles["Body"]))

    key_rows = [
        ["Provider", "Config Key", "Where to Obtain", "Required?"],
        ["DeepSeek", "ai_router.deepseek_api_key", "platform.deepseek.com/api_keys", "YES (routine AI tasks)"],
        ["OpenRouter", "ai_router.openrouter_api_key", "openrouter.ai/keys", "YES (premium AI tasks)"],
        ["QuickBooks Online", "qbo.client_id / qbo.client_secret", "developer.intuit.com", "YES (for posting)"],
        ["Microsoft Graph", "graph.client_id / graph.tenant_id", "Azure AD App Registration", "Optional (email/SharePoint)"],
        ["Cloudflare", "cloudflare.tunnel_token", "dash.cloudflare.com", "Optional (client portal exposure)"],
        ["SMTP (email)", "digest.smtp_user / smtp_password", "Your email provider", "Optional (daily digest)"],
        ["WhatsApp Business API", "openclaw.whatsapp_token", "Meta Business Suite", "Optional (OpenClaw bridge)"],
        ["Telegram Bot", "openclaw.telegram_bot_token", "@BotFather on Telegram", "Optional (OpenClaw bridge)"],
    ]
    story.append(std_table(key_rows[0], key_rows[1:], [1.4*inch, 1.8*inch, 1.8*inch, 1.5*inch]))
    story.append(Spacer(1, 8))
    story.extend(warning_box(
        "API keys grant access to external services on your behalf. Treat them like "
        "passwords. Store .env outside the web-accessible directory. Rotate keys "
        "immediately if you suspect compromise.", styles))

    story.append(Paragraph("Testing API Connectivity", styles["H3"]))
    story.append(Paragraph(
        "After entering API keys, verify connectivity using the Troubleshoot page:", styles["Body"]))
    api_test_steps = [
        "Navigate to Administration, then Troubleshoot",
        "Check 4 (AI Provider): verifies DeepSeek and OpenRouter API keys return valid responses",
        "Check 5 (QBO Connection): verifies the QuickBooks OAuth token is valid and not expired",
        "Check 6 (Graph API): verifies Microsoft 365 Graph API access to the configured mailbox",
        "If any check fails, update the corresponding key in ledgerlink.config.json and restart",
    ]
    for i, step in enumerate(api_test_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("2.5  Configuration File (ledgerlink.config.json)", styles["H2"]))
    story.append(Paragraph(
        "All runtime settings live in ledgerlink.config.json in the installation "
        "directory. This file is read at startup; changes require a service restart "
        "unless noted.", styles["Body"]))

    config_rows = [
        ["Key", "Default", "Description"],
        ["host", "127.0.0.1", "Bind address for all three HTTP servers"],
        ["port", "8787", "Main dashboard port"],
        ["ai_router.routine_provider", "deepseek", "Provider for classify/extract tasks"],
        ["ai_router.premium_provider", "openrouter", "Provider for complex reasoning tasks"],
        ["security.bcrypt_rounds", "12", "bcrypt cost factor (higher = slower but more secure)"],
        ["security.session_ttl_hours", "12", "JWT session lifetime"],
        ["client_portal.enabled", "true", "Enable/disable port 8788"],
        ["client_portal.max_upload_mb", "20", "Maximum single-file upload size"],
        ["digest.smtp_host", "smtp.office365.com", "SMTP server for daily digest emails"],
        ["ingest.api_key", "(generated)", "Bearer token for port 8789 ingest API"],
    ]
    story.append(std_table(config_rows[0], config_rows[1:], [2.2*inch, 1.5*inch, 2.8*inch]))
    story.append(Spacer(1, 8))
    story.extend(tip_box(
        "Keep a backup copy of ledgerlink.config.json outside the installation "
        "directory. It contains API keys and SMTP credentials that would be lost "
        "if you uninstall and reinstall the application.", styles))
    story.append(PageBreak())



# ── Section 3: User Management ────────────────────────────────────────────────
def build_section_3_user_management(story, styles):
    story.append(Paragraph("Section 3 — User Management", styles["H1"]))
    story.extend(section_rule(styles))

    story.append(Paragraph("3.1  Creating User Accounts", styles["H2"]))
    story.append(Paragraph(
        "User accounts are managed from Administration → User Management. Only users "
        "with the owner or manager role can create new accounts. Each account is stored "
        "in the dashboard_users table with a bcrypt-hashed password.", styles["Body"]))

    create_steps = [
        "Navigate to Administration → User Management",
        "Click Add User in the top-right corner",
        "Enter a unique username (letters, numbers, underscores only)",
        "Select the role from the dropdown (owner, manager, employee, client)",
        "Enter a temporary password — the user must change it on first login",
        "Set the language preference (EN or FR) for the user's UI",
        "Assign the user to one or more client portfolios (for employee role)",
        "Click Save — the account is active immediately",
    ]
    for i, step in enumerate(create_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    story.extend(warning_box(
        "Do not create accounts with role='client' through the main User Management "
        "screen. Client accounts must be created through the Client Portal section "
        "(Administration → Client Portal → Add Client) to ensure correct isolation "
        "and credential scoping.", styles))

    story.append(Paragraph("3.2  Roles &amp; Permissions", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI implements four distinct roles with strictly enforced "
        "permission boundaries. The role is checked on every API request via JWT "
        "token validation.", styles["Body"]))

    roles_rows = [
        ["Role", "Client Access", "Can Assign", "Can Post", "Admin", "Description"],
        ["owner", "All clients", "Yes", "Yes", "Full", "Firm owner — complete system access including license and user management"],
        ["manager", "All clients", "Yes", "Yes", "Partial", "Team lead — can oversee all clients and assign work to employees"],
        ["employee", "Assigned only", "No", "No", "None", "Staff accountant — sees only documents assigned to their portfolio"],
        ["client", "Own docs only", "No", "No", "None", "End client — accesses Client Portal (port 8788) only"],
    ]
    story.append(std_table(roles_rows[0], roles_rows[1:], [0.9*inch, 1.0*inch, 0.8*inch, 0.8*inch, 0.7*inch, 2.3*inch]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Permission Details by Feature", styles["H3"]))
    perm_rows = [
        ["Feature", "owner", "manager", "employee", "client"],
        ["View all documents", "YES", "YES", "Assigned only", "Own only"],
        ["Approve / Reject", "YES", "YES", "YES (assigned)", "NO"],
        ["Post to QuickBooks", "YES", "YES", "NO", "NO"],
        ["Lock period", "YES", "YES", "NO", "NO"],
        ["Manage users", "YES", "NO", "NO", "NO"],
        ["View license", "YES", "NO", "NO", "NO"],
        ["View audit log", "YES", "YES", "NO", "NO"],
        ["Time tracking", "YES", "YES", "YES", "NO"],
        ["Generate invoices", "YES", "YES", "NO", "NO"],
        ["Audit module", "YES", "YES", "NO", "NO"],
        ["Upload documents", "YES", "YES", "YES", "YES (portal)"],
    ]
    story.append(std_table(perm_rows[0], perm_rows[1:], [2.2*inch, 0.9*inch, 0.9*inch, 1.2*inch, 0.9*inch]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("3.3  Client Portfolios", styles["H2"]))
    story.append(Paragraph(
        "A portfolio is a named collection of clients assigned to an employee. "
        "Portfolios define the document scope visible to each staff member. An employee "
        "can belong to multiple portfolios. Owners and managers always see all clients "
        "regardless of portfolio assignment.", styles["Body"]))
    story.extend(bullets([
        "Create portfolios via Administration → Portfolios → New Portfolio",
        "Assign clients to a portfolio by selecting them from the client list",
        "Assign employees to a portfolio via the portfolio edit screen",
        "Portfolio membership is enforced at the database query level — not just UI",
        "Removing a client from a portfolio does not delete any documents",
        "An employee with no portfolio sees zero documents — verify assignments after onboarding",
    ], styles))
    story.extend(tip_box(
        "Use portfolios to implement team-based billing. Assign a group of clients to "
        "a senior employee's portfolio, then export time entries filtered by portfolio "
        "to generate per-team invoices.", styles))

    story.append(Paragraph("3.4  Password Policies", styles["H2"]))
    story.append(Paragraph(
        "Passwords are hashed with bcrypt at 12 rounds (configurable). The system "
        "detects legacy SHA-256 hashes left from older versions and forces a password "
        "reset on the next login. This check is automatic — no manual intervention "
        "is required.", styles["Body"]))
    pw_rows = [
        ["Policy", "Value"],
        ["Minimum length", "10 characters"],
        ["Complexity", "At least one uppercase, one digit, one special character"],
        ["bcrypt rounds", "12 (≈ 0.5 s verify time — normal and expected)"],
        ["Session TTL", "12 hours (configurable in ledgerlink.config.json)"],
        ["Failed login lockout", "5 attempts → 15-minute lockout"],
        ["Password reset", "Owner/manager can reset any user; users can reset own"],
        ["Legacy SHA-256", "Auto-detected → must_reset_password flag set → forced on next login"],
    ]
    story.append(std_table(pw_rows[0], pw_rows[1:], [2.5*inch, 4.0*inch]))
    story.append(Spacer(1, 8))
    story.extend(warning_box(
        "If you are migrating from LedgerLink AI v1.x, all existing passwords were "
        "stored as SHA-256 hashes. Every user will be prompted to reset their password "
        "on their first login after upgrading. This is expected behaviour and cannot "
        "be bypassed.", styles))
    story.append(PageBreak())



# ── Section 4: Daily Workflow ──────────────────────────────────────────────────
def build_section_4_daily_workflow(story, styles):
    story.append(Paragraph("Section 4 — Daily Workflow", styles["H1"]))
    story.extend(section_rule(styles))

    story.append(Paragraph("4.1  Document Intake Methods", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI accepts documents through four parallel intake channels. "
        "All channels converge on the same processing pipeline and review queue.", styles["Body"]))

    intake_rows = [
        ["Channel", "How It Works", "Best For"],
        ["Microsoft 365 Email", "Graph API polls the shared mailbox every 5 min; "
         "PDF attachments are extracted automatically",
         "Clients who email invoices directly"],
        ["SharePoint Folder", "Folder Watcher monitors the Inbox SharePoint library; "
         "new files trigger processing within 60 s",
         "Clients who upload to SharePoint"],
        ["Local Folder Watch", "scripts/folder_watcher.py monitors a local directory; "
         "useful for scanner drop-folders",
         "Office scanners and local filing"],
        ["Client Portal Upload", "Clients log in to port 8788 and drag-drop files; "
         "max 20 MB per file",
         "Clients without Microsoft 365"],
        ["Ingest API (port 8789)", "POST /ingest with Bearer token; used by "
         "scripts/ingest_folder_to_store.py",
         "Batch scripts and integrations"],
    ]
    story.append(std_table(intake_rows[0], intake_rows[1:], [1.4*inch, 2.8*inch, 2.3*inch]))
    story.append(Spacer(1, 8))
    story.extend(tip_box(
        "Enable all intake channels simultaneously — they are not mutually exclusive. "
        "Many clients use email for routine invoices and the portal for bulk monthly "
        "uploads.", styles))

    story.append(Paragraph("Processing Pipeline", styles["H3"]))
    pipeline_steps = [
        "Fingerprint check — SHA-256 hash compared against fingerprint_registry; duplicates rejected",
        "OCR extraction — pdf_extract.py or ocr_engine.py extracts raw text",
        "AI classification — document type detected (invoice, receipt, credit note, bank, other)",
        "Field extraction — vendor name, date, subtotal, tax amounts extracted by AI",
        "Hallucination guard — AI output validated against hard rules (amounts must be numeric, dates must be real)",
        "Tax engine — GST/QST amounts reconciled; tax code suggested",
        "Fraud engine — 9 fraud rules checked; flags written to documents table",
        "GL suggestion — learning memory queried for vendor GL mappings",
        "Queue insertion — document inserted into review queue with status Needs Review",
    ]
    for i, step in enumerate(pipeline_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))

    story.append(Paragraph("4.2  The Review Queue", styles["H2"]))
    story.append(Paragraph(
        "The review queue is the central workspace for accountants. It is accessible "
        "at http://127.0.0.1:8787 after login. Documents are sorted by urgency: "
        "fraud-flagged first, then by intake date.", styles["Body"]))

    status_rows = [
        ["Status", "Meaning", "Next Action"],
        ["Needs Review", "Document processed; awaiting human verification", "Review and approve or hold"],
        ["On Hold", "Flagged for follow-up (missing info, client query)", "Resolve and move to approved"],
        ["Ready to Post", "Approved by reviewer; awaiting QuickBooks posting", "Run posting queue"],
        ["Posted", "Successfully written to QuickBooks Online", "No action required"],
        ["Exception", "Pipeline error or unresolvable issue", "Check exception queue"],
        ["Rejected", "Duplicate, fraud-confirmed, or invalid document", "Archived — no posting"],
    ]
    story.append(std_table(status_rows[0], status_rows[1:], [1.2*inch, 2.8*inch, 2.5*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Review Panel Fields", styles["H3"]))
    field_rows = [
        ["Field", "Source", "Editable?"],
        ["Vendor name", "AI extraction (OCR)", "Yes — correction trains learning memory"],
        ["Invoice date", "AI extraction", "Yes"],
        ["Subtotal", "AI extraction", "Yes"],
        ["GST amount", "AI extraction + tax engine", "Yes"],
        ["QST amount", "AI extraction + tax engine", "Yes"],
        ["Total", "Calculated from subtotal + taxes", "Read-only (auto)"],
        ["GL account", "Learning memory suggestion", "Yes — required before posting"],
        ["Tax code", "Tax engine (T/Z/E/M/I)", "Yes — required before posting"],
        ["Client", "Intake channel metadata", "Yes (owner/manager only)"],
        ["Reviewer note", "Manual entry", "Yes"],
    ]
    story.append(std_table(field_rows[0], field_rows[1:], [1.4*inch, 2.5*inch, 2.6*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("4.3  Fraud Flags and AI Warnings", styles["H2"]))
    story.append(Paragraph(
        "The fraud engine applies nine rule categories to every document. Triggered "
        "rules are stored in the documents table and displayed prominently in the "
        "review panel.", styles["Body"]))

    fraud_rows = [
        ["Rule", "Trigger Condition", "Suggested Action"],
        ["Duplicate Invoice", "Same vendor + amount + date within 30 days", "Compare originals; reject if confirmed duplicate"],
        ["Timing Anomaly", "Invoice date on weekend or statutory holiday", "Verify with vendor; common for imported documents"],
        ["Round Amount", "Amount is a round number (e.g., $1,000.00 exactly)", "Low risk — review context"],
        ["Vendor Not Seen", "Vendor has no history in vendor_memory", "Verify vendor legitimacy before approving"],
        ["Amount Spike", "Amount > 3x vendor historical mean", "Confirm with client before posting"],
        ["Missing Tax", "Non-zero amount but no GST/QST detected", "Check if vendor is GST-registered"],
        ["Date in Future", "Invoice date more than 7 days in the future", "Confirm date with client"],
        ["High Amount", "Amount exceeds configurable threshold (default $10,000)", "Escalate to manager for dual approval"],
        ["Pattern Break", "Vendor billing cycle or amount differs from learned pattern", "Review vendor memory; may be legitimate"],
    ]
    story.append(std_table(fraud_rows[0], fraud_rows[1:], [1.3*inch, 2.4*inch, 2.8*inch]))
    story.append(Spacer(1, 8))
    story.extend(warning_box(
        "A fraud flag does NOT prevent posting. It is an advisory — the reviewer "
        "must make the final decision. Document your reasoning in the reviewer note "
        "field for every flagged item you approve.", styles))

    story.append(Paragraph("AI Extraction Confidence", styles["H3"]))
    story.append(Paragraph(
        "When the AI extraction confidence is below 80%, an amber warning is shown "
        "on the document card. Low confidence occurs with handwritten documents, "
        "poor scan quality, or unusual invoice layouts. Always verify field values "
        "against the original PDF in these cases.", styles["Body"]))

    story.append(Paragraph("4.4  Approvals, Holds and Bank Import", styles["H2"]))
    story.append(Paragraph(
        "Approving a document moves it to Ready to Post status. The posting queue "
        "(scripts/run_posting_queue.py) processes this queue and submits transactions "
        "to QuickBooks Online.", styles["Body"]))
    story.extend(bullets([
        "Approve: Reviewer clicks Approve — document moves to Ready to Post",
        "Hold: Reviewer clicks Hold and enters a reason — document stays in queue with On Hold status",
        "Bulk approve: Select multiple documents, then Actions, then Approve Selected",
        "Hold release: Click Resolve Hold — document returns to Needs Review for final approval",
        "Reject: Removes document from posting consideration; status set to Rejected",
    ], styles))

    story.append(Paragraph("Bank Statement Import", styles["H3"]))
    story.append(Paragraph(
        "Bank statements can be imported via Administration, then Bank Import. The system "
        "accepts CSV files exported from major Canadian banks. The bank_parser engine "
        "normalizes column formats and the bank_matcher attempts to match each "
        "transaction to a document in the review queue.", styles["Body"]))
    match_rows = [
        ["Match Confidence", "Colour", "Action"],
        ["90% or higher", "Green", "Auto-confirmed — no review needed"],
        ["70 to 89%", "Yellow", "Suggested — reviewer confirms"],
        ["Below 70%", "Red", "Unmatched — manual assignment required"],
    ]
    story.append(std_table(match_rows[0], match_rows[1:], [1.3*inch, 1.0*inch, 4.2*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Supported Banks for Statement Import", styles["H3"]))
    story.append(Paragraph(
        "The bank parser (src/engines/bank_parser.py) automatically detects the bank "
        "from header patterns and column layouts. The following banks are supported "
        "natively:", styles["Body"]))
    bank_rows = [
        ["Bank", "CSV Format", "PDF Format", "Auto-Detection Method"],
        ["Desjardins", "YES", "YES", "Header pattern: 'Mouvement Desjardins'"],
        ["National Bank (BNC)", "YES", "YES", "Header pattern: 'Banque Nationale'"],
        ["BMO", "YES", "Partial", "Column headers: Date, Description, Debit, Credit"],
        ["TD Canada Trust", "YES", "YES", "Header pattern: 'TD Canada Trust'"],
        ["RBC Royal Bank", "YES", "YES", "Header pattern: 'Royal Bank'"],
        ["CIBC", "YES", "NO", "Column headers: Transaction Date, Description, Amount"],
        ["Scotiabank", "YES", "NO", "Header pattern: 'Scotiabank'"],
    ]
    story.append(std_table(bank_rows[0], bank_rows[1:], [1.3*inch, 0.8*inch, 0.8*inch, 3.6*inch]))
    story.append(Spacer(1, 8))
    story.extend(tip_box(
        "For banks not in the supported list, export the statement as a generic CSV "
        "with columns: Date, Description, Amount (or Debit/Credit). The bank parser's "
        "generic CSV mode handles most standard formats.", styles))

    story.append(Paragraph("Split Payment Handling", styles["H3"]))
    story.append(Paragraph(
        "When a single bank transaction corresponds to multiple invoices (e.g., a "
        "client paid three invoices with one cheque), use the Split Payment feature:", styles["Body"]))
    split_steps = [
        "In the Bank Import screen, select the unmatched transaction",
        "Click Split Payment — a dialog appears",
        "Search for and select the invoices that make up this payment",
        "Verify that the selected invoice totals equal the bank transaction amount",
        "Click Confirm Split — each invoice is matched to its proportional share",
        "The bank transaction is marked as fully matched",
    ]
    for i, step in enumerate(split_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("4.5  Substance Engine Flags", styles["H2"]))
    story.append(Paragraph(
        "The substance engine (src/engines/substance_engine.py) runs after the fraud "
        "engine and classifies documents that require special accounting treatment. "
        "Unlike fraud flags (which are advisory), substance flags may block auto-approval "
        "because they affect the correct GL account selection.", styles["Body"]))

    subst_rows = [
        ["Flag", "Trigger", "Effect on Workflow"],
        ["CapEx", "Amount >= $1,500 AND keywords: equipment, machine, vehicle, furniture",
         "Blocks auto-approval; suggests asset GL account instead of expense"],
        ["Prepaid Expense", "Keywords: annual, subscription, 12-month, prepaid",
         "Blocks auto-approval; suggests prepaid asset GL account"],
        ["Loan / Financing", "Keywords: loan, financing, installment, payment plan",
         "Blocks auto-approval; may need liability GL account"],
        ["Tax Remittance", "Keywords: CRA, Revenu Quebec, installment, remittance",
         "Blocks auto-approval; prevents double-counting tax payments as expenses"],
        ["Personal Expense", "Keywords: home, personal, family, private",
         "Blocks auto-approval; flags for client discussion"],
        ["Customer Deposit", "Keywords: deposit, advance, retainer",
         "Blocks auto-approval; may need liability GL account (deferred revenue)"],
        ["Intercompany Transfer", "Payer and payee entity names are similar",
         "Blocks auto-approval; intercompany transactions need elimination"],
        ["Mixed Tax Invoice", "Both taxable and exempt items detected on same invoice",
         "Blocks auto-approval; requires manual tax code allocation per line"],
    ]
    story.append(std_table(subst_rows[0], subst_rows[1:], [1.2*inch, 2.5*inch, 2.8*inch]))
    story.append(Spacer(1, 8))
    story.extend(warning_box(
        "Substance flags that block auto-approval require manual GL assignment before "
        "the document can be posted. The reviewer must select the correct GL account "
        "and confirm the tax treatment. This ensures CapEx items are not expensed "
        "and deposits are not recorded as revenue.", styles))

    story.append(Paragraph("4.6  Learning Memory and Vendor Intelligence", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI maintains a learning memory system that improves over time. "
        "Every manual correction made by a reviewer is recorded and used to improve "
        "future suggestions.", styles["Body"]))

    learning_rows = [
        ["Correction Type", "How It's Learned", "Future Behaviour"],
        ["Vendor GL change", "Reviewer changes GL account for a vendor",
         "Next invoice from same vendor auto-suggests the corrected GL"],
        ["Tax code change", "Reviewer changes tax code (e.g., T to M)",
         "Next invoice from same vendor auto-suggests the corrected tax code"],
        ["Vendor name correction", "Reviewer fixes misspelled vendor name",
         "OCR learns the correct spelling; future fuzzy matches improve"],
        ["Amount correction", "Reviewer overrides extracted amount",
         "Hallucination guard learns to flag similar discrepancies"],
        ["Date correction", "Reviewer fixes incorrect date extraction",
         "AI extraction model improves date parsing for similar formats"],
    ]
    story.append(std_table(learning_rows[0], learning_rows[1:], [1.4*inch, 2.5*inch, 2.6*inch]))
    story.append(Spacer(1, 8))
    story.extend(tip_box(
        "The learning memory reaches useful accuracy after approximately 50 corrections "
        "per vendor. For high-volume vendors (e.g., monthly invoices from the same "
        "supplier), accuracy improves rapidly. For one-time vendors, the system relies "
        "on general patterns and tax engine rules rather than vendor-specific memory.", styles))

    story.append(Paragraph("Confidence Scoring", styles["H3"]))
    story.append(Paragraph(
        "Every AI extraction includes a confidence score (0.00 to 1.00). The score "
        "determines the workflow path:", styles["Body"]))
    conf_rows = [
        ["Score Range", "Label", "Workflow Impact"],
        ["0.90 – 1.00", "High confidence", "Auto-approval eligible (if no fraud/substance flags)"],
        ["0.70 – 0.89", "Medium confidence", "Needs human review; AI suggestion shown with amber indicator"],
        ["0.50 – 0.69", "Low confidence", "Needs human review; AI suggestion shown with red warning"],
        ["Below 0.50", "Very low confidence", "Routed to exception queue; OCR quality likely insufficient"],
    ]
    story.append(std_table(conf_rows[0], conf_rows[1:], [1.2*inch, 1.5*inch, 3.8*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Explain Decision Feature", styles["H3"]))
    story.append(Paragraph(
        "Every AI decision can be explained by clicking the Explain button on the "
        "document card. The explanation shows:", styles["Body"]))
    story.extend(bullets([
        "Which AI provider was used (DeepSeek or OpenRouter) and the task type",
        "The extraction confidence score with a breakdown by field",
        "Which fraud rules were triggered and why",
        "Which substance flags were raised and the triggering keywords",
        "The vendor memory match (if any) and the suggested GL account source",
        "The tax code determination logic (tax engine rule or AI suggestion)",
        "Whether the hallucination guard intervened and what it corrected",
        "The full AI prompt and response (available in Technical tab for debugging)",
    ], styles))
    story.append(PageBreak())

# ── Section 5: Quebec Tax Compliance ─────────────────────────────────────────
def build_section_5_quebec_tax(story, styles):
    story.append(Paragraph("Section 5 — Quebec Tax Compliance", styles["H1"]))
    story.extend(section_rule(styles))

    story.append(Paragraph("5.1  GST/QST Rates", styles["H2"]))
    story.append(Paragraph(
        "Quebec businesses are subject to two levels of sales tax: the federal Goods "
        "and Services Tax (GST) administered by the Canada Revenue Agency (CRA), and "
        "the Quebec Sales Tax (QST) administered by Revenu Quebec. Both taxes apply "
        "to most commercial transactions.", styles["Body"]))

    rate_rows = [
        ["Tax", "Rate", "Administrator", "Registration Threshold", "Filing Authority"],
        ["GST (TPS)", "5.000%", "Canada Revenue Agency", "$30,000 annual revenue", "CRA My Business Account"],
        ["QST (TVQ)", "9.975%", "Revenu Quebec", "$30,000 annual revenue", "Revenu Quebec clicSEQUR"],
        ["Combined", "14.975%", "Both", "Register for both separately", "File separately"],
    ]
    story.append(std_table(rate_rows[0], rate_rows[1:], [0.8*inch, 0.8*inch, 1.5*inch, 1.5*inch, 1.9*inch]))
    story.append(Spacer(1, 8))

    story.extend(tip_box(
        "The QST rate of 9.975% is calculated on the pre-GST amount (not on the "
        "GST-inclusive amount). LedgerLink AI always uses this base-on-base method. "
        "Example: $100 subtotal x 5% GST = $5.00; $100 x 9.975% QST = $9.975. "
        "Total = $114.975.", styles))

    story.append(Paragraph("5.2  Tax Codes T, Z, E, M, I", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI assigns a single-letter tax code to every document. This code "
        "drives the ITC/ITR recovery calculation and the Revenu Quebec filing summary.", styles["Body"]))

    code_rows = [
        ["Code", "Name", "GST Rate", "QST Rate", "ITC Recovery", "ITR Recovery", "Typical Use"],
        ["T", "Standard Taxable", "5%", "9.975%", "100%", "100%", "Most business purchases: office supplies, professional fees, equipment"],
        ["Z", "Zero-Rated", "0%", "0%", "None", "None", "Exports, basic groceries, prescription drugs, medical devices"],
        ["E", "Exempt", "0%", "0%", "None", "None", "Residential rent, health/dental, life insurance, financial services"],
        ["M", "Meals (50%)", "5%", "9.975%", "50%", "50%", "Restaurant meals, entertainment — CRA/Revenu Quebec limits recovery"],
        ["I", "Insurance (QC)", "0%", "9%*", "None", "None", "Quebec insurance premiums (non-recoverable 9% QST applies)"],
    ]
    story.append(std_table(code_rows[0], code_rows[1:], [0.4*inch, 1.0*inch, 0.6*inch, 0.7*inch, 0.8*inch, 0.8*inch, 2.2*inch]))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "* Insurance (Code I): The 9% Quebec insurance tax is a separate levy, not the standard QST. "
        "It is non-recoverable and does not generate an ITR.", styles["Caption"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Tax Code Examples", styles["H3"]))
    ex_rows = [
        ["Vendor", "Description", "Subtotal", "GST", "QST", "Total", "Code"],
        ["Bell Canada", "Internet service", "$200.00", "$10.00", "$19.95", "$229.95", "T"],
        ["Loblaws", "Office coffee/snacks", "$85.00", "$0.00", "$0.00", "$85.00", "E*"],
        ["Restaurant Le Pois", "Client lunch", "$120.00", "$6.00", "$11.97", "$137.97", "M"],
        ["Intact Assurance", "Business insurance", "$1,500.00", "$0.00", "$135.00", "$1,635.00", "I"],
        ["Manufacture client", "Export of goods", "$5,000.00", "$0.00", "$0.00", "$5,000.00", "Z"],
        ["Acco Brands", "Pens, paper, binders", "$45.00", "$2.25", "$4.49", "$51.74", "T"],
    ]
    story.append(std_table(ex_rows[0], ex_rows[1:], [1.2*inch, 1.4*inch, 0.8*inch, 0.6*inch, 0.6*inch, 0.8*inch, 0.5*inch]))
    story.append(Spacer(1, 6))
    story.append(Paragraph("* Basic groceries are exempt; prepared food and snacks may be taxable. LedgerLink AI flags ambiguous food purchases for manual review.", styles["Caption"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("5.3  ITC/ITR Recovery", styles["H2"]))
    story.append(Paragraph(
        "Input Tax Credits (ITC) recover GST paid on business purchases. Input Tax "
        "Refunds (ITR) recover QST paid. LedgerLink AI calculates both automatically "
        "based on the assigned tax code.", styles["Body"]))

    itc_rows = [
        ["Code", "ITC Formula", "ITR Formula", "Notes"],
        ["T", "GST amount x 100%", "QST amount x 100%", "Full recovery — most common"],
        ["Z", "$0.00", "$0.00", "No tax collected or paid"],
        ["E", "$0.00", "$0.00", "Exempt — no recovery available"],
        ["M", "GST amount x 50%", "QST amount x 50%", "CRA meals limitation"],
        ["I", "$0.00", "$0.00", "Insurance levy — not recoverable"],
    ]
    story.append(std_table(itc_rows[0], itc_rows[1:], [0.5*inch, 1.8*inch, 1.8*inch, 2.4*inch]))
    story.append(Spacer(1, 8))

    story.extend(warning_box(
        "ITC claims must be supported by valid tax invoices. A valid GST/QST invoice "
        "must show: supplier name, GST/QST registration numbers, date, description, "
        "amount before tax, and tax amounts. LedgerLink AI flags documents missing "
        "registration numbers for manual verification.", styles))

    story.append(Paragraph("5.4  Filing Summary and Revenu Quebec Pre-Fill", styles["H2"]))
    story.append(Paragraph(
        "The Filing Summary report aggregates all posted documents for a filing "
        "period and generates the totals needed for GST/QST returns. The Revenu "
        "Quebec pre-fill feature exports these totals in a format compatible with "
        "the clicSEQUR-Entreprises filing portal.", styles["Body"]))

    summary_rows = [
        ["Line", "Description", "Source"],
        ["101", "Total sales and revenue", "Sum of all document subtotals (taxable + zero-rated)"],
        ["105", "GST collected", "Sum of GST on outbound invoices (if applicable)"],
        ["106", "Adjustments to GST collected", "Credit notes, adjustments"],
        ["108", "Total ITC claimed", "Sum of ITC from Code T and 50% from Code M"],
        ["109", "Net GST remittance (105+106-108)", "Calculated automatically"],
        ["210", "QST collected", "Sum of QST on outbound invoices"],
        ["212", "Total ITR claimed", "Sum of ITR from Code T and 50% from Code M"],
        ["214", "Net QST remittance (210-212)", "Calculated automatically"],
    ]
    story.append(std_table(summary_rows[0], summary_rows[1:], [0.5*inch, 2.2*inch, 3.8*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("5.5  Quick Method and Deadline Calendar", styles["H2"]))
    story.append(Paragraph(
        "The Quick Method is a simplified GST/QST remittance option available to "
        "eligible small businesses. Instead of claiming ITCs/ITRs, the business "
        "remits a fixed percentage of gross revenue. LedgerLink AI supports Quick "
        "Method clients — flag them in their client profile.", styles["Body"]))

    qm_rows = [
        ["Business Type", "GST Quick Rate", "QST Quick Rate"],
        ["Service businesses", "3.6%", "6.6%"],
        ["Goods-resale businesses", "1.8%", "3.4%"],
        ["Threshold (max revenue)", "$400,000/year", "$400,000/year"],
    ]
    story.append(std_table(qm_rows[0], qm_rows[1:], [2.4*inch, 2.0*inch, 2.1*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Filing Deadline Calendar", styles["H3"]))
    deadline_rows = [
        ["Filing Frequency", "Period End", "Filing Deadline", "Payment Deadline"],
        ["Monthly", "Last day of month", "Last day of following month", "Same as filing"],
        ["Quarterly", "Mar 31 / Jun 30 / Sep 30 / Dec 31", "Last day of month after quarter", "Same as filing"],
        ["Annual", "Fiscal year end", "3 months after fiscal year end", "Same as filing"],
        ["Annual (individual)", "December 31", "June 15 of following year", "April 30 of following year"],
    ]
    story.append(std_table(deadline_rows[0], deadline_rows[1:], [1.2*inch, 1.8*inch, 2.0*inch, 1.5*inch]))
    story.append(Spacer(1, 8))
    story.extend(warning_box(
        "Late filing penalties start at $250 and escalate to 10% of net tax owing "
        "for repeat offenders. LedgerLink AI sends email reminders 14 days and "
        "3 days before each filing deadline. Ensure the digest SMTP settings are "
        "configured correctly.", styles))

    story.append(Paragraph("5.6  HST and Provincial Sales Tax — All Provinces", styles["H2"]))
    story.append(Paragraph(
        "While LedgerLink AI is optimised for Quebec (GST + QST), it also handles "
        "interprovincial transactions correctly. The table below shows the sales tax "
        "regime for every Canadian province and territory.", styles["Body"]))

    prov_rows = [
        ["Province / Territory", "Tax System", "GST Rate", "PST / QST Rate", "Combined Rate"],
        ["Alberta", "GST only", "5%", "—", "5%"],
        ["British Columbia", "GST + PST", "5%", "7% PST", "12%"],
        ["Manitoba", "GST + RST", "5%", "7% RST", "12%"],
        ["New Brunswick", "HST", "—", "—", "15%"],
        ["Newfoundland and Labrador", "HST", "—", "—", "15%"],
        ["Northwest Territories", "GST only", "5%", "—", "5%"],
        ["Nova Scotia", "HST", "—", "—", "15%"],
        ["Nunavut", "GST only", "5%", "—", "5%"],
        ["Ontario", "HST", "—", "—", "13%"],
        ["Prince Edward Island", "HST", "—", "—", "15%"],
        ["Quebec", "GST + QST", "5%", "9.975% QST", "14.975%"],
        ["Saskatchewan", "GST + PST", "5%", "6% PST", "11%"],
        ["Yukon", "GST only", "5%", "—", "5%"],
    ]
    story.append(std_table(prov_rows[0], prov_rows[1:], [1.6*inch, 1.0*inch, 0.7*inch, 1.1*inch, 1.0*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph(
        "When processing an interprovincial invoice, LedgerLink AI looks at the "
        "vendor's province (from the vendor address) and the client's province to "
        "determine which taxes apply. The general rule is: the <b>place of supply</b> "
        "determines the tax. For services, this is typically the buyer's province. "
        "For goods, it is the province where the goods are delivered.", styles["Body"]))
    story.extend(tip_box(
        "HST provinces use a single combined tax. The ITC recovery for HST is "
        "calculated on the full HST amount, not split between federal and provincial. "
        "Example: Ontario HST at 13% on a $100 purchase = $13.00 HST. ITC = $13.00 "
        "(full recovery). This differs from Quebec where GST and QST are tracked "
        "separately.", styles))

    story.append(Paragraph("HST Tax Code in LedgerLink AI", styles["H3"]))
    story.append(Paragraph(
        "LedgerLink AI uses the special tax code HST for invoices from HST provinces. "
        "The HST code triggers different ITC calculations than the standard T code:", styles["Body"]))
    hst_code_rows = [
        ["Code", "GST Component", "Provincial Component", "ITC Recovery", "ITR Recovery"],
        ["T (QC)", "5%", "9.975% QST", "100% of GST", "100% of QST"],
        ["HST (ON)", "5% (embedded)", "8% (embedded)", "100% of HST", "N/A (single tax)"],
        ["HST (NB/NS/NL/PE)", "5% (embedded)", "10% (embedded)", "100% of HST", "N/A"],
        ["M (meals, QC)", "5%", "9.975% QST", "50% of GST", "50% of QST"],
        ["M (meals, ON HST)", "5% (embedded)", "8% (embedded)", "50% of HST", "N/A"],
    ]
    story.append(std_table(hst_code_rows[0], hst_code_rows[1:], [1.3*inch, 1.2*inch, 1.3*inch, 1.2*inch, 1.5*inch]))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "When a Quebec firm receives an invoice from an Ontario vendor, LedgerLink AI "
        "assigns code HST and records the full 13% as a single amount. The ITC claim "
        "is filed on the federal GST return (line 108). No QST ITR applies because "
        "no QST was charged.", styles["Body"]))

    story.append(Paragraph("GST_ONLY Tax Code", styles["H3"]))
    story.append(Paragraph(
        "The GST_ONLY code applies to purchases from provinces that charge only GST "
        "(Alberta, territories). In these cases, only a 5% GST is charged. The ITC "
        "is 100% recoverable. No provincial tax component exists.", styles["Body"]))
    story.append(PageBreak())

    # ── 5.7  Quebec Payroll Compliance ────────────────────────────────────────
    story.append(Paragraph("5.7  Quebec Payroll Compliance", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI includes a payroll validation engine (src/engines/payroll_engine.py) "
        "that checks Quebec-specific payroll deductions and employer contributions. This "
        "section covers the key payroll components unique to Quebec.", styles["Body"]))

    story.append(Paragraph("Quebec Pension Plan (QPP / RRQ)", styles["H3"]))
    story.append(Paragraph(
        "Quebec employees contribute to the Quebec Pension Plan (QPP, or RRQ in French: "
        "Regime de rentes du Quebec) instead of the Canada Pension Plan (CPP). This is "
        "a critical distinction — using CPP rates for Quebec employees is a common error.", styles["Body"]))
    qpp_rows = [
        ["Component", "2026 Rate", "Maximum Pensionable Earnings", "Basic Exemption"],
        ["QPP Base (employee)", "6.40%", "$71,300", "$3,500"],
        ["QPP Base (employer)", "6.40%", "$71,300", "$3,500"],
        ["QPP2 (employee)", "4.00%", "$81,200 (YAMPE)", "Above $71,300"],
        ["QPP2 (employer)", "4.00%", "$81,200 (YAMPE)", "Above $71,300"],
    ]
    story.append(std_table(qpp_rows[0], qpp_rows[1:], [1.5*inch, 1.0*inch, 2.0*inch, 2.0*inch]))
    story.append(Spacer(1, 8))
    story.extend(warning_box(
        "LedgerLink AI validates that Quebec employees use QPP rates (not CPP). If a "
        "payroll file uses CPP rates for a Quebec employee, the payroll engine flags "
        "it as an error. CPP rates differ from QPP rates and applying the wrong plan "
        "results in incorrect remittances to both CRA and Revenu Quebec.", styles))

    story.append(Paragraph("Quebec Parental Insurance Plan (QPIP / RQAP)", styles["H3"]))
    story.append(Paragraph(
        "Quebec employees pay QPIP premiums (Regime quebecois d'assurance parentale) "
        "instead of the Employment Insurance (EI) parental benefits component. QPIP "
        "is administered by the Conseil de gestion de l'assurance parentale.", styles["Body"]))
    qpip_rows = [
        ["Component", "Employee Rate", "Employer Rate", "Maximum Insurable Earnings"],
        ["QPIP", "0.494%", "0.692%", "$94,000"],
        ["EI (rest of Canada)", "1.64%", "2.296% (1.4x)", "$65,700"],
        ["EI (Quebec reduced)", "1.32%", "1.848% (1.4x)", "$65,700"],
    ]
    story.append(std_table(qpip_rows[0], qpip_rows[1:], [1.5*inch, 1.2*inch, 1.2*inch, 2.6*inch]))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "Quebec employees pay a reduced EI rate because maternity/parental benefits "
        "are covered by QPIP. LedgerLink AI applies both the reduced EI rate and "
        "the QPIP rate automatically when the employee province is Quebec.", styles["Body"]))

    story.append(Paragraph("Health Services Fund (HSF / FSS)", styles["H3"]))
    story.append(Paragraph(
        "The Fonds des services de sante (FSS, or HSF in English) is a Quebec "
        "employer-only contribution. The rate depends on the employer's total "
        "payroll. There is no employee deduction.", styles["Body"]))
    hsf_rows = [
        ["Total Annual Payroll", "HSF Rate", "Notes"],
        ["$1,000,000 or less", "1.25%", "Small employer minimum rate"],
        ["$1,000,001 to $7,000,000", "1.25% to 4.26%", "Linear interpolation between thresholds"],
        ["$7,000,001 or more", "4.26%", "Maximum rate — large employers"],
        ["Primary/manufacturing sector", "1.25% to 2.31%", "Reduced rate for eligible sectors"],
    ]
    story.append(std_table(hsf_rows[0], hsf_rows[1:], [1.8*inch, 1.2*inch, 3.5*inch]))
    story.append(Spacer(1, 8))
    story.extend(tip_box(
        "The HSF rate changes each year and depends on the prior year's total payroll. "
        "LedgerLink AI validates that the HSF rate entered for each client falls within "
        "the correct band for their payroll level. An incorrect HSF rate will trigger "
        "a validation warning.", styles))

    story.append(Paragraph("CNESST (Workers Compensation)", styles["H3"]))
    story.append(Paragraph(
        "The CNESST (Commission des normes, de l'equite, de la sante et de la securite "
        "du travail) premium is Quebec's workplace safety insurance. The rate varies by "
        "industry classification unit code. LedgerLink AI maintains a table of 20+ "
        "industry codes with their corresponding rates.", styles["Body"]))
    cnesst_rows = [
        ["Industry Code", "Sector", "2026 Rate (per $100 payroll)"],
        ["54110", "Professional services", "$0.42"],
        ["52100", "Banking and finance", "$0.24"],
        ["23610", "Residential construction", "$6.85"],
        ["44110", "Grocery stores", "$1.72"],
        ["72110", "Hotels and restaurants", "$2.18"],
        ["62100", "Healthcare offices", "$0.98"],
        ["48100", "Trucking", "$4.53"],
        ["31310", "Food manufacturing", "$3.21"],
        ["81100", "Auto repair", "$2.95"],
        ["56130", "Janitorial / cleaning", "$3.67"],
    ]
    story.append(std_table(cnesst_rows[0], cnesst_rows[1:], [1.3*inch, 2.5*inch, 2.7*inch]))
    story.append(Spacer(1, 8))
    story.extend(warning_box(
        "CNESST rates are set annually by industry code. Ensure the correct unit code "
        "is entered in each client's profile. An incorrect code can result in significant "
        "over- or under-payment of premiums.", styles))

    story.append(Paragraph("RL-1 and T4 Reconciliation", styles["H3"]))
    story.append(Paragraph(
        "Quebec employers must file RL-1 slips (Releve 1) with Revenu Quebec in addition "
        "to T4 slips with the CRA. LedgerLink AI reconciles the amounts between "
        "the two forms to ensure consistency.", styles["Body"]))

    rl1_rows = [
        ["RL-1 Box", "T4 Box", "Description"],
        ["Box A — Employment income", "Box 14 — Employment income", "Gross salary/wages including taxable benefits"],
        ["Box B — QPP contributions", "Box 16 — CPP (should be $0 for QC)", "QPP deducted; T4 shows CPP=0 for Quebec employees"],
        ["Box C — EI premiums", "Box 18 — EI premiums", "Reduced EI rate for Quebec employees"],
        ["Box D — RPP contributions", "Box 20 — RPP contributions", "Registered pension plan deductions"],
        ["Box E — Income tax deducted", "Box 22 — Income tax deducted", "Federal + provincial; RL-1 shows QC tax only"],
        ["Box G — QPIP premiums", "N/A (not on T4)", "QPIP is Quebec-only; no T4 equivalent"],
        ["Box H — QPIP insurable earnings", "N/A", "QPIP insurable earnings up to maximum"],
        ["Box I — Union dues", "Box 44 — Union dues", "Must match between both forms"],
    ]
    story.append(std_table(rl1_rows[0], rl1_rows[1:], [1.8*inch, 1.8*inch, 2.9*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph(
        "LedgerLink AI runs an automated reconciliation between RL-1 and T4 amounts. "
        "Discrepancies are flagged for review. The most common error is entering CPP "
        "contributions on a T4 for a Quebec employee — Box 16 on the T4 should always "
        "be $0.00 for employees contributing to QPP via the RL-1.", styles["Body"]))
    story.extend(tip_box(
        "File RL-1 slips electronically via Revenu Quebec's clicSEQUR portal. The "
        "filing deadline is the last day of February for the prior calendar year. "
        "T4 slips are filed with the CRA by the same deadline.", styles))
    story.append(PageBreak())

# ── Section 6: Client Portal ──────────────────────────────────────────────────
def build_section_6_client_portal(story, styles):
    story.append(Paragraph("Section 6 — Client Portal", styles["H1"]))
    story.extend(section_rule(styles))

    story.append(Paragraph("6.1  Creating Client Accounts", styles["H2"]))
    story.append(Paragraph(
        "The Client Portal (port 8788) provides a secure, simplified interface "
        "for end clients to submit documents and track their status. Client accounts "
        "are isolated from the main dashboard — clients cannot see GL codes, "
        "tax codes, or internal reviewer notes.", styles["Body"]))

    story.extend(warning_box(
        "Always create client accounts through Administration, then Client Portal, "
        "then Add Client — not through the main User Management screen. This ensures "
        "the role is set to client and access is correctly restricted to port 8788.", styles))

    create_steps = [
        "Go to Administration, then Client Portal, then Add Client",
        "Enter the client company name and a unique username",
        "Enter the client contact email address",
        "Set a temporary password and tick Must Reset on First Login",
        "Select the language preference (EN or FR) for the client portal UI",
        "Click Save — credentials can be emailed to the client via the Send Credentials button",
    ]
    for i, step in enumerate(create_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    client_rows = [
        ["Field", "Visible to Client", "Notes"],
        ["Document status", "YES (plain language)", "Needs Review shown as Under Review; Posted shown as Complete"],
        ["GL account", "NO", "Internal accounting field — never shown to clients"],
        ["Tax code", "NO", "Internal field"],
        ["Reviewer notes", "Partial", "Only notes marked Share with Client are visible"],
        ["Amount", "YES", "Extracted amount visible for confirmation"],
        ["Vendor name", "YES", "Client can correct if AI extracted wrong vendor"],
        ["Invoice date", "YES", "Client can confirm or correct"],
        ["Fraud flags", "NO", "Internal — never shown to clients"],
    ]
    story.append(std_table(client_rows[0], client_rows[1:], [1.5*inch, 1.3*inch, 3.7*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("6.2  Document Submission Methods", styles["H2"]))
    story.append(Paragraph(
        "Clients can submit documents through the portal using three methods:", styles["Body"]))
    story.extend(bullets([
        "Drag and drop — drag PDF or image files directly onto the upload zone",
        "Browse — click the file picker button to select files from the local computer",
        "Mobile camera — on smartphones, the file picker opens the camera for direct photo capture",
    ], styles))
    story.append(Paragraph(
        "Each uploaded file is processed immediately through the standard pipeline. "
        "The client sees a status badge update within 2-3 minutes under normal load. "
        "The following file types are accepted: PDF, PNG, JPG, JPEG. Maximum size "
        "per file: 20 MB (configurable in ledgerlink.config.json).", styles["Body"]))

    story.extend(tip_box(
        "Advise clients to scan documents at 300 DPI minimum for best OCR accuracy. "
        "Photos taken in good lighting with a smartphone at 8 MP or higher also "
        "work well. Blurry or dark images result in lower AI confidence and may "
        "require manual data entry.", styles))

    story.append(Paragraph("Batch Submission", styles["H3"]))
    story.append(Paragraph(
        "Clients can select multiple files at once in the file picker. The portal "
        "queues them and processes each file sequentially. A progress indicator shows "
        "how many files remain. There is no hard limit on batch size, but very large "
        "batches (100+ files) should be submitted in multiple sessions to avoid "
        "browser timeout.", styles["Body"]))

    story.append(Paragraph("6.3  Cloudflare Tunnel", styles["H2"]))
    story.append(Paragraph(
        "By default, the client portal is only accessible from the local network "
        "(http://127.0.0.1:8788). To allow clients outside the office to access the "
        "portal, set up a Cloudflare Tunnel (formerly Argo Tunnel). This is the "
        "recommended approach — it is free, requires no port forwarding, and provides "
        "automatic TLS encryption.", styles["Body"]))

    tunnel_steps = [
        "Install cloudflared on the server: winget install Cloudflare.cloudflared",
        "Authenticate: cloudflared tunnel login (opens browser for Cloudflare account sign-in)",
        "Create a tunnel: cloudflared tunnel create ledgerlink-portal",
        "Configure the tunnel to point to http://127.0.0.1:8788",
        "Run scripts/setup_cloudflare.py for automated tunnel configuration",
        "Start the tunnel as a Windows service: cloudflared service install",
        "The portal is now accessible at https://portal.yourfirm.com (custom domain)",
    ]
    for i, step in enumerate(tunnel_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    story.extend(warning_box(
        "Do not expose the main dashboard (port 8787) through the Cloudflare tunnel. "
        "The dashboard is for internal staff only. Only the client portal (port 8788) "
        "should be internet-accessible. Port 8789 (ingest API) should also remain "
        "internal or use a separate secured tunnel.", styles))

    story.append(Paragraph("Cloudflare Access (Optional)", styles["H3"]))
    story.append(Paragraph(
        "For an additional layer of security, configure Cloudflare Access in front of "
        "the portal. This adds an email-based one-time-password gate before the "
        "LedgerLink login screen. Useful for high-value clients requiring extra "
        "verification.", styles["Body"]))

    story.append(Paragraph("6.4  Client Communications", styles["H2"]))
    story.append(Paragraph(
        "Accountants can send messages directly to clients through the Client "
        "Communications module (src/agents/core/client_comms.py). Messages appear in "
        "the client portal and can optionally be sent as emails.", styles["Body"]))

    story.extend(bullets([
        "Send a message: open the document card, click Message Client, type and send",
        "Mark a reviewer note as shared: toggle Share with Client in the note editor",
        "View message history: Client Portal, then Communications tab, then select client",
        "Email notifications: when enabled, the client receives an email for each new message",
        "Messages are stored in the client_communications table with full audit trail",
        "Clients can reply through the portal; replies appear in the accountant dashboard",
    ], styles))

    story.extend(tip_box(
        "Use client communications to request missing documents. The client receives "
        "the message in plain language with a link to re-upload. This keeps all "
        "correspondence in the system rather than in individual email inboxes.", styles))

    story.append(Paragraph("Daily Digest for Clients", styles["H3"]))
    story.append(Paragraph(
        "The daily digest (scripts/daily_digest.py) can be configured to send a "
        "summary email to each client showing: documents received today, documents "
        "still under review, and any messages from their accountant. Configure "
        "SMTP settings in ledgerlink.config.json under the digest section.", styles["Body"]))

    story.append(Paragraph("6.5  QR Codes for Client Onboarding", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI can generate unique QR codes for each client. When scanned, "
        "the QR code opens the client portal login page pre-filled with the client's "
        "username. This simplifies onboarding — print the QR code on business cards "
        "or engagement letters.", styles["Body"]))

    qr_steps = [
        "Navigate to Administration, then QR Codes",
        "Select one or more clients from the list",
        "Click Generate QR Code — a unique code is created for each client",
        "Download individual QR codes as PNG files (QR, then Download)",
        "Download all client QR codes as a single PDF (QR, then Export All PDF)",
        "The PDF includes one QR code per page with the client name and login URL",
        "Print and distribute to clients — they scan with any smartphone camera",
    ]
    for i, step in enumerate(qr_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    qr_config_rows = [
        ["Setting", "Default", "Description"],
        ["Portal URL in QR", "https://portal.yourfirm.com", "Must match Cloudflare tunnel domain"],
        ["QR image size", "300x300 px", "Suitable for print and screen"],
        ["Error correction", "Level M (15%)", "QR remains scannable even if partially obscured"],
        ["Logo overlay", "Optional", "Place firm logo in centre of QR code"],
    ]
    story.append(std_table(qr_config_rows[0], qr_config_rows[1:], [1.5*inch, 1.8*inch, 3.2*inch]))
    story.append(Spacer(1, 8))
    story.extend(tip_box(
        "Include the QR code in your engagement letter template. New clients can scan "
        "the code and immediately start submitting documents through the portal — no "
        "manual URL typing required.", styles))

    story.append(Paragraph("6.6  WhatsApp and Telegram via OpenClaw", styles["H2"]))
    story.append(Paragraph(
        "OpenClaw is the messaging bridge that connects LedgerLink AI to WhatsApp "
        "and Telegram. Clients can submit documents by sending photos or PDFs "
        "directly in a chat conversation. The OpenClaw bridge receives messages, "
        "extracts attachments, and forwards them to the LedgerLink ingest endpoint.", styles["Body"]))

    story.append(Paragraph("How the OpenClaw Bridge Works", styles["H3"]))
    story.extend(bullets([
        "OpenClaw runs as a separate service alongside LedgerLink AI",
        "It connects to WhatsApp Business API and/or Telegram Bot API",
        "When a client sends a document in chat, OpenClaw receives the file",
        "The file is POSTed to http://127.0.0.1:8787/ingest/openclaw with client metadata",
        "LedgerLink processes the document through the standard pipeline",
        "The client receives a confirmation reply in the chat: 'Document received — processing'",
        "Status updates can be sent back to the client via the same chat channel",
    ], styles))

    story.append(Paragraph("Setting Up WhatsApp Integration", styles["H3"]))
    openclaw_wa_steps = [
        "Register for WhatsApp Business API through Meta Business Suite",
        "Obtain the WhatsApp Business API token and phone number ID",
        "Configure the token in ledgerlink.config.json under openclaw.whatsapp",
        "Set the webhook URL to the OpenClaw endpoint (e.g., https://openclaw.yourfirm.com/webhook)",
        "Run scripts/run_openclaw_queue.py to start the bridge service",
        "Test by sending a PDF to the WhatsApp Business number from your personal phone",
        "Register client phone numbers in their client profile for automatic routing",
    ]
    for i, step in enumerate(openclaw_wa_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Setting Up Telegram Integration", styles["H3"]))
    openclaw_tg_steps = [
        "Create a Telegram Bot via @BotFather — record the bot token",
        "Configure the bot token in ledgerlink.config.json under openclaw.telegram",
        "Set the webhook URL to the OpenClaw Telegram endpoint",
        "Run scripts/run_openclaw_queue.py (handles both WhatsApp and Telegram)",
        "Share the bot link with clients — they send documents directly to the bot",
        "The bot replies with processing status and document ID for tracking",
    ]
    for i, step in enumerate(openclaw_tg_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))
    story.extend(warning_box(
        "WhatsApp and Telegram transmit documents through third-party servers. Ensure "
        "your firm's privacy policy covers this data flow. For sensitive documents, "
        "advise clients to use the secure client portal instead.", styles))

    story.append(Paragraph("OpenClaw Message Routing", styles["H3"]))
    story.append(Paragraph(
        "OpenClaw determines which client sent the document by matching the sender's "
        "phone number against the client_registry table. If the phone number is not "
        "registered, the document is placed in the exception queue with status "
        "'Unknown Sender' for manual routing by the accountant.", styles["Body"]))
    routing_rows = [
        ["Scenario", "Behaviour"],
        ["Known client phone number", "Document routed to correct client file automatically"],
        ["Unknown phone number", "Document placed in exception queue — manual assignment required"],
        ["Multiple clients same phone", "Document flagged for manual routing — ambiguous sender"],
        ["File too large (>20 MB)", "Rejected — client receives error message in chat"],
        ["Unsupported file type", "Rejected — client receives message listing accepted formats"],
        ["Client sends text only", "Ignored — OpenClaw only processes file attachments"],
    ]
    story.append(std_table(routing_rows[0], routing_rows[1:], [2.0*inch, 4.5*inch]))
    story.append(Spacer(1, 8))
    story.append(PageBreak())


# ── Section 7: Month-End and Billing ─────────────────────────────────────────
def build_section_7_monthend(story, styles):
    story.append(Paragraph("Section 7 — Month-End and Billing", styles["H1"]))
    story.extend(section_rule(styles))

    story.append(Paragraph("7.1  Month-End Checklist", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI provides a structured month-end close process managed by the "
        "period_close module. The checklist ensures all steps are completed before "
        "the period is locked.", styles["Body"]))

    checklist_rows = [
        ["Step", "Task", "Auto or Manual"],
        ["1", "Confirm all documents for the period are in Posted status", "Manual — review queue filter by date range"],
        ["2", "Run bank reconciliation — match all bank transactions to documents", "Auto (bank_matcher) + Manual review"],
        ["3", "Review and resolve all On Hold documents", "Manual"],
        ["4", "Verify GST/QST totals match bank remittances", "Manual — compare Filing Summary report"],
        ["5", "Review fraud flags — document resolution for any approved flagged items", "Manual"],
        ["6", "Generate trial balance from posted documents", "Auto — Audit Module, then Trial Balance"],
        ["7", "Lock the period to prevent further changes", "Manual — Period Close, then Lock Period"],
    ]
    story.append(std_table(checklist_rows[0], checklist_rows[1:], [0.4*inch, 3.5*inch, 2.6*inch]))
    story.append(Spacer(1, 8))

    story.extend(tip_box(
        "Run the checklist on the last business day of the month. Locking the period "
        "is irreversible without owner intervention — confirm with your team before "
        "clicking Lock.", styles))

    story.append(Paragraph("7.2  Locking Periods", styles["H2"]))
    story.append(Paragraph(
        "A locked period prevents any document in that period from being edited, "
        "approved, or posted. Lock records are stored in the period_close_locks table. "
        "Locks can only be removed by an owner-role user.", styles["Body"]))
    story.extend(warning_box(
        "Locking a period is a compliance action. Once locked, the audit trail "
        "is considered final. Unlocking a locked period should be treated as a "
        "significant event and documented in the engagement file.", styles))

    lock_steps = [
        "Navigate to Period Close in the main dashboard navigation",
        "Select the month and year to lock",
        "Review the checklist — all 7 items must show green checkmarks",
        "Click Lock Period — a confirmation dialog appears",
        "Enter your password to confirm the lock",
        "The period is now locked — the lock icon appears in the period selector",
    ]
    for i, step in enumerate(lock_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("7.3  Time Tracking", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI includes a built-in time tracker for billable work. Time "
        "entries are linked to clients and can be exported for invoice generation. "
        "The time_tracker module stores entries in the time_entries table.", styles["Body"]))

    story.extend(bullets([
        "Start a timer: Dashboard, then Time Tracker, then Start Timer, then select client",
        "Manual entry: Dashboard, then Time Tracker, then Add Entry, then enter start/end times",
        "Adjust rate: each entry can have a custom hourly rate or inherit the client default",
        "Add description: required field — describe the work performed",
        "Export: Time Tracker, then Export, then select date range and client",
        "Time entries are linked to the audit_log for compliance tracking",
    ], styles))

    story.extend(tip_box(
        "Use the browser tab timer for accurate time tracking. Do not rely on memory "
        "for time entries — start the timer when you begin work and stop it when "
        "you finish. Rounding rules (e.g., nearest 6 minutes) can be set per client.", styles))

    story.append(Paragraph("7.4  Invoice Generation", styles["H2"]))
    story.append(Paragraph(
        "Invoices are generated from time entries using the invoice_generator module. "
        "Invoices include GST/QST amounts based on the firm's own tax registration "
        "and can be exported as PDF for delivery to clients.", styles["Body"]))

    invoice_rows = [
        ["Field", "Source"],
        ["Invoice number", "Auto-incremented from last invoice in invoices table"],
        ["Client name/address", "Client profile"],
        ["Service period", "Date range of included time entries"],
        ["Line items", "One row per time entry (date, description, hours, rate, subtotal)"],
        ["Subtotal", "Sum of all line items"],
        ["GST (5%)", "Subtotal x 5% (firm is GST registrant)"],
        ["QST (9.975%)", "Subtotal x 9.975% (firm is QST registrant)"],
        ["Total", "Subtotal + GST + QST"],
        ["Payment terms", "Configurable: Net 15, Net 30, Due on Receipt"],
    ]
    story.append(std_table(invoice_rows[0], invoice_rows[1:], [1.8*inch, 4.7*inch]))
    story.append(Spacer(1, 8))

    story.extend(warning_box(
        "Invoice numbers are permanent. Once an invoice is generated, its number "
        "cannot be reused even if the invoice is cancelled. This is required for "
        "Quebec tax compliance — sequential invoice numbering is mandatory for "
        "GST/QST registrants.", styles))

    story.append(Paragraph("7.5  Bank Reconciliation — Two-Sided Statement", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI performs bank reconciliation using the two-sided method. The "
        "reconciliation engine (src/engines/reconciliation_engine.py) calculates both "
        "the bank side and the book side independently, then compares the results. The "
        "difference must be $0.00 (tolerance: $0.01) before the reconciliation can be "
        "finalized.", styles["Body"]))

    story.append(Paragraph("Bank Side Calculation", styles["H3"]))
    bank_side_rows = [
        ["Item", "Operation", "Source"],
        ["Bank statement ending balance", "Starting point", "Downloaded from bank CSV/PDF"],
        ["+ Deposits in transit", "Add", "Deposits recorded in books but not yet on bank statement"],
        ["- Outstanding cheques", "Subtract", "Cheques written but not yet cleared by bank"],
        ["+ / - Bank errors", "Adjust", "Corrections for bank recording errors"],
        ["= Adjusted bank balance", "Result", "Should match adjusted book balance"],
    ]
    story.append(std_table(bank_side_rows[0], bank_side_rows[1:], [2.0*inch, 1.0*inch, 3.5*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Book Side Calculation", styles["H3"]))
    book_side_rows = [
        ["Item", "Operation", "Source"],
        ["GL cash account balance", "Starting point", "Trial balance from posted documents"],
        ["+ Interest earned", "Add", "Bank interest credits not yet recorded in GL"],
        ["- Bank charges", "Subtract", "Service fees, NSF charges not yet in GL"],
        ["- NSF items", "Subtract", "Returned cheques/payments"],
        ["+ / - Book errors", "Adjust", "Corrections for recording errors in the books"],
        ["= Adjusted book balance", "Result", "Should match adjusted bank balance"],
    ]
    story.append(std_table(book_side_rows[0], book_side_rows[1:], [2.0*inch, 1.0*inch, 3.5*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Performing a Bank Reconciliation", styles["H3"]))
    recon_steps = [
        "Navigate to Bank Reconciliation, then New Reconciliation",
        "Select the client and bank account from the dropdown",
        "Enter the bank statement ending balance and statement date",
        "Click Auto-Populate — the system scans for unmatched bank transactions and populates deposits in transit and outstanding cheques",
        "Review auto-populated items — confirm or remove each one",
        "Add any additional items manually (bank charges, interest, errors)",
        "The system calculates the difference between adjusted bank and book balances in real time",
        "When the difference is $0.00, the Finalize button becomes active",
        "Click Finalize — the reconciliation is locked and a PDF report is generated",
        "Download the PDF for the client file (Reconciliation, then PDF)",
    ]
    for i, step in enumerate(recon_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    story.extend(tip_box(
        "The auto-populate feature uses the bank_matcher engine to identify deposits "
        "in transit and outstanding cheques. It compares bank statement transactions "
        "against posted documents using vendor name fuzzy matching (80% threshold), "
        "amount tolerance (2%), and date window (7 days).", styles))

    story.append(Paragraph("Reconciliation PDF Report", styles["H3"]))
    story.append(Paragraph(
        "The reconciliation PDF is a bilingual (EN/FR) professional report suitable "
        "for inclusion in the audit working papers. It includes:", styles["Body"]))
    story.extend(bullets([
        "Client name, bank account number, and statement date",
        "Bank side calculation with all line items",
        "Book side calculation with all line items",
        "Difference amount (should be $0.00)",
        "Prepared by, reviewed by, and date fields",
        "List of all deposits in transit with dates and amounts",
        "List of all outstanding cheques with cheque numbers, payees, and amounts",
        "Summary of bank charges and interest items",
    ], styles))
    story.extend(warning_box(
        "A finalized reconciliation cannot be edited. If an error is discovered after "
        "finalization, create a new reconciliation for the same period with the "
        "corrections. The original reconciliation is preserved in the audit trail.", styles))
    story.append(PageBreak())

# ── Section 8: CPA Audit Module ───────────────────────────────────────────────
def build_section_8_audit(story, styles):
    story.append(Paragraph("Section 8 — CPA Audit Module", styles["H1"]))
    story.extend(section_rule(styles))

    story.append(Paragraph(
        "The Audit Module (src/engines/audit_engine.py, 61.7 KB) is available on the "
        "entreprise license tier. It provides a full set of assurance tools for CPA "
        "firms conducting compilation, review, and audit engagements.", styles["Body"]))

    story.append(Paragraph("8.1  Engagement Types", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI supports three types of CPA engagements, each with different "
        "levels of assurance and documentation requirements:", styles["Body"]))

    eng_type_rows = [
        ["Type", "Standard", "Assurance Level", "Report Language", "Key Procedures"],
        ["Compilation", "CSRS 4200", "None", "'I have compiled...'",
         "Compile from client data; no verification; plausibility check only"],
        ["Review", "CSRE 2400", "Limited (negative)",
         "'Nothing has come to my attention...'",
         "Inquiry and analytical procedures; no detailed testing; assess plausibility"],
        ["Audit", "CAS 200-810", "Reasonable (positive)",
         "'In my opinion, the financial statements present fairly...'",
         "Risk assessment, control testing, substantive procedures, confirmations, sampling"],
    ]
    story.append(std_table(eng_type_rows[0], eng_type_rows[1:], [0.9*inch, 0.9*inch, 1.2*inch, 1.5*inch, 2.0*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph(
        "The engagement type determines which LedgerLink AI features are available. "
        "Compilation engagements use only working papers and financial statements. "
        "Review engagements add analytical procedures. Audit engagements unlock the "
        "full suite: control testing, statistical sampling, risk assessment, materiality, "
        "and all CAS-related modules.", styles["Body"]))

    story.extend(warning_box(
        "Selecting the wrong engagement type affects the audit trail and documentation "
        "requirements. Changing the type after fieldwork has begun requires partner "
        "approval and creates an engagement amendment record.", styles))

    story.append(Paragraph("Engagement Records", styles["H3"]))
    story.append(Paragraph(
        "An engagement record represents a single assurance or compilation engagement "
        "for one client for one fiscal period. Engagements organize all audit work "
        "and track budget vs. actual hours.", styles["Body"]))

    eng_rows = [
        ["Field", "Description"],
        ["Client", "The client entity being audited"],
        ["Fiscal period", "The year-end date of the engagement"],
        ["Engagement type", "Compilation, Review, or Audit"],
        ["Partner", "Signing partner (owner role)"],
        ["Manager", "Engagement manager (manager role)"],
        ["Staff", "Assigned staff accountants (employee role)"],
        ["Budget hours", "Planned hours by role (partner / manager / staff)"],
        ["Status", "Planning, Fieldwork, Completion, Signed Off"],
        ["Engagement letter", "Reference to signed engagement letter document"],
    ]
    story.append(std_table(eng_rows[0], eng_rows[1:], [1.5*inch, 5.0*inch]))
    story.append(Spacer(1, 8))

    create_eng = [
        "Navigate to Audit Module, then Engagements, then New Engagement",
        "Select the client and fiscal period end date",
        "Choose the engagement type (Compilation / Review / Audit)",
        "Assign partner, manager, and staff from dropdown lists",
        "Set budget hours for each role level",
        "Click Create — the engagement file is opened immediately",
    ]
    for i, step in enumerate(create_eng, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("8.2  Working Papers", styles["H2"]))
    story.append(Paragraph(
        "Working papers (dossiers de travail) are lead sheets organized by GL account. "
        "Each working paper captures the balance per books, audit adjustments, and the "
        "final confirmed balance. Tick marks and test evidence are recorded at the "
        "item level.", styles["Body"]))

    wp_rows = [
        ["Field", "Description"],
        ["Account code", "GL account number from chart_of_accounts"],
        ["Account name", "Description of the account"],
        ["Balance per books", "Trial balance amount before adjustments"],
        ["Audit adjustments", "Proposed journal entry adjustments"],
        ["Confirmed balance", "Balance per books plus/minus adjustments"],
        ["Variance", "Confirmed balance minus prior-year comparative"],
        ["Tick marks", "Standard tick notation: agreed to source, recalculated, confirmed"],
        ["Prepared by", "Staff accountant who completed the working paper"],
        ["Reviewed by", "Manager or partner who reviewed"],
        ["Status", "Open, Reviewed, Cleared"],
    ]
    story.append(std_table(wp_rows[0], wp_rows[1:], [1.5*inch, 5.0*inch]))
    story.append(Spacer(1, 8))

    story.extend(tip_box(
        "Working papers auto-populate the Balance per Books field from the trial "
        "balance generated by posted documents. If QuickBooks Online is the source "
        "system, run Audit Module, then Trial Balance, then Sync from QBO before "
        "opening working papers.", styles))

    story.append(Paragraph("8.3  Three-Way Matching", styles["H2"]))
    story.append(Paragraph(
        "Three-way matching links a Purchase Order, an Invoice, and a Payment into "
        "a single evidence chain. This provides strong audit evidence for payables "
        "and is stored in the audit_evidence table.", styles["Body"]))

    match_cols = [
        ["Document Type", "Key Fields Matched", "Tolerance"],
        ["Purchase Order", "PO number, vendor, amount", "PO must be within 5% of invoice"],
        ["Invoice", "Invoice number, vendor, amount, date", "Must reference PO number"],
        ["Payment", "Payment reference, amount, date", "Must be within 2 days of invoice due"],
    ]
    story.append(std_table(match_cols[0], match_cols[1:], [1.4*inch, 2.8*inch, 2.3*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph(
        "To create a three-way match: open the invoice document card, click "
        "Link Evidence, then select the matching PO and payment from the document "
        "list. The system validates the amounts and flags any discrepancies.", styles["Body"]))
    story.extend(warning_box(
        "Three-way matching evidence chains are reproducible and permanent. Once "
        "created, they cannot be deleted — only marked as superseded by a correction "
        "chain. This ensures the audit trail remains intact.", styles))

    story.append(Paragraph("8.4  Statistical Sampling", styles["H2"]))
    story.append(Paragraph(
        "The statistical sampling tool selects a reproducible random sample from a "
        "population of transactions. Results are deterministic (same seed = same "
        "sample) to support re-performance during quality reviews.", styles["Body"]))

    sample_rows = [
        ["Parameter", "Description", "Example"],
        ["Population", "All posted transactions in the period for a GL account", "Accounts Payable — 450 items"],
        ["Confidence level", "Statistical confidence desired", "95%"],
        ["Tolerable error", "Maximum acceptable error rate", "5%"],
        ["Expected error", "Estimated error rate based on prior periods", "2%"],
        ["Sample size", "Calculated by the engine (minimum 30)", "59 items"],
        ["Seed", "Random seed for reproducibility", "Auto-generated; stored in engagement"],
    ]
    story.append(std_table(sample_rows[0], sample_rows[1:], [1.2*inch, 2.6*inch, 2.7*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("8.5  Trial Balance and Financial Statements", styles["H2"]))
    story.append(Paragraph(
        "The trial balance is auto-generated from all documents with status Posted "
        "in the selected period. Debit and credit totals are rolled up by GL account "
        "and compared to prior-year figures.", styles["Body"]))

    story.extend(bullets([
        "Generate: Audit Module, then Trial Balance, then Generate for Period",
        "Export: Export to Excel (XLSX) or PDF for inclusion in working papers",
        "Sync: Import adjusted trial balance from QuickBooks Online",
        "Comparative: toggle Show Prior Year to display year-over-year columns",
        "Adjustments: enter proposed journal entries and see the adjusted trial balance",
    ], styles))

    story.append(Paragraph("Financial Statements", styles["H3"]))
    story.append(Paragraph(
        "LedgerLink AI generates draft financial statements from the adjusted trial "
        "balance. These are working drafts for review — not final CPA-signed statements.", styles["Body"]))

    fs_rows = [
        ["Statement", "Contents"],
        ["Balance Sheet", "Assets, liabilities, and equity grouped per ASPE/IFRS classification"],
        ["Income Statement", "Revenue, cost of sales, gross profit, operating expenses, net income"],
        ["Statement of Changes in Equity", "Opening equity, net income, dividends, closing equity"],
        ["Notes (draft)", "Significant accounting policies, going concern, related parties (template)"],
    ]
    story.append(std_table(fs_rows[0], fs_rows[1:], [1.8*inch, 4.7*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("8.6  Analytical Procedures (CAS 520)", styles["H2"]))
    story.append(Paragraph(
        "Analytical procedures compare current-period amounts to expectations derived "
        "from prior periods, budgets, or industry benchmarks. LedgerLink AI calculates "
        "variances automatically and flags items exceeding the threshold. CAS 520 "
        "requires analytical procedures at the planning and completion stages of every "
        "audit.", styles["Body"]))

    anal_rows = [
        ["Procedure", "Calculation", "Default Threshold"],
        ["Year-over-year variance", "(Current - Prior) / Prior x 100%", "More than 15% change"],
        ["Gross margin ratio", "Gross profit / Revenue x 100%", "More than 3 points change"],
        ["Current ratio", "Current assets / Current liabilities", "Below 1.0 flagged"],
        ["Debt-to-equity", "Total liabilities / Total equity", "Above 3.0 flagged"],
        ["Days payable outstanding", "AP / (COGS/365)", "More than 60 days flagged"],
        ["Revenue per client", "Total revenue / Number of active clients", "Variance more than 20%"],
    ]
    story.append(std_table(anal_rows[0], anal_rows[1:], [1.6*inch, 2.5*inch, 2.4*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("8.7  Materiality (CAS 320)", styles["H2"]))
    story.append(Paragraph(
        "CAS 320 — Materiality in Planning and Performing an Audit requires the auditor "
        "to determine materiality for the financial statements as a whole. LedgerLink AI "
        "calculates materiality automatically based on three levels:", styles["Body"]))

    mat_rows = [
        ["Level", "Typical Benchmark", "Purpose"],
        ["Overall materiality", "5-10% of pre-tax income; or 0.5-1% of total revenue; or 1-2% of total assets",
         "Maximum amount of misstatement that would not influence users' decisions"],
        ["Performance materiality", "50-75% of overall materiality",
         "Lower threshold used for detailed testing to ensure aggregate errors stay below overall materiality"],
        ["Clearly trivial threshold", "3-5% of overall materiality",
         "Misstatements below this amount need not be accumulated; they are clearly inconsequential"],
    ]
    story.append(std_table(mat_rows[0], mat_rows[1:], [1.6*inch, 2.2*inch, 2.7*inch]))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "To set materiality: navigate to Audit Module, then Materiality, then select "
        "the engagement. Enter the benchmark basis (revenue, assets, or income) and "
        "the percentage. LedgerLink AI calculates all three levels automatically and "
        "stores them in the cas_materiality table.", styles["Body"]))
    story.extend(tip_box(
        "For not-for-profit entities, use 0.5-2% of total expenses or total revenue "
        "as the materiality benchmark. For public sector entities, use total "
        "expenditures. LedgerLink AI supports custom benchmark selection.", styles))

    story.append(Paragraph("8.8  Risk Assessment (CAS 315)", styles["H2"]))
    story.append(Paragraph(
        "CAS 315 (Revised 2019) — Identifying and Assessing the Risks of Material "
        "Misstatement requires the auditor to assess risk at both the financial statement "
        "level and the assertion level. LedgerLink AI implements a risk assessment matrix "
        "that maps each significant account to audit assertions.", styles["Body"]))

    assertion_rows = [
        ["Assertion", "Applies To", "Description"],
        ["Completeness", "All account types", "All transactions that should have been recorded have been recorded"],
        ["Accuracy", "All account types", "Amounts and other data have been recorded appropriately"],
        ["Existence / Occurrence", "Assets, Revenue", "Assets exist and transactions actually occurred"],
        ["Cutoff", "Revenue, Expenses", "Transactions are recorded in the correct period"],
        ["Classification", "All account types", "Transactions are recorded in the proper accounts"],
        ["Rights and Obligations", "Assets, Liabilities", "The entity holds rights to assets; liabilities are obligations of the entity"],
        ["Presentation and Disclosure", "Financial statements", "Items are appropriately aggregated, described, and disclosed"],
        ["Valuation / Allocation", "Assets, Liabilities", "Assets and liabilities are recorded at appropriate amounts"],
    ]
    story.append(std_table(assertion_rows[0], assertion_rows[1:], [1.5*inch, 1.3*inch, 3.7*inch]))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "Risk levels are assessed as Low, Moderate, or High for each assertion on each "
        "significant account. The risk matrix is stored in the cas_risk_assessment table "
        "and can be generated automatically based on account characteristics, or entered "
        "manually by the engagement team.", styles["Body"]))

    story.append(Paragraph("CAS 500 — Assertion Coverage Matrix", styles["H3"]))
    story.append(Paragraph(
        "CAS 500 requires that audit evidence be obtained for each relevant assertion "
        "for each significant class of transactions, account balance, and disclosure. "
        "LedgerLink AI tracks assertion coverage through the working papers module:", styles["Body"]))
    cov_rows = [
        ["Account", "Completeness", "Accuracy", "Existence", "Cutoff", "Classification", "Rights"],
        ["Cash", "Recon", "Recon", "Confirm", "Cutoff test", "Review", "Bank letter"],
        ["Accounts Receivable", "Cutoff", "Recon", "Confirm", "Cutoff test", "Review", "Confirm"],
        ["Inventory", "Count", "Pricing", "Count", "Cutoff test", "Review", "Inspect"],
        ["Accounts Payable", "Search", "Recon", "Confirm", "Cutoff test", "Review", "Inspect"],
        ["Revenue", "Cutoff", "Recon", "Vouching", "Cutoff test", "Review", "Contract"],
        ["Payroll", "Recon", "Recalc", "Test", "Cutoff test", "Review", "Review"],
    ]
    story.append(std_table(cov_rows[0], cov_rows[1:], [1.1*inch, 0.9*inch, 0.9*inch, 0.8*inch, 0.8*inch, 1.0*inch, 0.8*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("8.9  Control Testing (CAS 330)", styles["H2"]))
    story.append(Paragraph(
        "CAS 330 — The Auditor's Responses to Assessed Risks requires the auditor to "
        "design and perform audit procedures responsive to the assessed risks. When "
        "relying on internal controls, control testing is required.", styles["Body"]))
    story.extend(bullets([
        "Add control tests: Audit Module, then Controls, then Add Control Test",
        "Select the control being tested (e.g., purchase order approval, bank reconciliation sign-off)",
        "Define the testing approach: inquiry, observation, inspection, reperformance",
        "Set the sample size based on frequency of control operation",
        "Record test results: effective, ineffective (with deviation details), or not tested",
        "LedgerLink AI calculates the deviation rate and compares to tolerable rate",
        "If deviation rate exceeds tolerable rate, the control is assessed as ineffective",
        "Ineffective controls trigger expanded substantive testing recommendations",
    ], styles))

    ctrl_rows = [
        ["Control Frequency", "Minimum Sample Size", "Tolerable Deviation Rate"],
        ["Annual (once per year)", "1", "0% (must work the one time)"],
        ["Quarterly", "2-4", "10-15%"],
        ["Monthly", "5-15", "5-10%"],
        ["Weekly", "20-25", "5%"],
        ["Daily", "25-40", "5%"],
        ["Per transaction", "25-60 (varies by volume)", "3-5%"],
    ]
    story.append(std_table(ctrl_rows[0], ctrl_rows[1:], [1.5*inch, 1.8*inch, 3.2*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("8.10  Going Concern (CAS 570)", styles["H2"]))
    story.append(Paragraph(
        "CAS 570 requires the auditor to evaluate whether there are conditions or events "
        "that cast significant doubt on the entity's ability to continue as a going "
        "concern. LedgerLink AI assists by automatically calculating key financial "
        "indicators:", styles["Body"]))
    gc_rows = [
        ["Indicator", "Warning Threshold", "Critical Threshold"],
        ["Current ratio", "Below 1.0", "Below 0.5"],
        ["Quick ratio", "Below 0.8", "Below 0.3"],
        ["Negative working capital", "Any negative amount", "Negative and declining"],
        ["Recurring net losses", "2 consecutive periods", "3+ consecutive periods"],
        ["Net cash outflow from operations", "1 period", "2+ consecutive periods"],
        ["Debt covenant violations", "Any violation", "Multiple covenants breached"],
        ["Loss of major customer/supplier", "More than 20% of revenue", "More than 40% of revenue"],
    ]
    story.append(std_table(gc_rows[0], gc_rows[1:], [2.0*inch, 2.0*inch, 2.5*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("8.11  Subsequent Events (CAS 560)", styles["H2"]))
    story.append(Paragraph(
        "CAS 560 requires audit procedures to identify events between the period end "
        "and the audit report date that may require adjustment or disclosure. LedgerLink "
        "AI flags documents with invoice dates after the period end that were posted to "
        "the period, and documents posted after the period end to a subsequent period.", styles["Body"]))
    story.extend(bullets([
        "Type I (adjusting events): events providing additional evidence about conditions at period end — require adjustment",
        "Type II (non-adjusting events): events indicative of conditions arising after period end — require disclosure only",
        "LedgerLink AI scans for large transactions between period end and audit report date",
        "The subsequent events review is available in the engagement detail under Completion tab",
    ], styles))

    story.append(Paragraph("8.12  Management Representation Letter (CAS 580)", styles["H2"]))
    story.append(Paragraph(
        "CAS 580 requires the auditor to obtain written representations from management "
        "as audit evidence. LedgerLink AI generates a template management representation "
        "letter for each engagement.", styles["Body"]))
    story.extend(bullets([
        "Generate: Audit Module, then Rep Letter, then Generate for the engagement",
        "The letter includes standard representations required by CAS 580",
        "Management acknowledges responsibility for financial statements and internal controls",
        "Representations about completeness of information provided to the auditor",
        "Representations about related party transactions (per CAS 550)",
        "Representations about subsequent events (per CAS 560)",
        "The letter is pre-populated with client name, fiscal period, and signing date",
        "Management must sign and date the letter on or after the audit report date",
    ], styles))

    story.append(Paragraph("8.13  Related Parties (CAS 550)", styles["H2"]))
    story.append(Paragraph(
        "CAS 550 requires the auditor to identify related party relationships and "
        "transactions. LedgerLink AI maintains a related party register per engagement.", styles["Body"]))
    rp_rows = [
        ["Field", "Description"],
        ["Related party name", "Entity or individual name"],
        ["Relationship type", "Parent, subsidiary, director, officer, key management, shareholder"],
        ["Nature of transactions", "Loans, purchases, sales, management fees, rent"],
        ["Transaction amounts", "Total amounts transacted during the period"],
        ["Outstanding balances", "Amounts receivable or payable at period end"],
        ["Terms and conditions", "Are terms arm's length? Market rate?"],
        ["Disclosure requirement", "Required/not required based on ASPE 3840 or IAS 24"],
    ]
    story.append(std_table(rp_rows[0], rp_rows[1:], [1.8*inch, 4.7*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("8.14  Audit Opinion (CAS 700)", styles["H2"]))
    story.append(Paragraph(
        "CAS 700 governs forming an opinion and reporting on financial statements. "
        "LedgerLink AI tracks the engagement through to the opinion stage:", styles["Body"]))
    opinion_rows = [
        ["Opinion Type", "When Used"],
        ["Unmodified (clean)", "Financial statements are free of material misstatement"],
        ["Qualified", "Misstatements are material but not pervasive; or inability to obtain sufficient evidence for specific items"],
        ["Adverse", "Misstatements are both material and pervasive"],
        ["Disclaimer", "Auditor unable to obtain sufficient appropriate evidence; possible effects are material and pervasive"],
    ]
    story.append(std_table(opinion_rows[0], opinion_rows[1:], [1.5*inch, 5.0*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("8.15  Quality Control (CSQC 1)", styles["H2"]))
    story.append(Paragraph(
        "Canadian Standard on Quality Control 1 (CSQC 1) requires CPA firms to establish "
        "a system of quality control. LedgerLink AI supports CSQC 1 compliance through:", styles["Body"]))
    story.extend(bullets([
        "Engagement acceptance procedures — documented in engagement creation workflow",
        "Partner and staff assignment — role-based access ensures qualified personnel",
        "Independence confirmation — engagement team members confirm independence at engagement start",
        "Supervision and review — tiered sign-off: staff prepares, manager reviews, partner approves",
        "Consultation documentation — complex issues flagged for partner consultation with full audit trail",
        "Engagement quality control review (EQCR) — for listed entities or high-risk engagements",
        "Monitoring — analytics dashboard tracks engagement hours, deadlines, and completion rates",
        "Documentation — all working papers, evidence, and communications preserved in the engagement file",
    ], styles))
    story.extend(warning_box(
        "CSQC 1 compliance ultimately depends on the firm's policies and procedures, "
        "not on software alone. LedgerLink AI provides the documentation framework — "
        "the firm must ensure its people follow the policies.", styles))
    story.append(PageBreak())


# ── Section 9: Administration ─────────────────────────────────────────────────
def build_section_9_administration(story, styles):
    story.append(Paragraph("Section 9 — Administration", styles["H1"]))
    story.extend(section_rule(styles))

    story.append(Paragraph("9.1  License Management and Tier Comparison", styles["H2"]))
    story.append(Paragraph(
        "Your LedgerLink AI license controls which features are available and how "
        "many clients and users you can manage. The license key is stored in "
        "ledgerlink.config.json and validated at startup.", styles["Body"]))

    tier_rows = [
        ["Feature", "Essentiel", "Professionnel", "Cabinet", "Entreprise"],
        ["Max clients", "10", "30", "75", "Unlimited"],
        ["Max users", "3", "5", "15", "Unlimited"],
        ["Basic review and posting", "YES", "YES", "YES", "YES"],
        ["AI router (dual provider)", "NO", "YES", "YES", "YES"],
        ["Bank parser and matcher", "NO", "YES", "YES", "YES"],
        ["Fraud engine", "NO", "YES", "YES", "YES"],
        ["Revenu Quebec forms", "NO", "YES", "YES", "YES"],
        ["Time tracking", "NO", "YES", "YES", "YES"],
        ["Month-end close", "NO", "YES", "YES", "YES"],
        ["Analytics dashboard", "NO", "NO", "YES", "YES"],
        ["Microsoft 365 integration", "NO", "NO", "YES", "YES"],
        ["Filing calendar", "NO", "NO", "YES", "YES"],
        ["Client communications", "NO", "NO", "YES", "YES"],
        ["Audit module", "NO", "NO", "NO", "YES"],
        ["Financial statements", "NO", "NO", "NO", "YES"],
        ["Statistical sampling", "NO", "NO", "NO", "YES"],
        ["API access (port 8789)", "NO", "NO", "NO", "YES"],
    ]
    story.append(std_table(tier_rows[0], tier_rows[1:], [2.3*inch, 0.9*inch, 1.1*inch, 0.9*inch, 1.0*inch]))
    story.append(Spacer(1, 8))

    story.extend(tip_box(
        "To view your current license status and remaining capacity, navigate to "
        "Administration, then License. The page shows: tier, expiry date, clients "
        "used vs. maximum, users used vs. maximum, and a list of enabled features.", styles))

    story.append(Paragraph("License Key Format", styles["H3"]))
    story.append(Paragraph(
        "License keys follow the format LLAI-[base64url-encoded JSON payload]. The "
        "payload contains tier, expiry date, client limit, user limit, and enabled "
        "features. The key is signed with HMAC-SHA256 using a secret stored in "
        "the .env file. Tampering with the key invalidates the signature and disables "
        "all premium features until a valid key is entered.", styles["Body"]))
    story.extend(warning_box(
        "Do not share your license key. Each key is tied to your firm and can only "
        "activate one installation at a time. Contact support@ledgerlink.ai to "
        "transfer a license to a new server.", styles))

    story.append(Paragraph("9.2  Troubleshoot Page and Autofix", styles["H2"]))
    story.append(Paragraph(
        "The Troubleshoot page (Administration, then Troubleshoot) runs 13 automated "
        "checks against the system and reports any issues found. Each check can be "
        "autofixed with a single click where possible.", styles["Body"]))

    check_rows = [
        ["Check", "What It Tests", "Autofix Available?"],
        ["1 — DB connectivity", "SQLite database opens and responds", "NO — restart service"],
        ["2 — DB schema", "All required tables and columns exist", "YES — runs migrate_db.py"],
        ["3 — Config file", "ledgerlink.config.json is valid JSON", "NO — manual edit required"],
        ["4 — AI provider", "DeepSeek and OpenRouter API keys valid", "NO — update keys in config"],
        ["5 — QBO connection", "QuickBooks Online OAuth token not expired", "YES — re-auth flow"],
        ["6 — Graph API", "Microsoft 365 token valid for mailbox access", "YES — re-auth flow"],
        ["7 — Port 8787", "Dashboard HTTP server is running and responding", "YES — restart server"],
        ["8 — Port 8788", "Client portal is running", "YES — restart server"],
        ["9 — Port 8789", "Ingest API is running", "YES — restart server"],
        ["10 — Disk space", "At least 1 GB free on database drive", "NO — manual cleanup"],
        ["11 — Password hashes", "No legacy SHA-256 hashes in dashboard_users", "YES — sets must_reset flag"],
        ["12 — License validity", "License key not expired and signature valid", "NO — contact support"],
        ["13 — bcrypt rounds", "bcrypt cost factor is 12 or higher", "YES — updates config"],
    ]
    story.append(std_table(check_rows[0], check_rows[1:], [1.4*inch, 3.0*inch, 1.5*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("9.3  Backups and Updates", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI data is stored in a single SQLite file (data/ledgerlink_agent.db). "
        "Back up this file daily using your preferred backup solution.", styles["Body"]))

    story.extend(bullets([
        "Recommended backup: copy data/ledgerlink_agent.db to a network share or cloud storage nightly",
        "Also back up: ledgerlink.config.json, .env, .o365_profile/ directory",
        "Test restores quarterly — a backup that cannot restore is not a backup",
        "The database can be safely copied while the service is running (SQLite WAL mode is enabled)",
        "Updates: download the new setup.exe installer; run it over the existing installation",
        "Updates are additive — they never delete data or configuration",
        "After updating, the installer runs migrate_db.py automatically to add any new columns",
        "Review the release notes before updating in production — check for breaking changes",
    ], styles))
    story.extend(warning_box(
        "Do not update on the last business day of the month or during a period-close "
        "cycle. Schedule updates for early morning on a low-traffic day. Always take "
        "a manual database backup immediately before updating.", styles))

    story.append(Paragraph("9.4  Vendor Memory Management", styles["H2"]))
    story.append(Paragraph(
        "Vendor memory (src/agents/tools/vendor_intelligence.py) stores learned patterns "
        "for each vendor: typical GL account, expected amount range, billing frequency, "
        "and tax code. This data improves over time as reviewers correct AI suggestions.", styles["Body"]))

    vm_rows = [
        ["Field", "Source", "Purpose"],
        ["Default GL account", "Most frequently approved GL for this vendor", "Auto-suggest GL on new documents"],
        ["Amount mean / stddev", "Statistical analysis of posted amounts", "Fraud engine: flag amounts outside 2 sigma"],
        ["Billing frequency", "Day-of-month pattern analysis", "Fraud engine: flag off-cycle invoices"],
        ["Tax code", "Most common tax code approved", "Auto-suggest tax code"],
        ["Last seen date", "Most recent document date", "Identify dormant vendors"],
        ["Correction history", "All manual overrides by reviewers", "Learning memory for future suggestions"],
    ]
    story.append(std_table(vm_rows[0], vm_rows[1:], [1.5*inch, 2.3*inch, 2.7*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Managing Vendor Memory", styles["H3"]))
    story.extend(bullets([
        "View: Administration, then Vendor Memory — browse all vendors and their learned patterns",
        "Edit: click a vendor to view and edit GL, tax code, and amount thresholds",
        "Reset single vendor: Administration, then Vendor Memory, then select vendor, then Reset — clears all learned patterns for that vendor",
        "Reset all: Administration, then Vendor Memory, then Reset All — clears entire vendor memory (use with caution)",
        "Backfill: run scripts/backfill_vendor_memory.py to rebuild vendor memory from posted documents",
        "Seed: run scripts/seed_vendor_knowledge.py to pre-load vendor patterns from a CSV file",
        "Export: export vendor memory to CSV for backup or migration to a new installation",
    ], styles))
    story.extend(warning_box(
        "Resetting vendor memory causes the system to lose all learned GL and tax code "
        "patterns. After a reset, every document will require manual GL assignment until "
        "the system re-learns vendor patterns. Only reset as a last resort for persistent "
        "incorrect suggestions.", styles))

    story.append(Paragraph("9.5  AI Cache Management", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI caches AI responses to reduce API costs and improve response time. "
        "Repeated identical extractions (same document hash) return the cached result "
        "instantly without calling the AI provider.", styles["Body"]))

    cache_rows = [
        ["Cache Type", "TTL", "Storage", "Clear Method"],
        ["Extraction cache", "30 days", "ai_cache table in SQLite", "Administration, then Cache, then Clear Extraction"],
        ["Classification cache", "30 days", "ai_cache table", "Administration, then Cache, then Clear Classification"],
        ["GL suggestion cache", "7 days", "ai_cache table", "Administration, then Cache, then Clear GL Suggestions"],
        ["Provider health cache", "5 minutes", "In-memory", "Automatic — clears on service restart"],
    ]
    story.append(std_table(cache_rows[0], cache_rows[1:], [1.5*inch, 0.8*inch, 1.8*inch, 2.4*inch]))
    story.append(Spacer(1, 8))

    story.extend(bullets([
        "View cache statistics: Administration, then Cache — shows hit rate, size, and last clear date",
        "Clear all caches: Administration, then Cache, then Clear All — forces fresh AI calls for all documents",
        "Cache is automatically pruned: entries older than 30 days are removed on service startup",
        "Disable caching: set ai_router.cache_enabled to false in ledgerlink.config.json (not recommended — increases API costs significantly)",
        "Cache does not apply to fraud engine or tax engine — these are deterministic and always run fresh",
    ], styles))
    story.extend(tip_box(
        "If a vendor changes their invoice format and the AI is returning stale "
        "extractions, clear the extraction cache for that vendor. This forces fresh "
        "AI calls for their documents while preserving cache for all other vendors.", styles))
    story.append(PageBreak())

# ── Section 10: Troubleshooting ───────────────────────────────────────────────
def build_section_10_troubleshooting(story, styles):
    story.append(Paragraph("Section 10 — Troubleshooting", styles["H1"]))
    story.extend(section_rule(styles))

    story.append(Paragraph(
        "This section covers the most common issues reported by LedgerLink AI users. "
        "For issues not covered here, run Administration, then Troubleshoot first — "
        "the autofix tool resolves 60% of common problems automatically.", styles["Body"]))

    story.append(Paragraph("10.1  Login Failures", styles["H2"]))
    issues = [
        ("Cannot log in — Invalid credentials",
         "Verify the username is correct (case-sensitive). If recently upgraded from v1.x, "
         "the must_reset_password flag may be set — use Administration, then User Management, "
         "then Reset Password to issue a new temporary password.",
         "1. Check username spelling (no spaces, exact case)\n"
         "2. Use Set Password script: python scripts/set_password.py <username>\n"
         "3. Verify bcrypt is installed: pip install bcrypt\n"
         "4. Check audit_log table for failed login entries"),
        ("Login hangs for more than 5 seconds",
         "bcrypt verification at 12 rounds takes 0.3-0.8 seconds on modern hardware. "
         "If it exceeds 5 seconds, the server CPU is overloaded or bcrypt_rounds is set "
         "too high in ledgerlink.config.json.",
         "1. Check server CPU usage during login attempt\n"
         "2. Reduce bcrypt_rounds to 10 in config (restart required)\n"
         "3. Check for runaway processes consuming CPU"),
        ("Session expires too quickly",
         "Default session TTL is 12 hours. If sessions expire sooner, the server clock "
         "may be drifting or the JWT secret was rotated.",
         "1. Verify server time is synchronized (NTP)\n"
         "2. Check session_ttl_hours in ledgerlink.config.json\n"
         "3. Ensure JWT secret has not changed since last session"),
    ]

    for title, cause, fix in issues:
        story.append(Paragraph(title, styles["H3"]))
        story.append(Paragraph(f"<b>Cause:</b> {cause}", styles["Body"]))
        story.extend(warning_box(f"<b>Fix:</b><br/>{fix.replace(chr(10), '<br/>')}", styles))

    story.append(Paragraph("10.2  PDF Viewer Issues", styles["H2"]))
    story.append(Paragraph(
        "LedgerLink AI uses an inline PDF viewer in the review dashboard. The viewer "
        "requires a modern browser with PDF.js support. Common issues:", styles["Body"]))

    pdf_rows = [
        ["Symptom", "Likely Cause", "Solution"],
        ["Blank PDF panel", "Browser security policy blocking iframe", "Allow localhost in browser settings; use Chrome or Edge"],
        ["PDF shows but text garbled", "Scanned PDF without text layer", "OCR engine processes this automatically — check ocr_engine logs"],
        ["Cannot scroll in PDF", "Mouse wheel captured by dashboard", "Click inside the PDF panel first, then scroll"],
        ["PDF download button missing", "Employee role does not have download permission", "Owner/manager can change per-user permissions"],
        ["PDF shows old version", "Browser cache", "Hard refresh: Ctrl+Shift+R"],
    ]
    story.append(std_table(pdf_rows[0], pdf_rows[1:], [1.4*inch, 2.0*inch, 3.1*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("10.3  QuickBooks Online Posting Failures", styles["H2"]))
    story.append(Paragraph(
        "Posting failures are logged in the posting_jobs table with an error_message "
        "column. Navigate to Administration, then Posting Jobs to view failed jobs.", styles["Body"]))

    qbo_rows = [
        ["Error", "Meaning", "Fix"],
        ["OAuth token expired", "QBO access token is older than 60 days", "Administration, then QBO, then Re-authenticate"],
        ["Vendor not found", "Vendor name does not match any QBO vendor", "Enable auto_create_vendors in client_config.json"],
        ["GL account not found", "GL account code does not exist in QBO chart of accounts", "Sync GL accounts: Administration, then QBO, then Sync Accounts"],
        ["Duplicate transaction", "QBO rejected as duplicate", "Check if document was already posted manually in QBO"],
        ["Amount mismatch", "Total does not match QBO calculation", "Verify tax amounts are correct; check tax code assignment"],
        ["Minor version mismatch", "QBO API minor_version outdated", "Update minor_version in client_config.json to 75 or higher"],
    ]
    story.append(std_table(qbo_rows[0], qbo_rows[1:], [1.5*inch, 2.0*inch, 3.0*inch]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("10.4  AI Extraction Wrong", styles["H2"]))
    story.append(Paragraph(
        "If the AI extracts incorrect vendor, amount, or date values, correct them "
        "manually in the review panel. Every correction is recorded in the "
        "learning_correction_store and used to improve future extractions for that "
        "vendor.", styles["Body"]))
    story.extend(bullets([
        "Vendor name wrong: edit the vendor field and click Save — learning memory updates",
        "Amount wrong: edit the subtotal, GST, or QST field — hallucination guard re-validates",
        "Date wrong: edit the date field — fraud engine re-runs date checks",
        "Tax code wrong: select correct code from dropdown — ITC/ITR recalculated immediately",
        "Systematic errors: check ocr_engine logs for extraction patterns; may need custom rules",
        "Poor scan quality: use a higher DPI scanner or retake photo in better lighting",
    ], styles))

    story.append(Paragraph("10.5  Email Intake Not Working", styles["H2"]))
    story.append(Paragraph(
        "Email intake relies on the Microsoft Graph API polling the shared mailbox. "
        "If documents emailed to the mailbox are not appearing in the review queue, "
        "check the following:", styles["Body"]))
    email_steps = [
        "Run Administration, then Troubleshoot, then Check 6 (Graph API) to verify token",
        "If token expired: Administration, then Microsoft 365, then Re-authenticate",
        "Check that the shared mailbox address in ledgerlink.config.json matches exactly",
        "Verify the service account has delegated access to the mailbox in Azure AD",
        "Check .ledgerlink_system/graph_mail.log for polling errors",
        "Ensure the email has a PDF or image attachment — emails without attachments are skipped",
        "Check spam folder in the mailbox — emails may be getting filtered before polling",
    ]
    for i, step in enumerate(email_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("10.6  Substance Flags — Wrong GL Code", styles["H2"]))
    story.append(Paragraph(
        "The substance engine (src/engines/substance_engine.py) classifies documents "
        "as CapEx, prepaids, loans, tax remittances, personal expenses, customer deposits, "
        "or intercompany transfers. If the substance flag suggests the wrong GL account, "
        "follow these steps:", styles["Body"]))
    substance_rows = [
        ["Substance Flag", "Typical Cause of Incorrect Flag", "Resolution"],
        ["CapEx (amount >= $1,500)", "Vendor sells both small and large items; AI triggered on total", "Override GL manually; correction trains learning memory"],
        ["Prepaid expense", "Keywords like 'annual' or 'subscription' in invoice text", "Change GL to current expense if period is <12 months"],
        ["Loan / financing", "Invoice mentions 'payment plan' or 'installments'", "Verify with vendor; may be a legitimate financing arrangement"],
        ["Tax remittance", "Keywords like 'CRA' or 'Revenu Quebec' in vendor name", "Correct GL; substance engine learns from correction"],
        ["Personal expense", "Keywords like 'home' or 'personal' detected", "Review with client; if legitimate business expense, override and note"],
        ["Customer deposit", "Keywords like 'deposit' or 'advance payment'", "Verify whether this is a liability (deposit) or revenue (payment)"],
        ["Intercompany transfer", "Similar entity names detected between payer and payee", "Confirm with client; intercompany transactions need elimination on consolidation"],
    ]
    story.append(std_table(substance_rows[0], substance_rows[1:], [1.3*inch, 2.5*inch, 2.7*inch]))
    story.append(Spacer(1, 8))
    story.extend(tip_box(
        "When the substance engine incorrectly flags a vendor repeatedly, correct the "
        "GL code and save. After 3 corrections for the same vendor, the learning memory "
        "overrides the substance engine's keyword detection for that vendor.", styles))

    story.append(Paragraph("10.7  Fraud Engine — False Positives", styles["H2"]))
    story.append(Paragraph(
        "The fraud engine is deliberately conservative — it prefers false positives "
        "(flagging legitimate transactions) over false negatives (missing suspicious "
        "ones). Common false positive scenarios and how to handle them:", styles["Body"]))
    fp_rows = [
        ["Flag", "False Positive Scenario", "Recommended Action"],
        ["Duplicate Invoice", "Same vendor bills same amount monthly (e.g., rent)", "Approve with note: 'Recurring monthly charge — not a duplicate'"],
        ["Round Amount", "Subscription at exactly $500.00/month", "Approve — round amounts on subscriptions are normal"],
        ["Vendor Not Seen", "New legitimate vendor (first invoice)", "Verify vendor legitimacy; approve and vendor memory will learn"],
        ["Amount Spike", "Annual insurance premium (much larger than monthly invoices)", "Approve with note: 'Annual premium — expected spike'"],
        ["Weekend Invoice", "Invoice generated by automated system on Saturday", "Approve if amount and vendor are otherwise correct"],
        ["Missing Tax", "Purchase from non-registrant small supplier (<$30K revenue)", "Confirm supplier is not GST-registered; assign code E or Z"],
        ["Pattern Break", "Vendor changed billing cycle from 15th to 1st of month", "Approve; vendor memory will update to new pattern after 3 occurrences"],
    ]
    story.append(std_table(fp_rows[0], fp_rows[1:], [1.3*inch, 2.5*inch, 2.7*inch]))
    story.append(Spacer(1, 8))
    story.extend(warning_box(
        "Never disable the fraud engine to eliminate false positives. Instead, "
        "document your reasoning in the reviewer note for each approved flagged item. "
        "The audit trail must show that a human reviewed every fraud flag.", styles))

    story.append(Paragraph("10.8  Vendor Memory Reset", styles["H2"]))
    story.append(Paragraph(
        "If the vendor memory has become contaminated with incorrect patterns "
        "(e.g., due to a bulk import error or systematic mis-classification), a "
        "targeted reset may be necessary.", styles["Body"]))
    reset_steps = [
        "Identify the affected vendors by reviewing recent incorrect GL suggestions",
        "Navigate to Administration, then Vendor Memory",
        "For individual vendors: select the vendor and click Reset Vendor",
        "For all vendors: click Reset All (requires owner role and password confirmation)",
        "After reset, run scripts/backfill_vendor_memory.py to rebuild from posted documents",
        "Verify that GL suggestions are now correct on a sample of new documents",
        "If suggestions remain incorrect, check the learning_correction_store for conflicting entries",
    ]
    for i, step in enumerate(reset_steps, 1):
        story.append(Paragraph(f"<b>{i}.</b> {step}", styles["Bullet"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("10.9  Running the Autofix Script", styles["H2"]))
    story.append(Paragraph(
        "The autofix script runs all 13 troubleshoot checks and applies automatic "
        "fixes where available. It is the first step for diagnosing any system issue.", styles["Body"]))
    story.append(Paragraph(
        "Run from the command line:", styles["Body"]))
    story.append(Paragraph(
        "<font face='Courier'>python scripts/run_doctor.bat</font>", styles["Code"]))
    story.append(Paragraph(
        "Or from the dashboard: Administration, then Troubleshoot, then Run All Checks. "
        "Each check shows a green checkmark (pass) or red X (fail). For failed checks "
        "with autofix support, click the Fix button next to the check.", styles["Body"]))

    autofix_rows = [
        ["Check", "Autofix Action"],
        ["DB schema outdated", "Runs migrate_db.py to add missing tables and columns"],
        ["QBO token expired", "Opens re-authentication flow in browser"],
        ["Graph API token expired", "Opens Microsoft 365 re-authentication flow"],
        ["Port not responding", "Restarts the LedgerLink Windows service"],
        ["Legacy SHA-256 hashes", "Sets must_reset_password flag on affected accounts"],
        ["bcrypt rounds too low", "Updates config to minimum 12 rounds"],
    ]
    story.append(std_table(autofix_rows[0], autofix_rows[1:], [2.0*inch, 4.5*inch]))
    story.append(Spacer(1, 8))
    story.extend(tip_box(
        "Schedule the autofix script to run weekly as a Windows Task Scheduler job. "
        "This proactively catches configuration drift and token expirations before "
        "they disrupt daily operations.", styles))

    story.append(Paragraph("10.10  Cloudflare Tunnel Issues", styles["H2"]))
    story.append(Paragraph(
        "If the client portal is not reachable through the Cloudflare tunnel, "
        "diagnose using these steps:", styles["Body"]))
    cf_rows = [
        ["Symptom", "Fix"],
        ["502 Bad Gateway from Cloudflare", "LedgerLink service is not running — check Windows Services for LedgerLinkAI"],
        ["Tunnel shows Healthy but site unreachable", "Flush Cloudflare cache; verify tunnel points to http://127.0.0.1:8788"],
        ["SSL certificate error", "Let Cloudflare handle TLS — do not configure TLS on the local service"],
        ["Slow uploads through tunnel", "Check server upload bandwidth; Cloudflare has a 100 MB body limit on free plan"],
        ["Tunnel disconnects randomly", "Enable tunnel keep-alive in cloudflared config; update cloudflared to latest"],
    ]
    story.append(std_table(cf_rows[0], cf_rows[1:], [2.2*inch, 4.3*inch]))
    story.append(Spacer(1, 8))
    story.append(PageBreak())


# ── Section 11: Glossary ──────────────────────────────────────────────────────
def build_section_11_glossary(story, styles):
    story.append(Paragraph("Section 11 — Glossary (FR / EN)", styles["H1"]))
    story.extend(section_rule(styles))
    story.append(Paragraph(
        "The following table provides bilingual definitions for key terms used "
        "throughout this manual and in the LedgerLink AI interface.", styles["Body"]))
    story.append(Spacer(1, 8))

    glossary = [
        ["English Term", "Terme francais", "Definition"],
        ["Accounts Payable", "Comptes fournisseurs", "Amounts owed by the business to suppliers for goods or services received but not yet paid."],
        ["Accounts Receivable", "Comptes clients", "Amounts owed to the business by customers for goods or services delivered but not yet paid."],
        ["AI Router", "Routeur IA", "The LedgerLink component that selects DeepSeek or OpenRouter for each AI task based on complexity."],
        ["Analytical Procedures", "Procedures analytiques", "Audit techniques that compare financial data to expectations to identify unusual fluctuations."],
        ["Audit Evidence", "Elements probants", "Information used by the auditor to support the audit opinion (three-way match, confirmations, etc.)."],
        ["Audit Log", "Journal d'audit", "Immutable record of all user actions and system events in LedgerLink AI."],
        ["Bank Matcher", "Rapprocheur bancaire", "LedgerLink engine that matches bank statement transactions to posted documents."],
        ["bcrypt", "bcrypt", "Secure password hashing algorithm used by LedgerLink AI. 12 rounds is the minimum."],
        ["Chart of Accounts", "Plan comptable", "Complete list of GL accounts used by a client entity."],
        ["Cloudflare Tunnel", "Tunnel Cloudflare", "Secure zero-trust network tunnel used to expose the client portal to the internet safely."],
        ["Compilation", "Compilation", "Lowest level of assurance — accountant compiles financial information with no verification."],
        ["CRA", "ARC", "Canada Revenue Agency — federal tax authority. Administers GST and corporate income tax."],
        ["Credit Note", "Note de credit", "Document issued by a vendor to reduce a previous invoice amount."],
        ["Days Payable Outstanding", "Delai moyen de paiement", "Average number of days a company takes to pay its suppliers."],
        ["DeepSeek", "DeepSeek", "AI provider used for routine tasks (classification, extraction) due to low cost."],
        ["Duplicate Guard", "Detecteur de doublons", "LedgerLink engine that prevents the same document from being processed twice using SHA-256 fingerprinting."],
        ["Engagement", "Mandat", "A formal accounting or audit assignment for a specific client and fiscal period."],
        ["Exception Queue", "File d'exceptions", "Queue of documents that could not be processed automatically and require manual intervention."],
        ["Filing Calendar", "Calendrier de production", "LedgerLink module tracking GST/QST filing deadlines per client."],
        ["Fingerprint", "Empreinte numerique", "SHA-256 hash of a document used to detect duplicates."],
        ["Fraud Engine", "Moteur anti-fraude", "LedgerLink engine applying 9 rule categories to detect suspicious documents."],
        ["GL Account", "Compte du grand livre", "General Ledger account code used to classify a transaction (e.g., 5100 — Office Supplies)."],
        ["GST (TPS)", "TPS (Taxe sur les produits et services)", "Federal Goods and Services Tax — 5% rate applicable across Canada."],
        ["Hallucination Guard", "Garde contre les hallucinations", "LedgerLink module that validates AI output against hard rules to prevent incorrect data entry."],
        ["HST", "TVH", "Harmonized Sales Tax — combines GST and provincial tax in Ontario (13%) and Atlantic provinces (15%)."],
        ["ITC", "CTI (Credit de taxe sur les intrants)", "Input Tax Credit — GST paid on business purchases, recoverable on the GST return."],
        ["ITR", "RTI (Remboursement de la taxe sur les intrants)", "Input Tax Refund — QST paid on business purchases, recoverable on the QST return."],
        ["JWT", "JWT (Jeton d'acces)", "JSON Web Token — used for session management in LedgerLink AI."],
        ["Lead Sheet", "Feuille de travail principale", "Summary working paper for a GL account showing opening balance, adjustments, and closing balance."],
        ["Learning Memory", "Memoire d'apprentissage", "LedgerLink database of past corrections used to suggest GL codes and tax codes for future documents."],
        ["OCR", "ROC (Reconnaissance optique)", "Optical Character Recognition — converts scanned images into machine-readable text."],
        ["OpenRouter", "OpenRouter", "AI provider used for complex reasoning tasks. Higher quality, higher cost than DeepSeek."],
        ["Period Lock", "Verrouillage de periode", "Prevents changes to documents in a closed accounting period."],
        ["Posting", "Ecriture comptable", "Writing an approved transaction to QuickBooks Online."],
        ["QBO", "QuickBooks En Ligne", "QuickBooks Online — cloud accounting software by Intuit, primary posting target."],
        ["QST (TVQ)", "TVQ (Taxe de vente du Quebec)", "Quebec Sales Tax — 9.975% rate administered by Revenu Quebec."],
        ["Quick Method", "Methode rapide", "Simplified GST/QST remittance option for small businesses with revenue under $400,000/year."],
        ["Revenu Quebec", "Revenu Quebec", "Quebec provincial tax authority. Administers QST and Quebec income tax."],
        ["Review Queue", "File de revision", "The central list of documents awaiting accountant review in LedgerLink AI."],
        ["Sampling", "Echantillonnage", "Statistical technique for selecting a representative subset of transactions for audit testing."],
        ["SHA-256", "SHA-256", "Cryptographic hash function — used for document fingerprinting (not passwords; passwords use bcrypt)."],
        ["Tax Code", "Code fiscal", "Single-letter code (T/Z/E/M/I) assigned to a document determining GST/QST treatment."],
        ["Tick Mark", "Crochet de verification", "Auditor notation indicating a specific procedure was performed on a working paper item."],
        ["Trial Balance", "Balance de verification", "List of all GL account balances before and after adjustments used to prepare financial statements."],
        ["Vendor Memory", "Memoire fournisseur", "LedgerLink database of vendor payment history used for fraud detection and pattern learning."],
        ["Working Paper", "Papier de travail", "Documentation supporting an audit conclusion, organized by GL account in LedgerLink AI."],
        ["Zero-Rated (Z)", "Detaxe (Z)", "Supply taxable at 0% GST/QST — vendor charges no tax, but can still claim ITCs/ITRs."],
        ["Balance Sheet", "Bilan", "Financial statement showing assets, liabilities, and equity at a point in time."],
        ["Income Statement", "Etat des resultats", "Financial statement showing revenue, expenses, and net income for a period."],
        ["Chart of Accounts (QC)", "Plan comptable quebecois (PCGQ)", "Quebec-standard chart of accounts used by many Quebec businesses and CPA firms."],
        ["Audit Engagement", "Mission de verification", "Full audit engagement under CAS — provides reasonable assurance on financial statements."],
        ["Review Engagement", "Mission d'examen", "Limited assurance engagement under CSRE 2400 — negative assurance form of reporting."],
        ["Compilation Engagement", "Mission de compilation", "No assurance — accountant compiles financial information from client-provided data."],
        ["Tick Mark", "Pointage", "Auditor notation on a working paper indicating a specific procedure was performed."],
        ["QPP", "RRQ (Regime de rentes du Quebec)", "Quebec Pension Plan — Quebec's equivalent of the Canada Pension Plan (CPP)."],
        ["QPIP", "RQAP (Regime quebecois d'assurance parentale)", "Quebec Parental Insurance Plan — provides maternity/parental/adoption benefits."],
        ["HSF", "FSS (Fonds des services de sante)", "Health Services Fund — Quebec employer-only payroll contribution."],
        ["CNESST", "CNESST", "Quebec workplace safety and compensation commission — workers comp insurance."],
        ["RL-1", "Releve 1", "Quebec employment income slip — filed with Revenu Quebec (analogous to federal T4)."],
        ["T4", "T4 (Feuillet de renseignements)", "Federal employment income slip — filed with CRA for each employee."],
        ["Materiality", "Seuil de signification", "Maximum tolerable misstatement in financial statements (CAS 320)."],
        ["Going Concern", "Continuite d'exploitation", "Assumption that the entity will continue operating for the foreseeable future (CAS 570)."],
        ["Subsequent Events", "Evenements posterieurs", "Events occurring between period end and audit report date (CAS 560)."],
        ["Management Rep Letter", "Lettre de declaration de la direction", "Written representations from management required by CAS 580."],
        ["Related Parties", "Parties liees", "Entities or individuals with control or significant influence over the entity (CAS 550)."],
        ["Quick Method", "Methode rapide", "Simplified GST/QST remittance option for eligible small businesses."],
        ["Substance Engine", "Moteur de substance", "LedgerLink engine classifying CapEx, prepaids, loans, and other special transaction types."],
        ["OpenClaw", "OpenClaw", "Messaging bridge connecting LedgerLink AI to WhatsApp and Telegram for document intake."],
        ["QR Code", "Code QR", "Machine-readable barcode linking clients to the portal login page."],
    ]

    # Split into three parts to avoid huge table
    story.append(std_table(glossary[0], glossary[1:24], [1.5*inch, 2.0*inch, 3.0*inch]))
    story.append(Spacer(1, 12))
    story.append(std_table(glossary[0], glossary[24:48], [1.5*inch, 2.0*inch, 3.0*inch]))
    story.append(Spacer(1, 12))
    story.append(std_table(glossary[0], glossary[48:], [1.5*inch, 2.0*inch, 3.0*inch]))
    story.append(Spacer(1, 12))

    story.extend(tip_box(
        "LedgerLink AI displays field labels in French when the user language is set "
        "to FR. The terms in this glossary correspond to both UI languages. "
        "Bilingual CPAs can switch language per-session via the language toggle "
        "in the top navigation bar.", styles))

    story.append(Paragraph("CAS Standards Quick Reference", styles["H2"]))
    story.append(Paragraph(
        "The following table provides a quick reference to all Canadian Auditing Standards "
        "(CAS) referenced throughout this manual and supported by LedgerLink AI:", styles["Body"]))

    cas_rows = [
        ["Standard", "Title", "LedgerLink AI Support"],
        ["CAS 200", "Overall Objectives of the Independent Auditor",
         "Engagement creation and acceptance workflow"],
        ["CAS 210", "Agreeing the Terms of Audit Engagements",
         "Engagement letter template generation"],
        ["CAS 220", "Quality Management for an Audit of Financial Statements",
         "Tiered sign-off workflow (staff/manager/partner)"],
        ["CAS 230", "Audit Documentation",
         "Working papers, evidence linking, immutable audit trail"],
        ["CAS 240", "The Auditor's Responsibilities Relating to Fraud",
         "Fraud engine with 13 rule categories"],
        ["CAS 260", "Communication with Those Charged with Governance",
         "Management communication templates"],
        ["CAS 315", "Identifying and Assessing the Risks of Material Misstatement",
         "Risk assessment matrix with assertion-level mapping"],
        ["CAS 320", "Materiality in Planning and Performing an Audit",
         "Three-level materiality calculator"],
        ["CAS 330", "The Auditor's Responses to Assessed Risks",
         "Control testing module with deviation rate analysis"],
        ["CAS 500", "Audit Evidence",
         "Assertion coverage matrix across all significant accounts"],
        ["CAS 505", "External Confirmations",
         "Confirmation tracking in audit evidence module"],
        ["CAS 520", "Analytical Procedures",
         "Automated variance analysis and ratio calculations"],
        ["CAS 530", "Audit Sampling",
         "Statistical sampling with reproducible random seeds"],
        ["CAS 540", "Auditing Accounting Estimates",
         "Estimate review checklist (integration planned)"],
        ["CAS 550", "Related Parties",
         "Related party register with transaction tracking"],
        ["CAS 560", "Subsequent Events",
         "Post-period transaction scanning and flagging"],
        ["CAS 570", "Going Concern",
         "Financial indicator monitoring with warning thresholds"],
        ["CAS 580", "Written Representations",
         "Management representation letter generator"],
        ["CAS 700", "Forming an Opinion and Reporting",
         "Engagement completion workflow with opinion tracking"],
        ["CAS 705", "Modifications to the Opinion",
         "Modified opinion documentation support"],
        ["CSQC 1", "Quality Control for Firms",
         "Firm-level quality control documentation framework"],
    ]
    story.append(std_table(cas_rows[0], cas_rows[1:], [0.7*inch, 2.8*inch, 3.0*inch]))
    story.append(Spacer(1, 12))

    story.append(Paragraph("Keyboard Shortcuts", styles["H2"]))
    story.append(Paragraph(
        "The following keyboard shortcuts are available in the review dashboard:", styles["Body"]))
    kb_rows = [
        ["Shortcut", "Action", "Available On"],
        ["A", "Approve selected document", "Document detail page"],
        ["H", "Place document on hold", "Document detail page"],
        ["R", "Return document to ready", "Document detail page"],
        ["N", "Next document in queue", "Document detail page"],
        ["P", "Previous document in queue", "Document detail page"],
        ["S", "Save changes", "Document detail page"],
        ["Ctrl + Enter", "Submit form / Confirm action", "All forms"],
        ["Esc", "Close dialog / Cancel", "All dialogs"],
        ["/", "Focus search field", "Queue page"],
        ["F5", "Refresh queue", "Queue page"],
        ["Ctrl + P", "Print / Export PDF", "Report pages"],
    ]
    story.append(std_table(kb_rows[0], kb_rows[1:], [1.2*inch, 2.5*inch, 2.8*inch]))
    story.append(Spacer(1, 12))
    story.append(PageBreak())


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    styles = make_styles()

    doc = SimpleDocTemplate(
        OUTPUT_PATH,
        pagesize=LETTER,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.85 * inch,
        bottomMargin=0.65 * inch,
        title="LedgerLink AI User Manual",
        author="LedgerLink AI Inc.",
        subject="User Manual — English Edition 2026",
    )

    story = []

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

    doc.build(
        story,
        onFirstPage=on_page_first,
        onLaterPages=on_page,
    )

    # Count pages
    import subprocess, sys
    try:
        from reportlab.lib.pagesizes import LETTER as _
        from PyPDF2 import PdfReader
        reader = PdfReader(OUTPUT_PATH)
        pages = len(reader.pages)
        print(f"PDF created: {OUTPUT_PATH}")
        print(f"Page count: {pages}")
    except ImportError:
        print(f"PDF created: {OUTPUT_PATH}")
        print("(Install PyPDF2 to get exact page count)")

    file_size = os.path.getsize(OUTPUT_PATH)
    print(f"File size: {file_size:,} bytes ({file_size/1024:.1f} KB)")


if __name__ == "__main__":
    main()
