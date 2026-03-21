"""
scripts/setup_wizard.py — LedgerLink Setup Wizard
Standalone HTTP server on port 8790. Python stdlib only (no Flask).
Bilingual FR/EN (default: FR). Guides the user through 6 setup steps.
"""
from __future__ import annotations

import argparse
import json
import socketserver
import sqlite3
import sys
import traceback
import urllib.parse
from datetime import date, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from pathlib import Path

import bcrypt

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent
STATE_FILE = ROOT_DIR / "data" / "setup_state.json"
CONFIG_FILE = ROOT_DIR / "ledgerlink.config.json"
DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"

# ---------------------------------------------------------------------------
# Bilingual strings
# ---------------------------------------------------------------------------
STRINGS: dict[str, dict[str, str]] = {
    "fr": {
        "title": "Assistant de configuration LedgerLink",
        "step1": "Informations cabinet",
        "step2": "Intelligence artificielle",
        "step3": "Courriel",
        "step4": "Microsoft 365",
        "step5": "Licence",
        "step6": "Terminé",
        "btn_next": "Suivant →",
        "btn_skip": "Ignorer cette étape",
        "btn_save": "Enregistrer",
        "btn_test": "Tester la connexion",
        "btn_open_dashboard": "Ouvrir le tableau de bord",
        "firm_name": "Nom du cabinet",
        "firm_address": "Adresse",
        "gst_number": "Numéro TPS",
        "qst_number": "Numéro TVQ",
        "owner_name": "Nom du propriétaire",
        "owner_email": "Courriel du propriétaire",
        "owner_password": "Mot de passe",
        "owner_password_confirm": "Confirmer le mot de passe",
        "ai_routine_provider": "Fournisseur routinier",
        "ai_routine_url": "URL de l'API",
        "ai_routine_key": "Clé API",
        "ai_routine_model": "Modèle",
        "ai_premium_provider": "Fournisseur premium",
        "smtp_host": "Serveur SMTP",
        "smtp_port": "Port",
        "smtp_user": "Utilisateur SMTP",
        "smtp_password": "Mot de passe SMTP",
        "from_email": "Courriel expéditeur",
        "from_name": "Nom expéditeur",
        "m365_tenant_id": "ID du locataire",
        "m365_client_id": "ID du client",
        "m365_client_secret": "Secret du client",
        "m365_sharepoint_site": "Site SharePoint",
        "license_key": "Clé de licence",
        "license_key_ph": "LLAI-...",
        "setup_complete_title": "Configuration terminée !",
        "setup_complete_msg": "Votre système LedgerLink est prêt à être utilisé.",
        "dashboard_url_label": "URL du tableau de bord",
        "already_complete": "Configuration déjà effectuée",
        "already_complete_msg": "Le système a déjà été configuré. Accédez au tableau de bord.",
        "err_required": "Tous les champs sont obligatoires.",
        "err_passwords": "Les mots de passe ne correspondent pas.",
        "err_email": "Adresse courriel invalide.",
        "validate_license": "Valider",
        "lang_toggle": "English",
        "firm_info_heading": "Informations sur le cabinet",
        "ai_heading": "Configuration de l'IA",
        "email_heading": "Configuration du courriel",
        "m365_heading": "Configuration Microsoft 365",
        "m365_optional": "Cette étape est optionnelle. Vous pouvez l'ignorer.",
        "license_heading": "Activation de la licence",
        "complete_heading": "Configuration terminée",
        "firm_name_label": "Nom du cabinet",
        "test_connection": "Tester la connexion",
        "test_email": "Tester",
        "simulated": "Test simulé (connexion OK)",
        "license_valid": "Licence valide",
        "license_invalid": "Clé de licence invalide",
        "save_success": "Enregistré avec succès",
    },
    "en": {
        "title": "LedgerLink Setup Wizard",
        "step1": "Firm Information",
        "step2": "Artificial Intelligence",
        "step3": "Email",
        "step4": "Microsoft 365",
        "step5": "License",
        "step6": "Complete",
        "btn_next": "Next →",
        "btn_skip": "Skip this step",
        "btn_save": "Save",
        "btn_test": "Test Connection",
        "btn_open_dashboard": "Open Dashboard",
        "firm_name": "Firm Name",
        "firm_address": "Address",
        "gst_number": "GST Number",
        "qst_number": "QST Number",
        "owner_name": "Owner Name",
        "owner_email": "Owner Email",
        "owner_password": "Password",
        "owner_password_confirm": "Confirm Password",
        "ai_routine_provider": "Routine Provider",
        "ai_routine_url": "API URL",
        "ai_routine_key": "API Key",
        "ai_routine_model": "Model",
        "ai_premium_provider": "Premium Provider",
        "smtp_host": "SMTP Host",
        "smtp_port": "Port",
        "smtp_user": "SMTP User",
        "smtp_password": "SMTP Password",
        "from_email": "From Email",
        "from_name": "From Name",
        "m365_tenant_id": "Tenant ID",
        "m365_client_id": "Client ID",
        "m365_client_secret": "Client Secret",
        "m365_sharepoint_site": "SharePoint Site",
        "license_key": "License Key",
        "license_key_ph": "LLAI-...",
        "setup_complete_title": "Setup Complete!",
        "setup_complete_msg": "Your LedgerLink system is ready to use.",
        "dashboard_url_label": "Dashboard URL",
        "already_complete": "Setup Already Complete",
        "already_complete_msg": "The system has already been configured. Access the dashboard.",
        "err_required": "All fields are required.",
        "err_passwords": "Passwords do not match.",
        "err_email": "Invalid email address.",
        "validate_license": "Validate",
        "lang_toggle": "Français",
        "firm_info_heading": "Firm Information",
        "ai_heading": "AI Configuration",
        "email_heading": "Email Configuration",
        "m365_heading": "Microsoft 365 Configuration",
        "m365_optional": "This step is optional. You may skip it.",
        "license_heading": "License Activation",
        "complete_heading": "Setup Complete",
        "firm_name_label": "Firm Name",
        "test_connection": "Test Connection",
        "test_email": "Test",
        "simulated": "Test simulated (connection OK)",
        "license_valid": "License is valid",
        "license_invalid": "Invalid license key",
        "save_success": "Saved successfully",
    },
}

STEP_PATHS = {1: "/setup/firm", 2: "/setup/ai", 3: "/setup/email",
              4: "/setup/microsoft365", 5: "/setup/license", 6: "/setup/complete"}


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

def validate_step1(data: dict) -> list[str]:
    """Validate step 1 form data. Returns list of error strings."""
    errors: list[str] = []
    required_fields = [
        "firm_name", "firm_address", "gst_number", "qst_number",
        "owner_name", "owner_email", "owner_password", "owner_password_confirm",
    ]
    if any(not data.get(f, "").strip() for f in required_fields):
        errors.append("All fields are required.")
        return errors  # no point checking further

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


# ---------------------------------------------------------------------------
# CSS / HTML helpers
# ---------------------------------------------------------------------------

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: #f1f5f9; min-height: 100vh; }
.wizard-wrap { display: flex; min-height: 100vh; }
.sidebar { background: #1e293b; color: #e2e8f0; width: 260px; min-width: 260px;
           padding: 32px 0; display: flex; flex-direction: column; }
.sidebar-brand { padding: 0 24px 32px; border-bottom: 1px solid #334155; }
.sidebar-brand h1 { font-size: 1.25rem; font-weight: 700; color: #fff; }
.sidebar-brand span { font-size: 0.75rem; color: #94a3b8; }
.sidebar-steps { padding: 24px 0; flex: 1; }
.sidebar-step { display: flex; align-items: center; gap: 12px;
                padding: 10px 24px; cursor: default; transition: background .15s; }
.sidebar-step.complete { color: #4ade80; }
.sidebar-step.current { background: #2563eb22; color: #93c5fd; font-weight: 600; }
.sidebar-step.pending { color: #64748b; }
.step-circle { width: 28px; height: 28px; border-radius: 50%; display: flex;
               align-items: center; justify-content: center; font-size: 0.75rem;
               font-weight: 700; flex-shrink: 0; }
.step-circle.complete { background: #16a34a; color: #fff; }
.step-circle.current { background: transparent; border: 2px solid #2563eb; color: #93c5fd; }
.step-circle.pending { background: #334155; color: #64748b; }
.step-label { font-size: 0.875rem; }
.main-area { flex: 1; padding: 40px; overflow-y: auto; }
.topbar { display: flex; justify-content: space-between; align-items: center;
          margin-bottom: 32px; }
.topbar h2 { font-size: 1.5rem; font-weight: 700; color: #1e293b; }
.lang-toggle { background: none; border: 1px solid #cbd5e1; border-radius: 6px;
               padding: 6px 14px; cursor: pointer; font-size: 0.875rem; color: #475569;
               text-decoration: none; }
.lang-toggle:hover { background: #f8fafc; }
.card { background: #fff; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,.08);
        padding: 32px; max-width: 680px; }
.card h3 { font-size: 1.125rem; font-weight: 600; color: #1e293b; margin-bottom: 24px; }
.form-group { margin-bottom: 18px; }
.form-group label { display: block; font-size: 0.875rem; font-weight: 500;
                    color: #374151; margin-bottom: 6px; }
.form-group input, .form-group select, .form-group textarea {
    width: 100%; padding: 10px 12px; border: 1px solid #d1d5db; border-radius: 6px;
    font-size: 0.9rem; color: #1e293b; transition: border-color .15s; }
.form-group input:focus, .form-group select:focus, .form-group textarea:focus {
    outline: none; border-color: #2563eb; box-shadow: 0 0 0 3px #2563eb22; }
.form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.btn { display: inline-flex; align-items: center; gap: 8px;
       padding: 10px 20px; border-radius: 8px; font-size: 0.9rem; font-weight: 500;
       cursor: pointer; border: none; transition: all .15s; text-decoration: none; }
.btn-primary { background: #2563eb; color: #fff; }
.btn-primary:hover { background: #1d4ed8; }
.btn-secondary { background: #f1f5f9; color: #475569; border: 1px solid #e2e8f0; }
.btn-secondary:hover { background: #e2e8f0; }
.btn-outline { background: transparent; border: 1px solid #d1d5db; color: #6b7280; }
.btn-outline:hover { background: #f9fafb; }
.btn-actions { display: flex; gap: 12px; margin-top: 28px; flex-wrap: wrap; }
.alert { padding: 12px 16px; border-radius: 8px; margin-bottom: 20px; font-size: 0.875rem; }
.alert-error { background: #fef2f2; border: 1px solid #fecaca; color: #b91c1c; }
.alert-success { background: #f0fdf4; border: 1px solid #bbf7d0; color: #15803d; }
.alert-info { background: #eff6ff; border: 1px solid #bfdbfe; color: #1d4ed8; }
.test-row { display: flex; gap: 10px; align-items: flex-end; }
.test-row .form-group { flex: 1; margin-bottom: 0; }
.test-result { margin-top: 10px; font-size: 0.85rem; }
.complete-box { text-align: center; padding: 40px 20px; }
.complete-icon { font-size: 4rem; margin-bottom: 16px; }
.complete-box h2 { font-size: 1.75rem; font-weight: 700; color: #1e293b; margin-bottom: 12px; }
.complete-box p { color: #6b7280; margin-bottom: 8px; }
.summary-pill { display: inline-block; background: #eff6ff; color: #1d4ed8;
                border-radius: 20px; padding: 4px 14px; font-size: 0.85rem;
                font-weight: 500; margin: 4px 4px; }
.already-box { text-align: center; padding: 60px 20px; }
.already-box h2 { font-size: 1.5rem; font-weight: 700; color: #1e293b; margin-bottom: 12px; }
.already-box p { color: #6b7280; margin-bottom: 24px; }
@media (max-width: 720px) {
    .wizard-wrap { flex-direction: column; }
    .sidebar { width: 100%; min-width: unset; flex-direction: row; flex-wrap: wrap;
               padding: 16px; }
    .sidebar-brand { border-bottom: none; border-right: 1px solid #334155;
                     padding: 0 16px 0 0; margin-right: 16px; }
    .sidebar-steps { display: flex; flex-wrap: wrap; padding: 0; }
    .sidebar-step { padding: 6px 12px; }
    .main-area { padding: 20px; }
    .form-row { grid-template-columns: 1fr; }
}
"""


def _s(lang: str, key: str) -> str:
    return STRINGS.get(lang, STRINGS["fr"]).get(key, STRINGS["fr"].get(key, key))


def _esc(v: object) -> str:
    import html
    return html.escape("" if v is None else str(v), quote=True)


def _sidebar_html(state: dict, current_step: int, lang: str) -> str:
    steps_complete = set(state.get("steps_complete", []))
    items = ""
    for n in range(1, 7):
        label = _s(lang, f"step{n}")
        if n in steps_complete:
            cls = "complete"
            circle_cls = "complete"
            marker = "✓"
        elif n == current_step:
            cls = "current"
            circle_cls = "current"
            marker = str(n)
        else:
            cls = "pending"
            circle_cls = "pending"
            marker = str(n)
        items += f"""
        <div class="sidebar-step {cls}">
            <div class="step-circle {circle_cls}">{marker}</div>
            <span class="step-label">{_esc(label)}</span>
        </div>"""
    return items


def _page(content: str, state: dict, current_step: int, lang: str,
          title: str = "") -> str:
    lang_url = f"/setup/lang?set={'en' if lang == 'fr' else 'fr'}"
    lang_label = _s(lang, "lang_toggle")
    page_title = title or _s(lang, "title")
    sidebar_items = _sidebar_html(state, current_step, lang)
    wizard_title = _s(lang, "title")
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
      <span>{_esc(wizard_title)}</span>
    </div>
    <div class="sidebar-steps">
      {sidebar_items}
    </div>
  </div>
  <div class="main-area">
    <div class="topbar">
      <h2>{_esc(page_title)}</h2>
      <a class="lang-toggle" href="{_esc(lang_url)}">{_esc(lang_label)}</a>
    </div>
    {content}
  </div>
</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Step renderers
# ---------------------------------------------------------------------------

def _render_step1(lang: str, state: dict, error: str = "", data: dict | None = None) -> str:
    d = data or {}
    err_html = f'<div class="alert alert-error">{_esc(error)}</div>' if error else ""
    return _page(f"""
{err_html}
<div class="card">
  <h3>{_esc(_s(lang, "firm_info_heading"))}</h3>
  <form method="POST" action="/setup/firm">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, "firm_name"))}</label>
        <input type="text" name="firm_name" value="{_esc(d.get('firm_name',''))}" required>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, "firm_address"))}</label>
        <input type="text" name="firm_address" value="{_esc(d.get('firm_address',''))}" required>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, "gst_number"))}</label>
        <input type="text" name="gst_number" value="{_esc(d.get('gst_number',''))}" required>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, "qst_number"))}</label>
        <input type="text" name="qst_number" value="{_esc(d.get('qst_number',''))}" required>
      </div>
    </div>
    <hr style="margin: 20px 0; border-color: #e2e8f0;">
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, "owner_name"))}</label>
        <input type="text" name="owner_name" value="{_esc(d.get('owner_name',''))}" required>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, "owner_email"))}</label>
        <input type="email" name="owner_email" value="{_esc(d.get('owner_email',''))}" required>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, "owner_password"))}</label>
        <input type="password" name="owner_password" required>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, "owner_password_confirm"))}</label>
        <input type="password" name="owner_password_confirm" required>
      </div>
    </div>
    <div class="btn-actions">
      <button type="submit" class="btn btn-primary">{_esc(_s(lang, "btn_next"))}</button>
    </div>
  </form>
</div>
""", state, 1, lang, _s(lang, "step1"))


def _render_step2(lang: str, state: dict, error: str = "", success: str = "") -> str:
    cfg = load_config().get("ai_router", {})
    routine = cfg.get("routine", cfg.get("routine_provider", {}))
    premium = cfg.get("premium", cfg.get("premium_provider", {}))
    err_html = f'<div class="alert alert-error">{_esc(error)}</div>' if error else ""
    ok_html = f'<div class="alert alert-success">{_esc(success)}</div>' if success else ""
    providers_html = "<option value='openrouter'>OpenRouter</option><option value='openai'>OpenAI</option><option value='local'>Local</option>"
    return _page(f"""
{err_html}{ok_html}
<div class="card">
  <h3>{_esc(_s(lang, "ai_heading"))}</h3>
  <form method="POST" action="/setup/ai">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    <p style="font-size:.85rem;color:#6b7280;margin-bottom:20px;">Routine (standard tasks) &amp; Premium (complex tasks)</p>
    <h4 style="margin-bottom:12px;color:#374151;">Routine</h4>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, "ai_routine_provider"))}</label>
        <select name="routine_provider">{providers_html}</select>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, "ai_routine_model"))}</label>
        <input type="text" name="routine_model" value="{_esc(routine.get('model','deepseek-chat'))}">
      </div>
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, "ai_routine_url"))}</label>
      <input type="url" name="routine_url" value="{_esc(routine.get('base_url',''))}">
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, "ai_routine_key"))}</label>
      <input type="text" name="routine_key" value="{_esc(routine.get('api_key',''))}">
    </div>
    <h4 style="margin:20px 0 12px;color:#374151;">Premium</h4>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, "ai_premium_provider"))}</label>
        <select name="premium_provider">{providers_html}</select>
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, "ai_routine_model"))}</label>
        <input type="text" name="premium_model" value="{_esc(premium.get('model','anthropic/claude-sonnet-4-6'))}">
      </div>
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, "ai_routine_url"))}</label>
      <input type="url" name="premium_url" value="{_esc(premium.get('base_url',''))}">
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, "ai_routine_key"))}</label>
      <input type="text" name="premium_key" value="{_esc(premium.get('api_key',''))}">
    </div>
    <div class="btn-actions">
      <button type="submit" class="btn btn-primary">{_esc(_s(lang, "btn_next"))}</button>
      <button type="button" class="btn btn-secondary" id="testAiBtn" onclick="testAiConn()">{_esc(_s(lang, "btn_test"))}</button>
    </div>
    <div id="testAiResult" class="test-result"></div>
  </form>
</div>
<script>
async function testAiConn() {{
  var btn = document.getElementById('testAiBtn');
  btn.disabled = true;
  btn.textContent = '...';
  try {{
    var form = document.querySelector('form');
    var data = new FormData(form);
    var params = new URLSearchParams(data);
    var r = await fetch('/setup/ai/test', {{method:'POST', body: params,
      headers: {{'Content-Type':'application/x-www-form-urlencoded'}}}});
    var j = await r.json();
    var el = document.getElementById('testAiResult');
    el.innerHTML = '<span style="color:' + (j.ok ? '#16a34a' : '#b91c1c') + '">' + j.message + '</span>';
  }} catch(e) {{ document.getElementById('testAiResult').textContent = 'Error: ' + e; }}
  btn.disabled = false;
  btn.textContent = '{_esc(_s(lang, "btn_test"))}';
}}
</script>
""", state, 2, lang, _s(lang, "step2"))


def _render_step3(lang: str, state: dict, error: str = "", success: str = "") -> str:
    cfg = load_config().get("email", load_config().get("digest", {}))
    err_html = f'<div class="alert alert-error">{_esc(error)}</div>' if error else ""
    ok_html = f'<div class="alert alert-success">{_esc(success)}</div>' if success else ""
    return _page(f"""
{err_html}{ok_html}
<div class="card">
  <h3>{_esc(_s(lang, "email_heading"))}</h3>
  <form method="POST" action="/setup/email">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, "smtp_host"))}</label>
        <input type="text" name="smtp_host" value="{_esc(cfg.get('smtp_host','smtp.gmail.com'))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, "smtp_port"))}</label>
        <input type="number" name="smtp_port" value="{_esc(cfg.get('smtp_port','587'))}">
      </div>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, "smtp_user"))}</label>
        <input type="text" name="smtp_user" value="{_esc(cfg.get('smtp_user',''))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, "smtp_password"))}</label>
        <input type="password" name="smtp_password" value="{_esc(cfg.get('smtp_password',''))}">
      </div>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, "from_email"))}</label>
        <input type="email" name="from_email" value="{_esc(cfg.get('from_address',''))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, "from_name"))}</label>
        <input type="text" name="from_name" value="{_esc(cfg.get('from_name','LedgerLink AI'))}">
      </div>
    </div>
    <div class="btn-actions">
      <button type="submit" class="btn btn-primary">{_esc(_s(lang, "btn_next"))}</button>
      <button type="button" class="btn btn-secondary" id="testEmailBtn" onclick="testEmail()">{_esc(_s(lang, "test_email"))}</button>
    </div>
    <div id="testEmailResult" class="test-result"></div>
  </form>
</div>
<script>
async function testEmail() {{
  var btn = document.getElementById('testEmailBtn');
  btn.disabled = true;
  try {{
    var form = document.querySelector('form');
    var data = new FormData(form);
    var params = new URLSearchParams(data);
    var r = await fetch('/setup/email/test', {{method:'POST', body: params,
      headers: {{'Content-Type':'application/x-www-form-urlencoded'}}}});
    var j = await r.json();
    var el = document.getElementById('testEmailResult');
    el.innerHTML = '<span style="color:' + (j.ok ? '#16a34a' : '#b91c1c') + '">' + j.message + '</span>';
  }} catch(e) {{ document.getElementById('testEmailResult').textContent = 'Error: ' + e; }}
  btn.disabled = false;
  btn.textContent = '{_esc(_s(lang, "test_email"))}';
}}
</script>
""", state, 3, lang, _s(lang, "step3"))


def _render_step4(lang: str, state: dict, error: str = "", success: str = "") -> str:
    cfg = load_config().get("microsoft365", {})
    err_html = f'<div class="alert alert-error">{_esc(error)}</div>' if error else ""
    ok_html = f'<div class="alert alert-success">{_esc(success)}</div>' if success else ""
    return _page(f"""
{err_html}{ok_html}
<div class="card">
  <h3>{_esc(_s(lang, "m365_heading"))}</h3>
  <div class="alert alert-info" style="margin-bottom:20px;">{_esc(_s(lang, "m365_optional"))}</div>
  <form method="POST" action="/setup/microsoft365">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    <div class="form-group">
      <label>{_esc(_s(lang, "m365_tenant_id"))}</label>
      <input type="text" name="tenant_id" value="{_esc(cfg.get('tenant_id',''))}">
    </div>
    <div class="form-row">
      <div class="form-group">
        <label>{_esc(_s(lang, "m365_client_id"))}</label>
        <input type="text" name="client_id" value="{_esc(cfg.get('client_id',''))}">
      </div>
      <div class="form-group">
        <label>{_esc(_s(lang, "m365_client_secret"))}</label>
        <input type="password" name="client_secret" value="{_esc(cfg.get('client_secret',''))}">
      </div>
    </div>
    <div class="form-group">
      <label>{_esc(_s(lang, "m365_sharepoint_site"))}</label>
      <input type="text" name="sharepoint_site" value="{_esc(cfg.get('sharepoint_site',''))}">
    </div>
    <div class="btn-actions">
      <button type="submit" name="action" value="save" class="btn btn-primary">{_esc(_s(lang, "btn_save"))}</button>
      <button type="submit" name="action" value="skip" class="btn btn-outline">{_esc(_s(lang, "btn_skip"))}</button>
    </div>
  </form>
</div>
""", state, 4, lang, _s(lang, "step4"))


def _render_step5(lang: str, state: dict, error: str = "", success: str = "") -> str:
    cfg = load_config().get("license", {})
    err_html = f'<div class="alert alert-error">{_esc(error)}</div>' if error else ""
    ok_html = f'<div class="alert alert-success">{_esc(success)}</div>' if success else ""
    return _page(f"""
{err_html}{ok_html}
<div class="card">
  <h3>{_esc(_s(lang, "license_heading"))}</h3>
  <form method="POST" action="/setup/license">
    <input type="hidden" name="lang" value="{_esc(lang)}">
    <div class="form-group">
      <label>{_esc(_s(lang, "license_key"))}</label>
      <textarea name="license_key" rows="3" style="width:100%;padding:10px 12px;border:1px solid #d1d5db;border-radius:6px;font-family:monospace;font-size:0.85rem;"
        placeholder="{_esc(_s(lang, 'license_key_ph'))}">{_esc(cfg.get('key',''))}</textarea>
    </div>
    <div class="btn-actions">
      <button type="submit" class="btn btn-primary">{_esc(_s(lang, "btn_next"))}</button>
      <button type="button" class="btn btn-secondary" id="validateBtn" onclick="validateLic()">{_esc(_s(lang, "validate_license"))}</button>
    </div>
    <div id="licResult" class="test-result"></div>
  </form>
</div>
<script>
async function validateLic() {{
  var btn = document.getElementById('validateBtn');
  btn.disabled = true;
  try {{
    var key = document.querySelector('[name=license_key]').value;
    var params = new URLSearchParams({{license_key: key, lang: '{_esc(lang)}'}});
    var r = await fetch('/setup/license/validate', {{method:'POST', body: params,
      headers: {{'Content-Type':'application/x-www-form-urlencoded'}}}});
    var j = await r.json();
    var el = document.getElementById('licResult');
    if (j.ok) {{
      el.innerHTML = '<span style="color:#16a34a">✓ ' + j.message + '</span>';
    }} else {{
      el.innerHTML = '<span style="color:#b91c1c">✗ ' + j.message + '</span>';
    }}
  }} catch(e) {{ document.getElementById('licResult').textContent = 'Error: ' + e; }}
  btn.disabled = false;
  btn.textContent = '{_esc(_s(lang, "validate_license"))}';
}}
</script>
""", state, 5, lang, _s(lang, "step5"))


def _render_step6(lang: str, state: dict) -> str:
    cfg = load_config()
    firm = cfg.get("firm", {})
    firm_name = firm.get("firm_name", "LedgerLink")
    lic = cfg.get("license", {})
    tier = "—"
    if lic.get("key"):
        try:
            from src.engines.license_engine import get_license_status
            status = get_license_status()
            tier = status.get("tier", "—")
        except Exception:
            pass
    port = cfg.get("port", 8787)
    dash_url = f"http://127.0.0.1:{port}"
    return _page(f"""
<div class="card">
  <div class="complete-box">
    <div class="complete-icon">🎉</div>
    <h2>{_esc(_s(lang, "setup_complete_title"))}</h2>
    <p>{_esc(_s(lang, "setup_complete_msg"))}</p>
    <div style="margin: 20px 0;">
      <span class="summary-pill">{_esc(firm_name)}</span>
      <span class="summary-pill">{_esc(tier)}</span>
    </div>
    <p style="margin-bottom:6px;"><strong>{_esc(_s(lang, "dashboard_url_label"))}:</strong></p>
    <p><a href="{_esc(dash_url)}" style="color:#2563eb;">{_esc(dash_url)}</a></p>
    <div style="margin-top:32px;">
      <a href="{_esc(dash_url)}" class="btn btn-primary" style="font-size:1rem;padding:14px 28px;">
        {_esc(_s(lang, "btn_open_dashboard"))}
      </a>
    </div>
  </div>
</div>
""", state, 6, lang, _s(lang, "step6"))


def _render_already_complete(lang: str, state: dict) -> str:
    cfg = load_config()
    port = cfg.get("port", 8787)
    dash_url = f"http://127.0.0.1:{port}"
    return _page(f"""
<div class="card">
  <div class="already-box">
    <h2>{_esc(_s(lang, "already_complete"))}</h2>
    <p>{_esc(_s(lang, "already_complete_msg"))}</p>
    <a href="{_esc(dash_url)}" class="btn btn-primary">{_esc(_s(lang, "btn_open_dashboard"))}</a>
  </div>
</div>
""", state, 6, lang, _s(lang, "already_complete"))


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

class SetupWizardHandler(BaseHTTPRequestHandler):
    server_version = "LedgerLinkSetupWizard/1.0"

    def log_message(self, fmt: str, *args: object) -> None:  # type: ignore[override]
        pass  # suppress access log noise

    # ------------------------------------------------------------------
    # Cookie helpers
    # ------------------------------------------------------------------

    def _get_lang(self) -> str:
        cookie_str = self.headers.get("Cookie", "")
        for part in cookie_str.split(";"):
            part = part.strip()
            if part.startswith("wizard_lang="):
                val = part[len("wizard_lang="):]
                if val in ("fr", "en"):
                    return val
        return "fr"

    # ------------------------------------------------------------------
    # Response helpers
    # ------------------------------------------------------------------

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

    def _first_incomplete_step(self, state: dict) -> int:
        steps_complete = set(state.get("steps_complete", []))
        for n in range(1, 7):
            if n not in steps_complete:
                return n
        return 6

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
                referer = self.headers.get("Referer", "/")
                self._redirect(referer, extra_headers=[
                    ("Set-Cookie", f"wizard_lang={new_lang}; Path=/; SameSite=Lax"),
                ])
                return

            # Root: redirect to first incomplete step or already-complete page
            if path in ("/", ""):
                if state.get("setup_complete"):
                    self._send_html(_render_already_complete(lang, state))
                    return
                step = self._first_incomplete_step(state)
                self._redirect(STEP_PATHS[step])
                return

            if path == "/setup/firm":
                if state.get("setup_complete"):
                    self._send_html(_render_already_complete(lang, state))
                    return
                self._send_html(_render_step1(lang, state))
                return

            if path == "/setup/ai":
                if state.get("setup_complete"):
                    self._send_html(_render_already_complete(lang, state))
                    return
                self._send_html(_render_step2(lang, state))
                return

            if path == "/setup/email":
                if state.get("setup_complete"):
                    self._send_html(_render_already_complete(lang, state))
                    return
                self._send_html(_render_step3(lang, state))
                return

            if path == "/setup/microsoft365":
                if state.get("setup_complete"):
                    self._send_html(_render_already_complete(lang, state))
                    return
                self._send_html(_render_step4(lang, state))
                return

            if path == "/setup/license":
                if state.get("setup_complete"):
                    self._send_html(_render_already_complete(lang, state))
                    return
                self._send_html(_render_step5(lang, state))
                return

            if path == "/setup/complete":
                # Mark complete on visit
                steps_complete = set(state.get("steps_complete", []))
                steps_complete.add(6)
                state["steps_complete"] = list(steps_complete)
                state["setup_complete"] = True
                save_state(state)
                self._send_html(_render_step6(lang, state))
                return

            # 404
            self._send_html(f"<h2>404 Not Found</h2><p><a href='/'>Home</a></p>", 404)

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

            # -- Step 1: Firm info --
            if path == "/setup/firm":
                errors = validate_step1(form)
                if errors:
                    self._send_html(_render_step1(lang, state, error=" ".join(errors), data=form))
                    return
                # Save to config
                cfg = load_config()
                cfg["firm"] = {
                    "firm_name": form["firm_name"].strip(),
                    "firm_address": form["firm_address"].strip(),
                    "gst_number": form["gst_number"].strip(),
                    "qst_number": form["qst_number"].strip(),
                    "owner_name": form["owner_name"].strip(),
                    "owner_email": form["owner_email"].strip(),
                }
                save_config(cfg)
                # Create owner user
                try:
                    upsert_owner_user(
                        email=form["owner_email"].strip(),
                        name=form["owner_name"].strip(),
                        password=form["owner_password"],
                        lang=lang,
                    )
                except Exception as exc:
                    self._send_html(_render_step1(lang, state, error=str(exc), data=form))
                    return
                # Mark step 1 complete
                steps_complete = set(state.get("steps_complete", []))
                steps_complete.add(1)
                state["steps_complete"] = list(steps_complete)
                save_state(state)
                self._redirect("/setup/ai")
                return

            # -- Step 2: AI config --
            if path == "/setup/ai":
                cfg = load_config()
                cfg["ai_router"] = {
                    "routine": {
                        "provider": form.get("routine_provider", "openrouter"),
                        "base_url": form.get("routine_url", ""),
                        "api_key": form.get("routine_key", ""),
                        "model": form.get("routine_model", ""),
                    },
                    "premium": {
                        "provider": form.get("premium_provider", "openrouter"),
                        "base_url": form.get("premium_url", ""),
                        "api_key": form.get("premium_key", ""),
                        "model": form.get("premium_model", ""),
                    },
                }
                save_config(cfg)
                steps_complete = set(state.get("steps_complete", []))
                steps_complete.add(2)
                state["steps_complete"] = list(steps_complete)
                save_state(state)
                self._redirect("/setup/email")
                return

            # -- Step 2: AI test --
            if path == "/setup/ai/test":
                url = form.get("routine_url", "").strip()
                key = form.get("routine_key", "").strip()
                if not url:
                    self._send_json({"ok": False, "message": "API URL is required"})
                    return
                try:
                    import urllib.request
                    req = urllib.request.Request(
                        url.rstrip("/") + "/models",
                        headers={"Authorization": f"Bearer {key}"},
                    )
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        self._send_json({"ok": True, "message": f"Connected ({resp.status})"})
                except Exception as exc:
                    self._send_json({"ok": False, "message": f"Connection failed: {exc}"})
                return

            # -- Step 3: Email config --
            if path == "/setup/email":
                cfg = load_config()
                try:
                    smtp_port = int(form.get("smtp_port", "587"))
                except ValueError:
                    smtp_port = 587
                cfg["email"] = {
                    "smtp_host": form.get("smtp_host", "").strip(),
                    "smtp_port": smtp_port,
                    "smtp_user": form.get("smtp_user", "").strip(),
                    "smtp_password": form.get("smtp_password", ""),
                    "from_address": form.get("from_email", "").strip(),
                    "from_name": form.get("from_name", "LedgerLink AI").strip(),
                    "enabled": True,
                }
                save_config(cfg)
                steps_complete = set(state.get("steps_complete", []))
                steps_complete.add(3)
                state["steps_complete"] = list(steps_complete)
                save_state(state)
                self._redirect("/setup/microsoft365")
                return

            # -- Step 3: Email test (simulated) --
            if path == "/setup/email/test":
                host = form.get("smtp_host", "").strip()
                if not host:
                    self._send_json({"ok": False, "message": "SMTP host is required"})
                    return
                self._send_json({"ok": True, "message": "Test email sent (simulated)"})
                return

            # -- Step 4: Microsoft 365 --
            if path == "/setup/microsoft365":
                action = form.get("action", "save")
                if action == "skip":
                    steps_complete = set(state.get("steps_complete", []))
                    steps_complete.add(4)
                    state["steps_complete"] = list(steps_complete)
                    save_state(state)
                    self._redirect("/setup/license")
                    return
                cfg = load_config()
                cfg["microsoft365"] = {
                    "tenant_id": form.get("tenant_id", "").strip(),
                    "client_id": form.get("client_id", "").strip(),
                    "client_secret": form.get("client_secret", ""),
                    "sharepoint_site": form.get("sharepoint_site", "").strip(),
                }
                save_config(cfg)
                steps_complete = set(state.get("steps_complete", []))
                steps_complete.add(4)
                state["steps_complete"] = list(steps_complete)
                save_state(state)
                self._redirect("/setup/license")
                return

            # -- Step 5: License validate (AJAX) --
            if path == "/setup/license/validate":
                key = form.get("license_key", "").strip()
                if not key:
                    self._send_json({"ok": False, "message": "License key is required"})
                    return
                try:
                    from src.engines.license_engine import load_license, get_signing_secret
                    secret = get_signing_secret()
                    payload = load_license(key, secret)
                    tier = payload.get("tier", "—")
                    expiry = payload.get("expiry_date", "—")
                    self._send_json({"ok": True, "message": f"Valid — tier: {tier}, expires: {expiry}"})
                except Exception as exc:
                    self._send_json({"ok": False, "message": str(exc)})
                return

            # -- Step 5: License save --
            if path == "/setup/license":
                key = form.get("license_key", "").strip()
                if key:
                    try:
                        from src.engines.license_engine import save_license_to_config, get_signing_secret
                        secret = get_signing_secret()
                        save_license_to_config(key, secret)
                    except Exception as exc:
                        self._send_html(_render_step5(lang, state, error=str(exc)))
                        return
                steps_complete = set(state.get("steps_complete", []))
                steps_complete.add(5)
                state["steps_complete"] = list(steps_complete)
                save_state(state)
                self._redirect("/setup/complete")
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
