#!/usr/bin/env python3
"""
autofix.py — OtoCPA self-healing diagnostic & repair script
==================================================================
Checks 10 health conditions in order and auto-fixes where possible.

Usage:
    python scripts/autofix.py [--lang en|fr]
"""
from __future__ import annotations

# Force UTF-8 output on Windows before any print() calls
import sys as _sys
if hasattr(_sys.stdout, "reconfigure"):
    try:
        _sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        _sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import argparse
import json
import locale
import os
import platform
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ──────────────────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parent.parent
DB_PATH     = ROOT / "data" / "otocpa_agent.db"
BACKUP_DIR  = ROOT / "data" / "backups"
CONFIG_PATH = ROOT / "otocpa.config.json"
LOGS_DIR    = ROOT / "logs"
SYS_LOGS    = ROOT / ".otocpa_system" / "logs"
MIGRATE_PY  = ROOT / "scripts" / "migrate_db.py"
DASHBOARD_PY= ROOT / "scripts" / "review_dashboard.py"
DASH_PORT   = 8787
PORTAL_PORT = 8788

# ──────────────────────────────────────────────────────────────────────────────
# Bilingual strings
# ──────────────────────────────────────────────────────────────────────────────
_STRINGS: dict[str, dict[str, str]] = {
    "en": {
        "header":           "OtoCPA — Self-Healing Diagnostics",
        "pass":             "PASS ",
        "fail":             "FAIL ",
        "fixed":            "FIXED",
        "warn":             "WARN ",
        "skip":             "SKIP ",
        # check labels
        "lbl_db":           "1. Database health (integrity & foreign keys)",
        "lbl_cols":         "2. Missing columns",
        "lbl_tables":       "3. Missing tables",
        "lbl_sessions":     "4. Orphaned sessions",
        "lbl_locks":        "5. Locked periods blocking edits",
        "lbl_ports":        "6. Port conflicts (8787, 8788)",
        "lbl_config":       "7. Config file (otocpa.config.json)",
        "lbl_deps":         "8. Python dependencies",
        "lbl_logs":         "9. Recent log errors (last 24 h)",
        "lbl_dashboard":    "10. Dashboard smoke test",
        "lbl_inbox":        "11. Inbox folder (folder watcher)",
        "lbl_cloudflare":   "12. Cloudflare Tunnel service",
        # db
        "db_not_found":     "Database not found at {path}",
        "db_ok":            "Integrity OK — no foreign-key violations",
        "db_corrupted":     "PRAGMA integrity_check failed: {detail}",
        "db_fk_errors":     "{n} foreign-key violation(s) found",
        "db_restore_q":     "Database appears corrupted. Restore from latest backup? [y/N]: ",
        "db_restore_ok":    "Restored from {backup}",
        "db_restore_none":  "No valid backup found in {dir} — manual recovery needed",
        "db_restore_skip":  "Skipped — database may still be corrupted",
        # columns
        "cols_ok":          "All expected columns present",
        "cols_missing":     "{n} column(s) missing — running migrate_db.py automatically...",
        "cols_fixed":       "migrate_db.py completed — all columns added",
        "cols_fail":        "migrate_db.py failed (see output above) — manual intervention needed",
        # tables
        "tables_ok":        "All expected tables present",
        "tables_fixed":     "Recreated {n} missing table(s): {names}",
        "tables_fail":      "Failed to recreate table(s): {names}",
        # sessions
        "sess_ok":          "No orphaned sessions found",
        "sess_fixed":       "{n} expired session(s) deleted",
        "sess_db_skip":     "Skipping — database unavailable",
        # locks
        "locks_ok":         "No locked periods found",
        "locks_found":      "{n} locked period(s) found:",
        "locks_row":        "  • {client} {period}  (locked by {user} on {at})",
        "locks_q":          "  Unlock {client} / {period}? [y/N]: ",
        "locks_unlocked":   "  → Unlocked {client} / {period}",
        "locks_kept":       "  → Kept locked",
        # ports
        "ports_ok":         "Ports 8787 and 8788 are free",
        "port_busy":        "Port {port} is in use by PID {pid} ({name})",
        "port_kill_q":      "  Kill PID {pid} ({name}) to free port {port}? [y/N]: ",
        "port_killed":      "  → Killed PID {pid}",
        "port_kill_fail":   "  → Could not kill PID {pid} — kill it manually",
        "port_kept":        "  → Left running",
        # config
        "cfg_ok":           "Config file valid",
        "cfg_missing":      "Config missing — regenerating with safe defaults...",
        "cfg_fixed":        "Config regenerated at {path}",
        "cfg_bad_json":     "JSON syntax error: {err}",
        "cfg_repaired":     "Bad config backed up to {bak} and regenerated",
        # deps
        "deps_ok":          "All required packages installed",
        "deps_missing":     "{n} package(s) missing: {pkgs}",
        "deps_installing":  "Running pip install {pkgs} ...",
        "deps_fixed":       "All missing packages installed",
        "deps_fail":        "pip install failed — run manually: pip install {pkgs}",
        # logs
        "logs_ok":          "No ERROR lines in the last 24 h",
        "logs_none":        "No log files found (nothing to check)",
        "logs_errors":      "{n} ERROR line(s) in the last 24 h:",
        "logs_from":        "  From {file}:",
        "logs_line":        "    {line}",
        # dashboard
        "dash_already":     "Dashboard already running — testing directly",
        "dash_starting":    "Starting dashboard on port {port} for smoke test...",
        "dash_ok":          "Dashboard responded HTTP {code} ✓",
        "dash_fail":        "Dashboard did not respond (code={code}) — check logs",
        "dash_nostart":     "Could not start dashboard subprocess — check Python path",
        "dash_timeout":     "Dashboard did not start within {sec}s — check for startup errors",
        # inbox folder
        "inbox_skip":       "Folder watcher not configured in otocpa.config.json — skipping",
        "inbox_ok":         "Inbox folder exists and is writable: {path}",
        "inbox_created":    "Inbox folder created: {path}",
        "inbox_not_writable": "Inbox folder exists but is not writable: {path}",
        "inbox_create_fail":  "Could not create inbox folder {path}: {err}",
        # cloudflare
        "cf_skip":            "cloudflared not installed — run scripts/setup_cloudflare.py to configure",
        "cf_running":         "Cloudflare Tunnel running — public URL: {url}",
        "cf_stopped":         "Cloudflare Tunnel service is stopped — attempting restart...",
        "cf_restarted":       "Cloudflare Tunnel restarted — public URL: {url}",
        "cf_restart_fail":    "Could not restart Cloudflare Tunnel — run: sc start cloudflared",
        "cf_no_url":          "Tunnel is running but public_portal_url not set in otocpa.config.json",
        # license
        "lbl_license":        "13. License check",
        "lic_ok":             "License valid — tier: {tier}, expires: {expiry}",
        "lic_none":           "No license installed — run: python scripts/generate_license.py",
        "lic_invalid":        "License invalid: {error}",
        "lic_expired":        "License EXPIRED on {expiry}",
        "lbl_version":        "14. Version check",
        "ver_ok":             "Up to date — version {version}",
        "ver_update":         "Update available: {installed} → {remote}",
        "ver_error":          "Could not check for updates: {error}",
        "ver_no_file":        "version.json not found",
        # summary
        "summary_ok":       "System healthy",
        "summary_issues":   "{n} issue(s) need manual attention:",
        "summary_item":     "  • {item}",
        "divider":          "─" * 72,
    },
    "fr": {
        "header":           "OtoCPA — Diagnostics auto-réparation",
        "pass":             "OK   ",
        "fail":             "ÉCHEC",
        "fixed":            "RÉPRD",
        "warn":             "AVERT",
        "skip":             "IGNR ",
        # check labels
        "lbl_db":           "1. Santé de la base de données (intégrité et clés étrangères)",
        "lbl_cols":         "2. Colonnes manquantes",
        "lbl_tables":       "3. Tables manquantes",
        "lbl_sessions":     "4. Sessions orphelines",
        "lbl_locks":        "5. Périodes verrouillées bloquant les modifications",
        "lbl_ports":        "6. Conflits de port (8787, 8788)",
        "lbl_config":       "7. Fichier de configuration (otocpa.config.json)",
        "lbl_deps":         "8. Dépendances Python",
        "lbl_logs":         "9. Erreurs récentes dans les journaux (dernières 24 h)",
        "lbl_dashboard":    "10. Test de démarrage du tableau de bord",
        "lbl_inbox":        "11. Dossier de réception (surveillance de dossier)",
        "lbl_cloudflare":   "12. Service Cloudflare Tunnel",
        # db
        "db_not_found":     "Base de données introuvable : {path}",
        "db_ok":            "Intégrité OK — aucune violation de clé étrangère",
        "db_corrupted":     "PRAGMA integrity_check a échoué : {detail}",
        "db_fk_errors":     "{n} violation(s) de clé étrangère trouvée(s)",
        "db_restore_q":     "La base de données semble corrompue. Restaurer depuis la dernière sauvegarde? [o/N]: ",
        "db_restore_ok":    "Restaurée depuis {backup}",
        "db_restore_none":  "Aucune sauvegarde valide dans {dir} — récupération manuelle nécessaire",
        "db_restore_skip":  "Ignoré — la base de données pourrait rester corrompue",
        # columns
        "cols_ok":          "Toutes les colonnes attendues sont présentes",
        "cols_missing":     "{n} colonne(s) manquante(s) — exécution automatique de migrate_db.py...",
        "cols_fixed":       "migrate_db.py exécuté — toutes les colonnes ajoutées",
        "cols_fail":        "migrate_db.py a échoué (voir sortie ci-dessus) — intervention manuelle requise",
        # tables
        "tables_ok":        "Toutes les tables attendues sont présentes",
        "tables_fixed":     "{n} table(s) manquante(s) recréée(s) : {names}",
        "tables_fail":      "Échec de la recréation des tables : {names}",
        # sessions
        "sess_ok":          "Aucune session orpheline trouvée",
        "sess_fixed":       "{n} session(s) expirée(s) supprimée(s)",
        "sess_db_skip":     "Ignoré — base de données indisponible",
        # locks
        "locks_ok":         "Aucune période verrouillée",
        "locks_found":      "{n} période(s) verrouillée(s) :",
        "locks_row":        "  • {client} {period}  (verrouillé par {user} le {at})",
        "locks_q":          "  Déverrouiller {client} / {period}? [o/N]: ",
        "locks_unlocked":   "  → {client} / {period} déverrouillé",
        "locks_kept":       "  → Maintenu verrouillé",
        # ports
        "ports_ok":         "Les ports 8787 et 8788 sont disponibles",
        "port_busy":        "Le port {port} est utilisé par le PID {pid} ({name})",
        "port_kill_q":      "  Terminer le PID {pid} ({name}) pour libérer le port {port}? [o/N]: ",
        "port_killed":      "  → PID {pid} terminé",
        "port_kill_fail":   "  → Impossible de terminer le PID {pid} — arrêtez-le manuellement",
        "port_kept":        "  → Laissé actif",
        # config
        "cfg_ok":           "Fichier de configuration valide",
        "cfg_missing":      "Configuration manquante — régénération avec valeurs par défaut...",
        "cfg_fixed":        "Configuration régénérée : {path}",
        "cfg_bad_json":     "Erreur de syntaxe JSON : {err}",
        "cfg_repaired":     "Mauvaise configuration sauvegardée sous {bak} et régénérée",
        # deps
        "deps_ok":          "Tous les paquets requis sont installés",
        "deps_missing":     "{n} paquet(s) manquant(s) : {pkgs}",
        "deps_installing":  "Exécution de pip install {pkgs} ...",
        "deps_fixed":       "Tous les paquets manquants installés",
        "deps_fail":        "pip install a échoué — exécutez : pip install {pkgs}",
        # logs
        "logs_ok":          "Aucune ligne ERROR dans les dernières 24 h",
        "logs_none":        "Aucun fichier journal trouvé (rien à vérifier)",
        "logs_errors":      "{n} ligne(s) ERROR dans les dernières 24 h :",
        "logs_from":        "  Depuis {file} :",
        "logs_line":        "    {line}",
        # dashboard
        "dash_already":     "Tableau de bord déjà actif — test direct",
        "dash_starting":    "Démarrage du tableau de bord sur le port {port} pour le test...",
        "dash_ok":          "Tableau de bord répondu HTTP {code} ✓",
        "dash_fail":        "Tableau de bord n'a pas répondu (code={code}) — vérifiez les journaux",
        "dash_nostart":     "Impossible de démarrer le sous-processus — vérifiez Python",
        "dash_timeout":     "Le tableau de bord n'a pas démarré en {sec}s — vérifiez les erreurs",
        # inbox folder
        "inbox_skip":       "Surveillance de dossier non configurée — ignoré",
        "inbox_ok":         "Dossier de réception accessible en écriture : {path}",
        "inbox_created":    "Dossier de réception créé : {path}",
        "inbox_not_writable": "Dossier de réception non accessible en écriture : {path}",
        "inbox_create_fail":  "Impossible de créer le dossier {path} : {err}",
        # cloudflare
        "cf_skip":            "cloudflared non installé — exécutez scripts/setup_cloudflare.py pour configurer",
        "cf_running":         "Tunnel Cloudflare actif — URL publique : {url}",
        "cf_stopped":         "Le service Cloudflare Tunnel est arrêté — tentative de redémarrage...",
        "cf_restarted":       "Tunnel Cloudflare redémarré — URL publique : {url}",
        "cf_restart_fail":    "Impossible de redémarrer le tunnel — exécutez : sc start cloudflared",
        "cf_no_url":          "Tunnel actif mais public_portal_url absent de otocpa.config.json",
        # license
        "lbl_license":        "13. Vérification de la licence",
        "lic_ok":             "Licence valide — forfait : {tier}, expire le : {expiry}",
        "lic_none":           "Aucune licence installée — exécutez : python scripts/generate_license.py",
        "lic_invalid":        "Licence invalide : {error}",
        "lic_expired":        "Licence EXPIRÉE le {expiry}",
        "lbl_version":        "14. Vérification de version",
        "ver_ok":             "À jour — version {version}",
        "ver_update":         "Mise à jour disponible : {installed} → {remote}",
        "ver_error":          "Impossible de vérifier les mises à jour : {error}",
        "ver_no_file":        "version.json introuvable",
        # summary
        "summary_ok":       "Système en bonne santé",
        "summary_issues":   "{n} problème(s) nécessitent une intervention manuelle :",
        "summary_item":     "  • {item}",
        "divider":          "─" * 72,
    },
}

# ──────────────────────────────────────────────────────────────────────────────
# Language detection & translation helper
# ──────────────────────────────────────────────────────────────────────────────
_LANG: str = "en"


def _detect_lang() -> str:
    """Return 'fr' if the Windows locale is French, otherwise 'en'."""
    try:
        loc = locale.getdefaultlocale()[0] or ""
        if loc.lower().startswith("fr"):
            return "fr"
    except Exception:
        pass
    # On Windows, also check LANG/LC_ALL env vars
    for env in ("LANG", "LC_ALL", "LC_MESSAGES"):
        val = os.environ.get(env, "")
        if val.lower().startswith("fr"):
            return "fr"
    return "en"


def t(key: str, **kw: object) -> str:
    s = _STRINGS[_LANG].get(key, _STRINGS["en"].get(key, key))
    if kw:
        s = s.format(**kw)
    return s


def _is_yes(answer: str) -> bool:
    a = answer.strip().lower()
    return a in ("y", "yes", "o", "oui")


# ──────────────────────────────────────────────────────────────────────────────
# Terminal colors (Windows-safe via ANSI where supported)
# ──────────────────────────────────────────────────────────────────────────────
def _enable_ansi() -> bool:
    """Enable ANSI escape codes on Windows 10+ terminals."""
    if platform.system() == "Windows":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
            return True
        except Exception:
            return False
    return sys.stdout.isatty()


_USE_COLOR = False

_C = {
    "green":  "\033[92m",
    "red":    "\033[91m",
    "yellow": "\033[93m",
    "cyan":   "\033[96m",
    "bold":   "\033[1m",
    "reset":  "\033[0m",
}


def _color(text: str, code: str) -> str:
    if not _USE_COLOR:
        return text
    return _C.get(code, "") + text + _C["reset"]


STATUS_COLORS = {
    "pass":  "green",
    "fixed": "cyan",
    "fail":  "red",
    "warn":  "yellow",
    "skip":  "yellow",
}

# ──────────────────────────────────────────────────────────────────────────────
# Result tracking
# ──────────────────────────────────────────────────────────────────────────────
_results: list[tuple[str, str, str]] = []  # (label, status_key, detail)
_manual_items: list[str] = []


def _record(label: str, status_key: str, detail: str = "", manual: bool = False) -> None:
    _results.append((label, status_key, detail))
    tag = t(status_key)
    colored_tag = _color(f"[{tag}]", STATUS_COLORS.get(status_key, "reset"))
    print(f"  {colored_tag}  {label}")
    if detail:
        for line in detail.splitlines():
            print(f"           {line}")
    if manual and detail:
        _manual_items.append(f"{label}: {detail.splitlines()[0]}")
    elif manual:
        _manual_items.append(label)


# ──────────────────────────────────────────────────────────────────────────────
# Windows port → PID helper
# ──────────────────────────────────────────────────────────────────────────────
def _port_to_pid(port: int) -> int | None:
    """Return the PID listening on *port*, or None if free / unknown."""
    # Try psutil first (fast and reliable)
    try:
        import psutil  # type: ignore[import]
        for conn in psutil.net_connections(kind="inet"):
            if conn.laddr.port == port and conn.status in ("LISTEN", "ESTABLISHED"):
                return conn.pid
        return None
    except Exception:
        pass

    # Fallback: parse netstat on Windows
    if platform.system() == "Windows":
        try:
            out = subprocess.check_output(
                ["netstat", "-ano"],
                stderr=subprocess.DEVNULL,
                text=True,
            )
            for line in out.splitlines():
                if f":{port} " in line and ("LISTENING" in line or "ESTABLISHED" in line):
                    parts = line.split()
                    if parts:
                        try:
                            return int(parts[-1])
                        except ValueError:
                            pass
        except Exception:
            pass
    else:
        # Linux/macOS
        try:
            out = subprocess.check_output(
                ["ss", "-tlnp", f"sport = :{port}"],
                stderr=subprocess.DEVNULL,
                text=True,
            )
            m = re.search(r"pid=(\d+)", out)
            if m:
                return int(m.group(1))
        except Exception:
            pass
    return None


def _pid_name(pid: int) -> str:
    try:
        import psutil  # type: ignore[import]
        return psutil.Process(pid).name()
    except Exception:
        pass
    if platform.system() == "Windows":
        try:
            out = subprocess.check_output(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                stderr=subprocess.DEVNULL,
                text=True,
            )
            parts = out.strip().split(",")
            if parts:
                return parts[0].strip('"')
        except Exception:
            pass
    return "unknown"


def _kill_pid(pid: int) -> bool:
    try:
        if platform.system() == "Windows":
            result = subprocess.call(
                ["taskkill", "/F", "/PID", str(pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return result == 0
        else:
            import signal as _signal
            os.kill(pid, _signal.SIGTERM)
            return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Check 1 — Database health
# ──────────────────────────────────────────────────────────────────────────────
def check_db_health() -> bool:
    """Returns True if the DB is usable after this check."""
    label = t("lbl_db")

    if not DB_PATH.exists():
        _record(label, "fail", t("db_not_found", path=DB_PATH), manual=True)
        return False

    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row

        # integrity_check
        rows = conn.execute("PRAGMA integrity_check").fetchall()
        integrity_ok = len(rows) == 1 and rows[0][0] == "ok"

        # foreign_key_check
        fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
        conn.close()

        if integrity_ok and not fk_rows:
            _record(label, "pass", t("db_ok"))
            return True

        issues: list[str] = []
        if not integrity_ok:
            detail = "; ".join(r[0] for r in rows[:5])
            issues.append(t("db_corrupted", detail=detail))
        if fk_rows:
            issues.append(t("db_fk_errors", n=len(fk_rows)))

        combined = " | ".join(issues)
        # Offer restore only for real corruption (not just FK violations)
        if not integrity_ok and BACKUP_DIR.exists():
            print(f"\n  {'─'*60}")
            print(f"  ⚠  {combined}")
            ans = input(f"  {t('db_restore_q')}").strip()
            if _is_yes(ans):
                # Find most recent backup
                backups = sorted(
                    BACKUP_DIR.glob("*.db"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
                if backups:
                    best = backups[0]
                    shutil.copy2(str(best), str(DB_PATH))
                    _record(label, "fixed", t("db_restore_ok", backup=best.name))
                    return True
                else:
                    _record(label, "fail", t("db_restore_none", dir=BACKUP_DIR), manual=True)
                    return False
            else:
                _record(label, "fail", t("db_restore_skip") + " | " + combined, manual=True)
                return False
        else:
            # FK violations only — warn but don't block
            _record(label, "warn", combined)
            return True

    except Exception as exc:
        _record(label, "fail", str(exc), manual=True)
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Check 2 — Missing columns
# ──────────────────────────────────────────────────────────────────────────────
# Sentinel columns: one per table that was added in a recent migration.
# If any are absent we know migrate_db.py needs to run.
_SENTINEL_COLS: list[tuple[str, str]] = [
    ("dashboard_users",    "must_reset_password"),
    ("dashboard_sessions", "last_seen_at"),
    ("documents",          "fraud_flags"),
    ("posting_jobs",       "error_text"),
    ("vendor_memory",      "vendor_key"),
    ("learning_memory",    "memory_key"),
    ("period_close",       "responsible_user"),
    ("period_close_locks", "locked_by"),
    ("time_entries",       "billable"),
    ("client_config",      "filing_frequency"),
    ("audit_log",          "event_type"),
    ("client_communications", "direction"),
]


def check_missing_columns() -> None:
    label = t("lbl_cols")
    if not DB_PATH.exists():
        _record(label, "skip", t("sess_db_skip"))
        return

    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        missing: list[str] = []
        for table, col in _SENTINEL_COLS:
            # Only check tables that actually exist
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if not exists:
                continue
            cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if col not in cols:
                missing.append(f"{table}.{col}")
        conn.close()

        if not missing:
            _record(label, "pass", t("cols_ok"))
            return

        print(f"\n           {t('cols_missing', n=len(missing))}")
        for m in missing:
            print(f"           • {m}")

        ret = subprocess.run(
            [sys.executable, str(MIGRATE_PY)],
            capture_output=False,
            text=True,
        )
        if ret.returncode == 0:
            _record(label, "fixed", t("cols_fixed"))
        else:
            _record(label, "fail", t("cols_fail"), manual=True)

    except Exception as exc:
        _record(label, "fail", str(exc), manual=True)


# ──────────────────────────────────────────────────────────────────────────────
# Check 3 — Missing tables
# ──────────────────────────────────────────────────────────────────────────────
_TABLE_DDLS: dict[str, str] = {
    "period_close": """
        CREATE TABLE IF NOT EXISTS period_close (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code      TEXT NOT NULL,
            period           TEXT NOT NULL,
            checklist_item   TEXT NOT NULL,
            status           TEXT NOT NULL DEFAULT 'open',
            responsible_user TEXT,
            due_date         TEXT,
            completed_by     TEXT,
            completed_at     TEXT,
            notes            TEXT,
            UNIQUE(client_code, period, checklist_item)
        )""",
    "period_close_locks": """
        CREATE TABLE IF NOT EXISTS period_close_locks (
            client_code TEXT NOT NULL,
            period      TEXT NOT NULL,
            locked_by   TEXT,
            locked_at   TEXT,
            PRIMARY KEY (client_code, period)
        )""",
    "time_entries": """
        CREATE TABLE IF NOT EXISTS time_entries (
            entry_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            username         TEXT NOT NULL,
            client_code      TEXT NOT NULL,
            document_id      TEXT,
            started_at       TEXT NOT NULL,
            ended_at         TEXT,
            duration_minutes REAL,
            description      TEXT,
            billable         INTEGER NOT NULL DEFAULT 1,
            hourly_rate      REAL
        )""",
    "invoices": """
        CREATE TABLE IF NOT EXISTS invoices (
            invoice_id   TEXT PRIMARY KEY,
            client_code  TEXT NOT NULL,
            period_start TEXT NOT NULL,
            period_end   TEXT NOT NULL,
            generated_by TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            hourly_rate  REAL NOT NULL,
            subtotal     REAL NOT NULL,
            gst_amount   REAL NOT NULL,
            qst_amount   REAL NOT NULL,
            total_amount REAL NOT NULL,
            entry_count  INTEGER NOT NULL DEFAULT 0
        )""",
    "client_config": """
        CREATE TABLE IF NOT EXISTS client_config (
            client_code             TEXT PRIMARY KEY,
            quick_method            INTEGER NOT NULL DEFAULT 0,
            quick_method_type       TEXT    NOT NULL DEFAULT 'retail',
            updated_at              TEXT,
            filing_frequency        TEXT    NOT NULL DEFAULT 'monthly',
            gst_registration_number TEXT,
            qst_registration_number TEXT,
            fiscal_year_end         TEXT    NOT NULL DEFAULT '12-31'
        )""",
    "gst_filings": """
        CREATE TABLE IF NOT EXISTS gst_filings (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code  TEXT NOT NULL,
            period_label TEXT NOT NULL,
            deadline     TEXT NOT NULL,
            filed_at     TEXT,
            filed_by     TEXT,
            UNIQUE(client_code, period_label)
        )""",
    "audit_log": """
        CREATE TABLE IF NOT EXISTS audit_log (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type     TEXT    NOT NULL DEFAULT 'ai_call',
            username       TEXT,
            document_id    TEXT,
            provider       TEXT,
            task_type      TEXT,
            prompt_snippet TEXT,
            latency_ms     INTEGER,
            created_at     TEXT    NOT NULL DEFAULT ''
        )""",
    "bank_statements": """
        CREATE TABLE IF NOT EXISTS bank_statements (
            statement_id      TEXT PRIMARY KEY,
            bank_name         TEXT,
            file_name         TEXT,
            client_code       TEXT,
            imported_by       TEXT,
            imported_at       TEXT,
            period_start      TEXT,
            period_end        TEXT,
            transaction_count INTEGER DEFAULT 0,
            matched_count     INTEGER DEFAULT 0,
            unmatched_count   INTEGER DEFAULT 0
        )""",
    "bank_transactions": """
        CREATE TABLE IF NOT EXISTS bank_transactions (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            statement_id        TEXT NOT NULL,
            document_id         TEXT NOT NULL,
            txn_date            TEXT,
            description         TEXT,
            debit               REAL,
            credit              REAL,
            balance             REAL,
            matched_document_id TEXT,
            match_confidence    REAL,
            match_reason        TEXT
        )""",
    "client_communications": """
        CREATE TABLE IF NOT EXISTS client_communications (
            comm_id     TEXT PRIMARY KEY,
            document_id TEXT NOT NULL,
            client_code TEXT NOT NULL,
            direction   TEXT NOT NULL DEFAULT 'outbound',
            message     TEXT NOT NULL,
            sent_at     TEXT,
            sent_by     TEXT,
            read_at     TEXT
        )""",
}


def check_missing_tables() -> None:
    label = t("lbl_tables")
    if not DB_PATH.exists():
        _record(label, "skip", t("sess_db_skip"))
        return

    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        existing = {
            r["name"]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        missing = [tbl for tbl in _TABLE_DDLS if tbl not in existing]

        if not missing:
            conn.close()
            _record(label, "pass", t("tables_ok"))
            return

        failed: list[str] = []
        created: list[str] = []
        for tbl in missing:
            try:
                conn.execute(_TABLE_DDLS[tbl])
                created.append(tbl)
            except Exception as exc:
                failed.append(f"{tbl} ({exc})")
        conn.commit()
        conn.close()

        if created and not failed:
            _record(label, "fixed", t("tables_fixed", n=len(created), names=", ".join(created)))
        elif failed:
            _record(
                label, "fail",
                t("tables_fail", names=", ".join(failed)),
                manual=True,
            )

    except Exception as exc:
        _record(label, "fail", str(exc), manual=True)


# ──────────────────────────────────────────────────────────────────────────────
# Check 4 — Orphaned sessions
# ──────────────────────────────────────────────────────────────────────────────
def check_orphaned_sessions() -> None:
    label = t("lbl_sessions")
    if not DB_PATH.exists():
        _record(label, "skip", t("sess_db_skip"))
        return

    try:
        conn = sqlite3.connect(str(DB_PATH))
        # Confirm table exists
        tbl = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='dashboard_sessions'"
        ).fetchone()
        if not tbl:
            conn.close()
            _record(label, "skip", t("sess_db_skip"))
            return

        now_iso = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "DELETE FROM dashboard_sessions WHERE expires_at < ?", (now_iso,)
        )
        deleted = cur.rowcount
        conn.commit()
        conn.close()

        if deleted == 0:
            _record(label, "pass", t("sess_ok"))
        else:
            _record(label, "fixed", t("sess_fixed", n=deleted))

    except Exception as exc:
        _record(label, "fail", str(exc), manual=True)


# ──────────────────────────────────────────────────────────────────────────────
# Check 5 — Locked periods
# ──────────────────────────────────────────────────────────────────────────────
def check_locked_periods() -> None:
    label = t("lbl_locks")
    if not DB_PATH.exists():
        _record(label, "skip", t("sess_db_skip"))
        return

    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        tbl = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='period_close_locks'"
        ).fetchone()
        if not tbl:
            conn.close()
            _record(label, "pass", t("locks_ok"))
            return

        locks = conn.execute(
            "SELECT client_code, period, locked_by, locked_at FROM period_close_locks"
        ).fetchall()

        if not locks:
            conn.close()
            _record(label, "pass", t("locks_ok"))
            return

        print(f"\n           {t('locks_found', n=len(locks))}")
        unlocked = 0
        for row in locks:
            client = row["client_code"] or "?"
            period = row["period"] or "?"
            user   = row["locked_by"] or "system"
            at     = (row["locked_at"] or "")[:10]
            print(t("locks_row", client=client, period=period, user=user, at=at))
            ans = input(t("locks_q", client=client, period=period)).strip()
            if _is_yes(ans):
                conn.execute(
                    "DELETE FROM period_close_locks WHERE client_code=? AND period=?",
                    (row["client_code"], row["period"]),
                )
                conn.commit()
                print(t("locks_unlocked", client=client, period=period))
                unlocked += 1
            else:
                print(t("locks_kept"))

        conn.close()
        remaining = len(locks) - unlocked
        if remaining == 0:
            _record(label, "fixed", f"All {unlocked} lock(s) removed")
        elif unlocked > 0:
            _record(label, "warn", f"{unlocked} unlocked, {remaining} still locked")
        else:
            _record(label, "warn", f"{remaining} lock(s) kept — edits blocked for those periods")

    except Exception as exc:
        _record(label, "fail", str(exc), manual=True)


# ──────────────────────────────────────────────────────────────────────────────
# Check 6 — Port conflicts
# ──────────────────────────────────────────────────────────────────────────────
def _port_in_use(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except (OSError, ConnectionRefusedError):
        return False


def check_port_conflicts() -> None:
    label = t("lbl_ports")
    conflicts: list[tuple[int, int, str]] = []  # (port, pid, name)

    for port in (DASH_PORT, PORTAL_PORT):
        if not _port_in_use(port):
            continue
        pid = _port_to_pid(port)
        name = _pid_name(pid) if pid else "unknown"
        conflicts.append((port, pid or 0, name))

    if not conflicts:
        _record(label, "pass", t("ports_ok"))
        return

    print()
    resolved = 0
    for port, pid, name in conflicts:
        print(f"  ⚠  {t('port_busy', port=port, pid=pid, name=name)}")
        if pid:
            ans = input(t("port_kill_q", pid=pid, name=name, port=port)).strip()
            if _is_yes(ans):
                if _kill_pid(pid):
                    print(t("port_killed", pid=pid))
                    resolved += 1
                else:
                    print(t("port_kill_fail", pid=pid))
            else:
                print(t("port_kept"))

    still_busy = [
        t("port_busy", port=p, pid=pid, name=name)
        for p, pid, name in conflicts
        if _port_in_use(p)
    ]

    if not still_busy:
        _record(label, "fixed", f"{resolved} conflict(s) resolved")
    else:
        _record(
            label, "warn",
            "\n".join(still_busy),
            manual=len(still_busy) > 0,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Check 7 — Config file
# ──────────────────────────────────────────────────────────────────────────────
_DEFAULT_CONFIG: dict = {
    "host": "127.0.0.1",
    "port": 8787,
    "session_hours": 12,
    "ai_router": {
        "routine_provider": {
            "base_url": "https://api.deepseek.com/v1",
            "api_key": "",
            "model": "deepseek-chat",
        },
        "premium_provider": {
            "base_url": "https://openrouter.ai/api/v1",
            "api_key": "",
            "model": "anthropic/claude-sonnet-4-6",
        },
        "routine_tasks": ["classify_document", "extract_vendor", "suggest_gl"],
        "complex_tasks": [
            "explain_anomaly",
            "escalation_decision",
            "compliance_narrative",
            "working_paper",
        ],
    },
    "digest": {
        "enabled": False,
        "smtp_host": "smtp.gmail.com",
        "smtp_port": 587,
        "smtp_user": "yourfirm@gmail.com",
        "smtp_password": "your-app-password",
        "from_address": "otocpa@yourfirm.com",
        "from_name": "OtoCPA",
    },
    "security": {
        "bcrypt_rounds": 12,
        "audit_log": True,
        "api_key_encryption": True,
    },
    "client_portal": {
        "enabled": True,
        "port": 8788,
        "max_upload_mb": 20,
    },
    "ingest": {
        "port": 8789,
        "api_key": "",
    },
}


def check_config() -> None:
    label = t("lbl_config")

    if not CONFIG_PATH.exists():
        print(f"\n           {t('cfg_missing')}")
        CONFIG_PATH.write_text(
            json.dumps(_DEFAULT_CONFIG, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        _record(label, "fixed", t("cfg_fixed", path=CONFIG_PATH))
        return

    try:
        json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        _record(label, "pass", t("cfg_ok"))
    except json.JSONDecodeError as exc:
        bak = CONFIG_PATH.with_suffix(".json.bak")
        shutil.copy2(str(CONFIG_PATH), str(bak))
        CONFIG_PATH.write_text(
            json.dumps(_DEFAULT_CONFIG, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        _record(
            label, "fixed",
            t("cfg_bad_json", err=exc) + "\n" + t("cfg_repaired", bak=bak.name),
        )


# ──────────────────────────────────────────────────────────────────────────────
# Check 8 — Python dependencies
# ──────────────────────────────────────────────────────────────────────────────
_REQUIRED_PACKAGES: list[tuple[str, str]] = [
    # (import_name, pip_package_name)
    ("bcrypt",       "bcrypt"),
    ("pdfplumber",   "pdfplumber"),
    ("PIL",          "Pillow"),
    ("requests",     "requests"),
    ("reportlab",    "reportlab"),
]

# Optional — warn only, don't auto-install
_OPTIONAL_PACKAGES: list[tuple[str, str]] = [
    ("fitz",     "PyMuPDF"),
    ("psutil",   "psutil"),
    ("watchdog", "watchdog"),
]


def _importable(name: str) -> bool:
    import importlib.util
    return importlib.util.find_spec(name) is not None


def check_dependencies() -> None:
    label = t("lbl_deps")

    missing_req = [
        pip for imp, pip in _REQUIRED_PACKAGES if not _importable(imp)
    ]
    missing_opt = [
        pip for imp, pip in _OPTIONAL_PACKAGES if not _importable(imp)
    ]

    if not missing_req and not missing_opt:
        _record(label, "pass", t("deps_ok"))
        return

    if missing_req:
        pkgs_str = " ".join(missing_req)
        print(f"\n           {t('deps_missing', n=len(missing_req), pkgs=pkgs_str)}")
        print(f"           {t('deps_installing', pkgs=pkgs_str)}")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--quiet"] + missing_req,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            _record(label, "fixed", t("deps_fixed"))
        else:
            _record(
                label, "fail",
                t("deps_fail", pkgs=pkgs_str),
                manual=True,
            )
    elif missing_opt:
        _record(
            label, "warn",
            f"Optional packages not installed: {', '.join(missing_opt)} "
            f"(run: pip install {' '.join(missing_opt)})",
        )

    if missing_opt and missing_req:
        print(
            f"           Optional packages not installed: {', '.join(missing_opt)}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Check 9 — Log file errors
# ──────────────────────────────────────────────────────────────────────────────
_ERROR_RE = re.compile(r"\b(ERROR|CRITICAL|FATAL|Traceback)\b", re.IGNORECASE)
_TIMESTAMP_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})"
)

# Plain-language translations of common error patterns
_ERROR_HINTS: dict[str, str] = {
    "OperationalError":  "Database operation failed — check DB health (check 1)",
    "ConnectionRefused": "Could not connect to a service — check port conflicts (check 6)",
    "PermissionError":   "File permission issue — check folder permissions",
    "ImportError":       "Missing Python package — check dependencies (check 8)",
    "KeyError":          "Missing config key — check config file (check 7)",
    "JSONDecodeError":   "Corrupt JSON file — check config file (check 7)",
    "FileNotFoundError": "Missing file or folder — check installation",
}


def _plain_english(line: str) -> str:
    for pattern, hint in _ERROR_HINTS.items():
        if pattern.lower() in line.lower():
            return f"  → {hint}"
    return ""


def _scan_log_file(path: Path, since: datetime) -> list[str]:
    """Return ERROR/CRITICAL lines from the last 24 h in *path*."""
    # Skip files not touched in the last 48 h (wider window to be safe)
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        if mtime < since - timedelta(hours=24):
            return []
    except Exception:
        return []

    hits: list[str] = []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        for line in text.splitlines():
            if not _ERROR_RE.search(line):
                continue
            # Try to parse timestamp and filter
            m = _TIMESTAMP_RE.search(line)
            if m:
                try:
                    ts_str = m.group(1).replace("T", " ")
                    ts = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
                    if ts < since:
                        continue
                except ValueError:
                    pass
            hits.append(line.strip())
    except Exception:
        pass
    return hits[:20]  # cap at 20 lines per file


def check_log_errors() -> None:
    label = t("lbl_logs")
    since = datetime.now(timezone.utc) - timedelta(hours=24)

    log_dirs = [d for d in (LOGS_DIR, SYS_LOGS) if d.exists()]
    if not log_dirs:
        _record(label, "skip", t("logs_none"))
        return

    all_hits: dict[str, list[str]] = {}
    for d in log_dirs:
        for lf in d.glob("*.log"):
            hits = _scan_log_file(lf, since)
            if hits:
                all_hits[lf.name] = hits

    if not all_hits:
        _record(label, "pass", t("logs_ok"))
        return

    total = sum(len(v) for v in all_hits.values())
    print(f"\n           {t('logs_errors', n=total)}")
    shown = 0
    for fname, lines in all_hits.items():
        print(t("logs_from", file=fname))
        for line in lines[:5]:  # show at most 5 per file
            short = line[:110] + ("…" if len(line) > 110 else "")
            print(t("logs_line", line=short))
            hint = _plain_english(line)
            if hint:
                print(f"           {hint}")
            shown += 1
        if len(lines) > 5:
            print(f"           … and {len(lines) - 5} more line(s) in this file")

    _record(label, "warn", t("logs_errors", n=total))


# ──────────────────────────────────────────────────────────────────────────────
# Check 10 — Dashboard smoke test
# ──────────────────────────────────────────────────────────────────────────────
def _http_get_code(url: str, timeout: int = 5) -> int | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "autofix-smoketest/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return None


def check_dashboard() -> None:
    label = t("lbl_dashboard")
    url = f"http://127.0.0.1:{DASH_PORT}/"
    proc: subprocess.Popen | None = None

    already_up = _port_in_use(DASH_PORT)
    if already_up:
        print(f"\n           {t('dash_already')}")
    else:
        if not DASHBOARD_PY.exists():
            _record(label, "fail", t("dash_nostart"), manual=True)
            return
        print(f"\n           {t('dash_starting', port=DASH_PORT)}")
        try:
            proc = subprocess.Popen(
                [sys.executable, str(DASHBOARD_PY)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                cwd=str(ROOT),
            )
        except Exception as exc:
            _record(label, "fail", f"{t('dash_nostart')}: {exc}", manual=True)
            return

        # Wait up to 12 s for the port to open
        deadline = time.time() + 12
        started = False
        while time.time() < deadline:
            if _port_in_use(DASH_PORT):
                started = True
                break
            time.sleep(0.5)

        if not started:
            if proc:
                proc.terminate()
            _record(label, "fail", t("dash_timeout", sec=12), manual=True)
            return

    code = _http_get_code(url, timeout=6)

    if proc:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()

    if code == 200:
        _record(label, "pass", t("dash_ok", code=code))
    elif code is not None:
        _record(label, "warn", t("dash_fail", code=code))
    else:
        _record(label, "fail", t("dash_fail", code="timeout"), manual=True)


# ──────────────────────────────────────────────────────────────────────────────
# Check 11 — Inbox folder (folder watcher)
# ──────────────────────────────────────────────────────────────────────────────
def check_inbox_folder() -> None:
    label = t("lbl_inbox")

    # Read config; skip if folder watcher not configured
    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        cfg = {}

    inbox_str = cfg.get("inbox_folder", "")
    if not inbox_str:
        _record(label, "skip", t("inbox_skip"))
        return

    inbox = Path(inbox_str)

    if inbox.exists():
        # Check writability via a temp file probe
        probe = inbox / ".otocpa_write_test"
        try:
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
            _record(label, "pass", t("inbox_ok", path=inbox))
        except Exception:
            _record(label, "fail", t("inbox_not_writable", path=inbox), manual=True)
        return

    # Folder doesn't exist — try to create it
    try:
        inbox.mkdir(parents=True, exist_ok=True)
        _record(label, "fixed", t("inbox_created", path=inbox))
    except Exception as exc:
        _record(label, "fail", t("inbox_create_fail", path=inbox, err=exc), manual=True)


# ──────────────────────────────────────────────────────────────────────────────
# Check 12 — Cloudflare Tunnel service
# ──────────────────────────────────────────────────────────────────────────────

def _cf_service_running() -> bool:
    """Return True if the cloudflared Windows service is RUNNING."""
    try:
        out = subprocess.check_output(
            ["sc", "query", "cloudflared"],
            stderr=subprocess.DEVNULL, text=True,
        )
        return "RUNNING" in out
    except Exception:
        return False


def _cf_public_url() -> str:
    """Return the public_portal_url from otocpa.config.json, or ''."""
    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return cfg.get("public_portal_url", "")
    except Exception:
        return ""


def check_cloudflare_tunnel() -> None:
    label = t("lbl_cloudflare")

    # Skip if cloudflared binary is not present anywhere
    cf_bin = ROOT / "cloudflare" / "cloudflared.exe"
    if not cf_bin.exists():
        # Also check system PATH
        if not shutil.which("cloudflared"):
            _record(label, "skip", t("cf_skip"))
            return

    running = _cf_service_running()
    public_url = _cf_public_url()

    if running:
        if public_url:
            _record(label, "pass", t("cf_running", url=public_url))
        else:
            _record(label, "warn", t("cf_no_url"))
        return

    # Service is stopped — attempt restart
    print(f"\n           {t('cf_stopped')}")
    try:
        subprocess.run(
            ["sc", "start", "cloudflared"],
            check=True, capture_output=True, timeout=15,
        )
        time.sleep(3)
        if _cf_service_running():
            url = _cf_public_url()
            _record(label, "fixed", t("cf_restarted", url=url or "(URL not configured)"))
        else:
            _record(label, "fail", t("cf_restart_fail"), manual=True)
    except Exception:
        _record(label, "fail", t("cf_restart_fail"), manual=True)


# Check 13 — License
def check_license() -> None:
    label = t("lbl_license")
    try:
        # Import inline to avoid hard dependency
        import sys
        sys.path.insert(0, str(ROOT))
        from src.engines.license_engine import get_license_status
        status = get_license_status()
    except Exception as exc:
        _record(label, "warn", f"Could not import license_engine: {exc}")
        return

    if not status.get("valid"):
        error = status.get("error", "unknown")
        if "expired" in error.lower():
            expiry = status.get("expiry_date", "?")
            _record(label, "fail", t("lic_expired", expiry=expiry), manual=True)
        elif status.get("tier") == "none":
            _record(label, "warn", t("lic_none"))
        else:
            _record(label, "fail", t("lic_invalid", error=error), manual=True)
        return

    _record(label, "pass", t("lic_ok", tier=status["tier"], expiry=status["expiry_date"]))


# Check 14 — Version
def check_version() -> None:
    label = t("lbl_version")
    version_file = ROOT / "version.json"
    if not version_file.exists():
        _record(label, "warn", t("ver_no_file"))
        return

    try:
        ver_data = json.loads(version_file.read_text(encoding="utf-8"))
        installed = ver_data.get("version", "0.0.0")
    except Exception as exc:
        _record(label, "warn", t("ver_error", error=str(exc)))
        return

    # Try to check for updates
    try:
        update_url = None
        if CONFIG_PATH.exists():
            cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            update_url = cfg.get("update_url")
        if not update_url:
            update_url = "https://releases.otocpa.ai/latest/version.json"

        req = urllib.request.Request(update_url, headers={"User-Agent": "OtoCPA-Autofix/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            remote_data = json.loads(resp.read().decode("utf-8"))
        remote_ver = remote_data.get("version", "0.0.0")

        installed_tuple = tuple(int(x) for x in installed.split("."))
        remote_tuple = tuple(int(x) for x in remote_ver.split("."))

        if remote_tuple > installed_tuple:
            _record(label, "warn", t("ver_update", installed=installed, remote=remote_ver))
        else:
            _record(label, "pass", t("ver_ok", version=installed))
    except Exception:
        # Cannot reach update server — just report installed version
        _record(label, "pass", t("ver_ok", version=installed))


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    global _LANG, _USE_COLOR

    parser = argparse.ArgumentParser(
        description="OtoCPA — self-healing diagnostic script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--lang",
        choices=["en", "fr"],
        default=None,
        help="Output language (en/fr). Auto-detected from system locale if omitted.",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable coloured output",
    )
    args = parser.parse_args()

    _LANG = args.lang if args.lang else _detect_lang()
    _USE_COLOR = not args.no_color and _enable_ansi()

    # ── Header ────────────────────────────────────────────────────────────────
    div = t("divider")
    print()
    print(_color(div, "bold"))
    print(_color(f"  {t('header')}", "bold"))
    print(_color(f"  {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}  •  Python {sys.version.split()[0]}", "cyan"))
    print(_color(div, "bold"))
    print()

    # ── Run all checks ────────────────────────────────────────────────────────
    db_ok = check_db_health()
    print()
    check_missing_columns()
    print()
    check_missing_tables()
    print()
    check_orphaned_sessions()
    print()
    check_locked_periods()
    print()
    check_port_conflicts()
    print()
    check_config()
    print()
    check_dependencies()
    print()
    check_log_errors()
    print()
    check_dashboard()
    print()
    check_inbox_folder()
    print()
    check_cloudflare_tunnel()
    print()
    check_license()
    print()
    check_version()

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print(_color(div, "bold"))

    fail_count = sum(1 for _, s, _ in _results if s == "fail")
    warn_count  = sum(1 for _, s, _ in _results if s == "warn")

    if not _manual_items:
        print(_color(f"  ✔  {t('summary_ok')}", "green"))
    else:
        n = len(_manual_items)
        print(_color(f"  ✘  {t('summary_issues', n=n)}", "red"))
        for item in _manual_items:
            print(t("summary_item", item=item))

    if warn_count:
        print(f"  {'─'*40}")
        print(
            _color(
                f"  {warn_count} warning(s) — review log output above for details",
                "yellow",
            )
        )

    print(_color(div, "bold"))
    print()
    sys.exit(1 if _manual_items else 0)


if __name__ == "__main__":
    main()
