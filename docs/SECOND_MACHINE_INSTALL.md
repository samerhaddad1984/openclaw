# Installing OtoCPA on a Second Machine

This guide walks you through setting up OtoCPA on a new Windows or Mac
workstation.  It assumes you already have a working OtoCPA instance on
another machine.

---

## Quick Start (automated)

Copy the entire OtoCPA folder to the new machine (USB, network share, or
zip), then run:

```bash
# Windows
python scripts/install_second_machine.py --config "E:\otocpa.config.json"

# Mac
python3 scripts/install_second_machine.py --config "/Volumes/USB/otocpa.config.json"
```

The script handles dependency installation, database setup, service
registration, and opens the dashboard in your browser.

---

## Windows — Step by Step

### 1. Install Python 3.11+

Download from <https://www.python.org/downloads/> and run the installer.
**Check "Add Python to PATH"** during installation.

Verify:

```cmd
python --version
```

### 2. Install dependencies

```cmd
cd C:\path\to\OtoCPA
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 3. Run database migration

```cmd
python scripts/migrate_db.py
```

### 4. Copy your configuration

Copy `otocpa.config.json` from your first machine (USB, network share, or
email) into the OtoCPA root directory:

```cmd
copy "E:\otocpa.config.json" "C:\path\to\OtoCPA\otocpa.config.json"
```

### 5. Register as a Windows Service

```cmd
python installer/service_wrapper.py install
```

### 6. Start the service

```cmd
python installer/service_wrapper.py start
```

### 7. Open the dashboard

Navigate to <http://127.0.0.1:8787/> in your browser.

---

## macOS — Step by Step

### 1. Install Python 3.11+ via Homebrew

```bash
brew install python@3.11
python3 --version
```

### 2. Install dependencies

```bash
cd /path/to/OtoCPA
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

### 3. Run database migration

```bash
python3 scripts/migrate_db.py
```

### 4. Copy your configuration

```bash
cp /Volumes/USB/otocpa.config.json ./otocpa.config.json
```

### 5. Create a launchd plist for auto-start

Create `~/Library/LaunchAgents/com.otocpa.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.otocpa</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/python3</string>
        <string>/path/to/OtoCPA/scripts/review_dashboard.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/path/to/OtoCPA</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
</dict>
</plist>
```

### 6. Load the plist

```bash
launchctl load ~/Library/LaunchAgents/com.otocpa.plist
```

### 7. Open the dashboard

Navigate to <http://127.0.0.1:8787/> in your browser.

---

## Sharing the Database Across Two Machines

### Option A — Shared network drive (simple)

Place the `.db` file on a network share that both machines can access, then
edit `otocpa.config.json` on **both** machines:

```json
{
  "database_path": "\\\\SERVER\\share\\otocpa_agent.db"
}
```

> **Note:** SQLite supports only one writer at a time.  This works well when
> only one machine is actively processing documents at once.

### Option B — One server, browser-only clients (recommended)

Run OtoCPA on one machine (the "server") and have the second machine
connect via its browser — no installation required on the client.

1. On the server machine, ensure `otocpa.config.json` has:

   ```json
   {
     "host": "0.0.0.0",
     "port": 8787
   }
   ```

2. Find the server's local IP (e.g. `192.168.1.50`).

3. On the second machine, open a browser to:

   ```
   http://192.168.1.50:8787/
   ```

Both users share the same database with no sync issues.

### Option C — Separate databases with manual sync

Each machine has its own database.  Use OtoCPA's backup/restore to sync:

1. On Machine A: **Settings > Backup > Export**
2. Copy the backup file to Machine B
3. On Machine B: **Settings > Backup > Import**

---

## Transferring Your License to a New Machine

1. On the **old** machine, go to **Settings > License** and click
   **Deactivate**.
2. Copy `otocpa.config.json` to the new machine (it contains your license
   key).
3. On the **new** machine, open OtoCPA.  It will re-activate the license
   automatically against the license server.

If you cannot deactivate on the old machine (e.g. it is broken), contact
support — licenses can be reset server-side.

---

## Copying Your Config from Old Machine to New Machine

The file `otocpa.config.json` in the OtoCPA root directory contains
all configuration: AI provider keys, SMTP settings, firm name, license,
network settings, and the database path.

1. On the old machine, locate `otocpa.config.json`.
2. Copy it to the new machine's OtoCPA root directory via USB, network
   share, or any file-transfer method.
3. **Review and update** any machine-specific paths (e.g. `database_path` if
   it references a local directory that differs on the new machine).

That's it — OtoCPA reads all settings from this single file on startup.
