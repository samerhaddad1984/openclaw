# OtoCPA --- System Administrator Guide

**Version:** 1.0
**Last updated:** 2026-03-25
**Support:** support@otocpa.com

---

## Table of Contents

1. [System Requirements](#section-1--system-requirements)
2. [Fresh Windows Installation](#section-2--fresh-windows-installation)
3. [Fresh Mac Installation](#section-3--fresh-mac-installation)
4. [License Management](#section-4--license-management)
5. [Remote Support Procedures](#section-5--remote-support-procedures)
6. [Backup and Restore](#section-6--backup-and-restore)
7. [Updates and Upgrades](#section-7--updates-and-upgrades)
8. [Security Management](#section-8--security-management)
9. [Performance Troubleshooting](#section-9--performance-troubleshooting)
10. [Common Error Messages](#section-10--common-error-messages)
11. [Support Escalation](#section-11--support-escalation)
12. [Multi-Machine Management](#section-12--multi-machine-management)

---

## Section 1 --- System Requirements

### Windows Requirements

| Requirement | Minimum | Recommended |
|---|---|---|
| Operating System | Windows 10 (64-bit) | Windows 11 (64-bit) |
| RAM | 4 GB | 8 GB |
| Free Disk Space | 10 GB | 50 GB |
| Python | 3.11 or higher | 3.12.x |
| Network Ports | 8787 (dashboard), 8788 (client portal) | Same |
| Internet | Required for initial setup and AI API calls | Broadband recommended |

Additional Windows notes:

- Administrator privileges are required for installation (the bootstrap installer checks via `IsUserAnAdmin()`).
- The installer registers OtoCPA as a Windows Service named `OtoCPA` that starts automatically on boot.
- .NET Framework 4.7+ is recommended for NSSM service wrapper support.
- Antivirus software may need an exclusion for `C:\Program Files\OtoCPA\`.

### Mac / Apple Requirements

| Requirement | Minimum | Recommended |
|---|---|---|
| Operating System | macOS 12 Monterey | macOS 14 Sonoma or later |
| RAM | 4 GB | 8 GB |
| Free Disk Space | 10 GB | 50 GB |
| Python | 3.11 via Homebrew | 3.12.x via Homebrew |
| Network Ports | 8787 (dashboard), 8788 (client portal) | Same |
| Internet | Required for initial setup and AI API calls | Broadband recommended |

Key differences from Windows:

- No Windows Service --- use `launchd` for auto-start instead (see Section 3, Step 7).
- The `bootstrap_install.py` script is Windows-only; Mac installation is manual (clone + pip install).
- NSSM and `sc.exe` commands do not apply; use `launchctl` instead.
- Cloudflare tunnel service is managed via `launchd` rather than `sc.exe`.

---

## Section 2 --- Fresh Windows Installation

### Step 1: Download and Install Python 3.11+

1. Open a browser and go to: `https://www.python.org/downloads/`
2. Download **Python 3.12.x** (64-bit) for Windows --- click the "Download Python 3.12.x" button.
3. Run the installer. **Critical settings:**
   - Check **"Add python.exe to PATH"** at the bottom of the first screen.
   - Click **"Install Now"** (or choose "Customize installation" and ensure "pip" and "for all users" are selected).
4. After installation, open a Command Prompt and verify:

```
python --version
```

Expected output: `Python 3.12.x` (any version 3.11 or higher is acceptable).

> **Note:** If you skip this step, the bootstrap installer will attempt to download and install Python 3.12.3 automatically from `https://www.python.org/ftp/python/3.12.3/python-3.12.3-amd64.exe`.

### Step 2: Run bootstrap_install.py

1. Open a **Command Prompt as Administrator** (right-click Command Prompt, select "Run as administrator").
2. Navigate to the folder containing the bootstrap script.
3. Run the installer:

```
python bootstrap_install.py --license-key LLAI-XXXX-YOUR-KEY-HERE --firm-name "Your Firm Name CPA"
```

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `--license-key` | Yes | --- | Your license key (starts with `LLAI-`) |
| `--firm-name` | Yes | --- | Your firm or cabinet name |
| `--release-url` | No | `https://releases.otocpa.ai/latest/otocpa-latest.zip` | URL to download the release archive |
| `--install-dir` | No | `C:\Program Files\OtoCPA` | Installation directory |
| `--skip-python` | No | Off | Skip Python installation check |

The installer performs these steps automatically:

1. Checks/installs Python 3.11+
2. Verifies pip is available
3. Downloads the latest OtoCPA release archive
4. Installs Python dependencies from `requirements.txt`
5. Initializes the database via `migrate_db.py`
6. Saves the license key and firm name to `otocpa.config.json`
7. Registers the `OtoCPA` Windows Service (auto-start)
8. Creates desktop shortcuts (Dashboard + Setup Wizard)
9. Opens the setup wizard in your default browser

**Installation log:** All output is logged to `C:\OtoCPA\install.log`. If the installation fails, check this file for details.

### Step 3: Setup Wizard Walkthrough

After the bootstrap installer completes, the setup wizard opens automatically at `http://127.0.0.1:8790/`. The wizard has 6 steps:

**Step 1 --- Firm Information:**

| Field | Description | Example |
|---|---|---|
| Firm Name | Your firm or cabinet name | Tremblay CPA Inc. |
| Address | Office address | 123 rue Saint-Laurent, Montreal, QC H2X 1Y1 |
| GST Number | Federal GST/HST number | 123456789 RT0001 |
| QST Number | Quebec QST number | 1234567890 TQ0001 |
| Owner Name | Primary administrator name | Jean Tremblay |
| Owner Email | Login email for the owner account | jean@tremblycpa.ca |
| Password | Minimum 8 characters | (your password) |
| Confirm Password | Must match password | (same password) |

**Step 2 --- AI Configuration:**

| Field | Description | Recommended Value |
|---|---|---|
| Routine Provider | AI provider for everyday extractions | `deepseek` |
| API URL | Provider's API endpoint | `https://api.deepseek.com/v1` |
| API Key | Your API key from the provider | `sk-...` (from DeepSeek dashboard) |
| Model | Model name to use | `deepseek-chat` |
| Premium Provider | AI provider for complex documents | `anthropic` |

**Step 3 --- Email (SMTP):**

| Field | Description |
|---|---|
| SMTP Host | Your SMTP server address |
| Port | SMTP port number |
| SMTP User | SMTP username / email |
| SMTP Password | SMTP password or app password |
| From Email | The "from" address on outgoing emails |
| From Name | Display name on outgoing emails |

**Step 4 --- Microsoft 365 (optional):**

| Field | Description |
|---|---|
| Tenant ID | Azure AD tenant ID |
| Client ID | Azure AD application (client) ID |
| Client Secret | Azure AD application secret |
| SharePoint Site | SharePoint site URL for document ingestion |

This step is optional. Click "Skip this step" if you do not use Microsoft 365.

**Step 5 --- License Activation:**

Paste your license key (starts with `LLAI-`) and click "Validate". The system verifies the key, displays your tier and expiry date, and activates the license.

**Step 6 --- Complete:**

The wizard confirms setup is complete and provides a link to the dashboard.

### Step 4: Configure API Keys

OtoCPA uses two AI providers:

**DeepSeek (routine provider --- recommended for cost efficiency):**

1. Go to `https://platform.deepseek.com/` and create an account.
2. Navigate to API Keys and generate a new key.
3. Copy the key (starts with `sk-`).
4. Enter it in the setup wizard Step 2, or edit `otocpa.config.json`:

```json
{
  "ai_router": {
    "routine": {
      "provider": "deepseek",
      "base_url": "https://api.deepseek.com/v1",
      "api_key": "sk-YOUR-DEEPSEEK-KEY",
      "model": "deepseek-chat"
    }
  }
}
```

**Anthropic (premium provider --- recommended for complex documents):**

1. Go to `https://console.anthropic.com/` and create an account.
2. Navigate to API Keys and generate a new key.
3. Copy the key (starts with `sk-ant-`).
4. Enter it in the setup wizard Step 2, or add to `otocpa.config.json`:

```json
{
  "ai_router": {
    "premium": {
      "provider": "anthropic",
      "api_key": "sk-ant-YOUR-ANTHROPIC-KEY"
    }
  }
}
```

### Step 5: Configure Email (SMTP)

**Gmail:**

| Setting | Value |
|---|---|
| SMTP Host | `smtp.gmail.com` |
| Port | `587` |
| SMTP User | `your-email@gmail.com` |
| SMTP Password | App Password (not your Gmail password) |

To generate a Gmail App Password: Google Account > Security > 2-Step Verification > App passwords > Generate.

**Outlook / Microsoft 365:**

| Setting | Value |
|---|---|
| SMTP Host | `smtp.office365.com` |
| Port | `587` |
| SMTP User | `your-email@yourdomain.com` |
| SMTP Password | Your Outlook password or App Password |

**Custom SMTP (e.g., your hosting provider):**

| Setting | Value |
|---|---|
| SMTP Host | Provided by your host (e.g., `mail.yourdomain.com`) |
| Port | Usually `587` (TLS) or `465` (SSL) |
| SMTP User | Usually your full email address |
| SMTP Password | Provided by your host |

### Step 6: Setup Cloudflare Tunnel

Cloudflare Tunnel provides secure remote access to the client portal (port 8788) without opening firewall ports.

1. Open a **Command Prompt as Administrator**.
2. Navigate to the OtoCPA installation directory.
3. Run the Cloudflare setup wizard:

```
python scripts/setup_cloudflare.py
```

The wizard performs 7 steps:

1. **Downloads `cloudflared.exe`** from the official Cloudflare release URL to the `cloudflare/` folder.
2. **Authenticates with Cloudflare** --- opens your browser to sign in to your Cloudflare account and authorize the certificate.
3. **Creates a tunnel** named `otocpa` (or reuses an existing one).
4. **Writes the tunnel configuration** (`cloudflare/config.yml`) pointing to `localhost:8788`.
5. **Sets up DNS routing** --- prompts you for a public hostname (e.g., `portal.yourfirm.com`). You must have a domain in your Cloudflare account.
6. **Registers `cloudflared` as a Windows Service** with auto-start.
7. **Saves the public URL** to `otocpa.config.json` as `public_portal_url`.

**Optional flag:** If you have already authenticated with Cloudflare, use `--skip-login`:

```
python scripts/setup_cloudflare.py --skip-login
```

**To verify the tunnel is running:**

```
sc query cloudflared
```

Or visit `/troubleshoot` in the review dashboard.

### Step 7: Test the Installation

Run through this checklist after installation:

- [ ] Open `http://127.0.0.1:8787/` in a browser --- dashboard login page loads
- [ ] Log in with the owner email and password set during setup
- [ ] Navigate to `/troubleshoot` --- all checks show green
- [ ] Navigate to `/license` --- license tier and expiry displayed correctly
- [ ] Upload a test document --- AI extraction runs and returns results
- [ ] Check email --- send a test email from `/admin` settings
- [ ] Check Cloudflare --- visit your public URL (e.g., `https://portal.yourfirm.com`) from an external device
- [ ] Run autofix to confirm system health:

```
python scripts/autofix.py --lang en
```

All 14 checks should show `[PASS]` or `[FIXED]`.

### Step 8: Create First User Accounts

1. Log in to the dashboard as the owner.
2. Navigate to `/users`.
3. Click "Add User" and fill in:
   - **Email** (used as username)
   - **Display Name**
   - **Role:** `owner`, `manager`, `employee`, or `readonly`
   - **Password** (temporary --- user can change on first login)
   - **Language:** English or French
4. Click Save. The user can now log in at the dashboard URL.

### Step 9: Generate and Activate License Key

If you need to generate a new license key (admin-side):

```
python scripts/generate_license.py --tier professionnel --firm "Tremblay CPA" --months 12
```

The script prints the license key to the console. Copy it and provide it to the client.

To activate the key on the client machine:

1. Open the dashboard at `http://127.0.0.1:8787/license`.
2. Paste the license key into the "License Key" field.
3. Click "Validate" / "Activate".
4. The page displays the tier, expiry date, and feature list.

### Step 10: Hand Off to CPA Firm

Provide the client with:

1. **Dashboard URL:** `http://127.0.0.1:8787/` (local) or their public Cloudflare URL
2. **Owner login credentials** (email and temporary password)
3. **License key** (if not already activated)
4. **Instructions to change their password** on first login
5. **Access instructions PDF** (download from the setup wizard Step 6 "Complete" page)
6. **Support contact:** support@otocpa.com

---

## Section 3 --- Fresh Mac Installation

### Step 1: Install Homebrew

Open Terminal and run:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Follow the on-screen instructions. After installation, add Homebrew to your PATH as instructed.

### Step 2: Install Python 3.11+ via Homebrew

```bash
brew install python@3.12
```

Verify:

```bash
python3 --version
```

Expected output: `Python 3.12.x`

### Step 3: Clone or Download OtoCPA

**Option A --- Git clone:**

```bash
git clone https://your-repo-url/OtoCPAAi.git
cd OtoCPAAi
```

**Option B --- Download archive:**

Download the release ZIP from the release server, then:

```bash
unzip otocpa-latest.zip -d ~/OtoCPA
cd ~/OtoCPA
```

### Step 4: Install Dependencies with pip

```bash
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

### Step 5: Run migrate_db.py

```bash
python3 scripts/migrate_db.py
```

This creates and initializes the SQLite database at `data/otocpa_agent.db`.

### Step 6: Start the Dashboard Manually

On Mac there is no Windows Service. Start the dashboard directly:

```bash
python3 scripts/review_dashboard.py
```

The dashboard starts on port 8787. Open `http://127.0.0.1:8787/` in your browser.

To run in the background:

```bash
nohup python3 scripts/review_dashboard.py > logs/dashboard.log 2>&1 &
```

### Step 7: Set Up launchd for Auto-Start on Mac

Create a launchd plist file to start OtoCPA automatically on boot:

```bash
cat > ~/Library/LaunchAgents/com.otocpa.dashboard.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.otocpa.dashboard</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/python3</string>
        <string>/Users/YOUR_USERNAME/OtoCPA/scripts/review_dashboard.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/Users/YOUR_USERNAME/OtoCPA</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/YOUR_USERNAME/OtoCPA/logs/dashboard.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/YOUR_USERNAME/OtoCPA/logs/dashboard_error.log</string>
</dict>
</plist>
EOF
```

Replace `YOUR_USERNAME` with your macOS username. Then load it:

```bash
launchctl load ~/Library/LaunchAgents/com.otocpa.dashboard.plist
```

To verify it is running:

```bash
launchctl list | grep otocpa
```

To stop:

```bash
launchctl unload ~/Library/LaunchAgents/com.otocpa.dashboard.plist
```

### Step 8: Configure Cloudflare Tunnel on Mac

1. Install cloudflared via Homebrew:

```bash
brew install cloudflare/cloudflare/cloudflared
```

2. Authenticate:

```bash
cloudflared login
```

3. Create a tunnel:

```bash
cloudflared tunnel create otocpa
```

4. Create the config file at `~/.cloudflared/config.yml`:

```yaml
tunnel: YOUR_TUNNEL_UUID
credentials-file: /Users/YOUR_USERNAME/.cloudflared/YOUR_TUNNEL_UUID.json

ingress:
  - hostname: portal.yourfirm.com
    service: http://localhost:8788
  - service: http_status:404
```

5. Route DNS:

```bash
cloudflared tunnel route dns otocpa portal.yourfirm.com
```

6. Install as a service:

```bash
sudo cloudflared service install
```

Or create a launchd plist similar to the dashboard one above, pointing to `cloudflared tunnel run`.

### Step 9: Remaining Steps (Same as Windows)

Complete the same remaining steps as Windows:

- Open the setup wizard at `http://127.0.0.1:8790/` (or run `python3 scripts/setup_wizard.py` to start it).
- Walk through all 6 wizard steps (firm info, AI, email, M365, license, complete).
- Configure API keys (see Section 2, Step 4).
- Configure email SMTP (see Section 2, Step 5).
- Test the installation (see Section 2, Step 7).
- Create user accounts (see Section 2, Step 8).
- Generate and activate license (see Section 2, Step 9).

---

## Section 4 --- License Management

### License Tiers

| Tier | Max Clients | Max Users | Key Features |
|---|---|---|---|
| **essentiel** | 10 | 3 | Basic review, basic posting |
| **professionnel** | 30 | 5 | + AI router, bank parser, fraud detection, Revenu Quebec, time tracking, month-end |
| **cabinet** | 75 | 15 | + Analytics, Microsoft 365, filing calendar, client comms |
| **entreprise** | Unlimited | Unlimited | + Audit module, financial statements, sampling, API access |

### How to Generate a New License Key

Run the following command from the OtoCPA installation directory:

```
python scripts/generate_license.py --tier professionnel --firm "Tremblay CPA" --months 12
```

**All parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `--tier` | Yes | --- | One of: `essentiel`, `professionnel`, `cabinet`, `entreprise` |
| `--firm` | Yes | --- | Firm name (must match exactly for display purposes) |
| `--months` | No | 12 | License validity period in months |
| `--secret` | No | From `.env` | HMAC signing secret (overrides `.env` / environment) |
| `--max-clients` | No | Tier default | Override the maximum client count |
| `--max-users` | No | Tier default | Override the maximum user count |

**Where to find your signing secret:**

The signing secret is stored as `OTOCPA_SIGNING_SECRET` in your `.env` file at the root of the OtoCPA installation. Example `.env` content:

```
OTOCPA_SIGNING_SECRET=your-long-random-secret-string-here
```

If neither `.env` nor the `OTOCPA_SIGNING_SECRET` environment variable is set, you must pass `--secret` on the command line. Without a secret, the script will exit with an error.

**Example output:**

```
------------------------------------------------------------------------
  OtoCPA License Key
------------------------------------------------------------------------
  Firm        : Tremblay CPA
  Tier        : professionnel
  Issued      : 2026-03-25
  Expires     : 2027-03-25  (12 months)
  Max Clients : 30
  Max Users   : 5
  Features    : basic_review, basic_posting, ai_router, bank_parser, ...
------------------------------------------------------------------------

  License Key:
  LLAI-eyJ0aWVyIjoicHJvZmVzc2lvbm5lbCIsIm...

------------------------------------------------------------------------
```

### How to Activate a License at Client Site

1. Open the dashboard in a browser: `http://127.0.0.1:8787/license`
2. Paste the license key (starts with `LLAI-`) into the License Key field.
3. Click **Validate** (or **Activer** in French).
4. The page displays: tier name, firm name, expiry date, days remaining, and list of included features.
5. Verify all information is correct.

### How to Upgrade a Client's Tier

1. Generate a new license key with the higher tier:

```
python scripts/generate_license.py --tier cabinet --firm "Tremblay CPA" --months 12
```

2. Send the new key to the client (email, secure message, etc.).
3. Client opens `/license` in their dashboard and pastes the new key.
4. Click Activate --- features unlock immediately.
5. The old key is replaced. No data is lost.

### How to Downgrade a Tier

The process is identical to upgrading:

1. Generate a new key with a lower tier:

```
python scripts/generate_license.py --tier essentiel --firm "Tremblay CPA" --months 12
```

2. Client activates the new key via `/license`.
3. All existing data is preserved.
4. Features above the new tier are hidden (not deleted).

### How to Renew an Expiring License

1. Generate a new key with the same tier and a new validity period:

```
python scripts/generate_license.py --tier professionnel --firm "Tremblay CPA" --months 12
```

2. Send the key to the client before the current license expires.
3. Client activates via `/license`.
4. If the client misses the renewal deadline, there is a **30-day grace period** during which the system continues to function with a warning banner. After the grace period, the license status shows "expired" and features are locked.

### How to Revoke a License

To revoke access immediately (e.g., for non-payment or contract termination):

1. Generate a key with an expiry date in the past:

```
python scripts/generate_license.py --tier essentiel --firm "Tremblay CPA" --months 0 --secret YOUR_SECRET
```

Alternatively, set the expiry manually by editing the generated key's payload (advanced). The simplest approach is to generate with `--months 0`, which creates a key that expires today.

2. Send the key to the client and have them activate it, or activate it remotely via their dashboard.
3. The system immediately shows "License expired" and locks features.

### How to Handle Lost License Keys

1. Re-generate a new key with the same parameters (tier, firm name, duration):

```
python scripts/generate_license.py --tier professionnel --firm "Tremblay CPA" --months 12
```

2. The old key continues to work until the new one is activated.
3. Once the client activates the new key, it replaces the old one in `otocpa.config.json`.
4. Only one key is active at a time per installation.

---

## Section 5 --- Remote Support Procedures

### How to Connect to a Client Machine Remotely

1. **Verify the Cloudflare tunnel is running** on the client machine. Ask the client to check:
   - Windows: `sc query cloudflared` --- should show `RUNNING`.
   - Or visit `/troubleshoot` in their local dashboard.

2. **Get their public URL:**
   - The public URL is stored in `otocpa.config.json` as `public_portal_url`.
   - The client can find it on the `/troubleshoot` page.
   - Example: `https://portal.tremblycpa.com`

3. **Connect via browser:**
   - Open the public URL in your browser.
   - Log in with admin credentials.

4. **Use `/admin/remote`** for system controls (restart services, view logs, etc.).

### How to Run Autofix Remotely

1. Connect to the client's dashboard via their public URL.
2. Navigate to `/troubleshoot`.
3. Click **"Run Autofix"** button.
4. The autofix script runs all 14 diagnostic checks:
   1. Database integrity and foreign keys
   2. Missing columns
   3. Missing tables
   4. Orphaned sessions
   5. Locked periods
   6. Port conflicts (8787, 8788)
   7. Config file validity
   8. Python dependencies
   9. Recent log errors (last 24 hours)
   10. Dashboard smoke test
   11. Inbox folder (folder watcher)
   12. Cloudflare tunnel service
   13. License check
   14. Version check
5. Review the results. Items marked `[PASS]` or `[FIXED]` are healthy. Items marked `[FAIL]` require manual intervention.

To run autofix from the command line (if you have terminal access):

```
python scripts/autofix.py --lang en
```

### Common Support Scenarios

**Dashboard not loading:**

1. Check if the Windows Service is running: `sc query OtoCPA`
2. If stopped, start it: `sc start OtoCPA`
3. If it won't start, check the log: `C:\OtoCPA\install.log`
4. Run autofix: `python scripts/autofix.py --lang en`
5. Check if port 8787 is in use by another process (autofix check #6 handles this)

**Login not working:**

1. Verify the user account is active: `/users` page as owner.
2. Reset the user's password via the `/users` page.
3. Check for expired sessions: autofix check #4 cleans these automatically.
4. If the owner is locked out, reset the admin password from the command line:

```
python scripts/set_password.py
```

**Documents not processing:**

1. Check AI API keys are valid and have credit: `/admin` settings page.
2. Check disk space: `/troubleshoot` shows available disk space.
3. Check the `data/` folder for the database file.
4. Review recent log errors: autofix check #9.
5. Test AI connectivity: upload a simple, clean document and check the result.

**Email not sending:**

1. Verify SMTP configuration in `otocpa.config.json`.
2. For Gmail: ensure you are using an App Password, not your regular password.
3. For Outlook: ensure your account allows SMTP relay.
4. Check spam/junk folders on the receiving end.
5. Test with the "Test Email" button in the setup wizard Step 3.

**Cloudflare tunnel disconnected:**

1. Check the service: `sc query cloudflared`
2. If stopped, restart: `sc start cloudflared`
3. If the service fails to start:
   - Check the log at `cloudflare/cloudflared.log`
   - Re-run the setup: `python scripts/setup_cloudflare.py --skip-login`
4. Verify DNS records in your Cloudflare dashboard.

**Database errors:**

1. Run a PRAGMA integrity check:

```
python -c "import sqlite3; conn=sqlite3.connect('data/otocpa_agent.db'); print(conn.execute('PRAGMA integrity_check').fetchone())"
```

2. If the result is not `('ok',)`, restore from backup (see Section 6).
3. Run `python scripts/migrate_db.py` to apply any missing migrations.

**QuickBooks connection failed:**

1. Check that QBO credentials are configured in `otocpa.config.json`.
2. Re-authorize the QBO connection via the dashboard `/admin` settings.
3. Verify the QBO API key has not expired.
4. Check Intuit's service status for outages.

**AI extraction returning wrong results:**

1. Check the AI API key balance --- if the key has run out of credits, calls silently fail or return garbage.
2. Check the provider's status page (DeepSeek status, Anthropic status).
3. Try switching to the premium provider for the problematic document.
4. Review the extraction rules in `/admin` settings.

---

## Section 6 --- Backup and Restore

### Automatic Backup

- **Location:** `data/backups/`
- **Naming convention:** `otocpa_agent_YYYYMMDD_HHMMSS.db`
- **Trigger:** Backups are created automatically before every update (by `update_otocpa.py`).
- **Application backups:** `data/backups/app_backup_YYYYMMDD_HHMMSS/` (contains `scripts/`, `src/`, `version.json`, and `otocpa.config.json`).

### How to Trigger a Manual Backup

**From the dashboard:**

1. Navigate to `/troubleshoot`.
2. Click **"Download DB Backup"**.
3. The browser downloads a copy of `otocpa_agent.db`.

**From the command line:**

```
copy data\otocpa_agent.db data\backups\otocpa_agent_manual_%date:~-4%%date:~4,2%%date:~7,2%.db
```

On Mac/Linux:

```bash
cp data/otocpa_agent.db "data/backups/otocpa_agent_manual_$(date +%Y%m%d_%H%M%S).db"
```

### How to Restore from Backup

**Step 1: Stop the Windows Service**

```
sc stop OtoCPA
```

Wait a few seconds for the service to stop completely.

**Step 2: Copy the backup file**

```
copy data\backups\otocpa_agent_20260325_143000.db data\otocpa_agent.db
```

Replace the filename with the backup you want to restore.

**Step 3: Run database migrations**

```
python scripts/migrate_db.py
```

This ensures the restored database has all the latest schema changes.

**Step 4: Start the Windows Service**

```
sc start OtoCPA
```

**Step 5: Verify data is intact**

1. Open the dashboard and log in.
2. Check that clients, documents, and users are present.
3. Navigate to `/troubleshoot` and run autofix to confirm system health.

### Backup Retention Policy

- Keep backups for the last **30 days**.
- Older backups can be safely deleted to free disk space.
- Before deleting old backups, verify that at least one recent backup is valid.

### How to Move Backups to External Storage

Copy the `data/backups/` folder to an external drive, network share, or cloud storage:

```
xcopy /E /Y data\backups\ E:\OtoCPA_Backups\
```

On Mac/Linux:

```bash
rsync -av data/backups/ /Volumes/ExternalDrive/OtoCPA_Backups/
```

Schedule this with Windows Task Scheduler or a cron job for automated offsite backups.

---

## Section 7 --- Updates and Upgrades

### How to Check for Updates

**From the dashboard:**

Navigate to `/admin/updates`. The page shows the installed version and checks the update server for newer versions.

**From the command line:**

```
python scripts/update_otocpa.py --check
```

Output example:

```
OtoCPA Update Check
==================================================
  Installed version : 1.2.0
  Release date      : 2026-03-01
  Update server     : https://releases.otocpa.ai/latest/version.json

  UPDATE AVAILABLE: 1.2.0 -> 1.3.0
  Release date : 2026-03-20
  Changelog    : Bug fixes and performance improvements

  Run: python update_otocpa.py --install
```

### How to Install an Update Remotely

1. Connect to the client's dashboard via their public Cloudflare URL.
2. Navigate to `/admin/updates`.
3. Click **"Install Update"** button.
4. The update process runs automatically:
   - Stops the OtoCPA service
   - Creates database and application backups
   - Downloads the update package
   - Applies the update files
   - Runs database migrations
   - Starts the service
   - Verifies the dashboard responds
5. If any step fails, the system **automatically rolls back** to the pre-update backup.

### How to Install an Update Manually

**Step 1: Check for available updates**

```
python scripts/update_otocpa.py --check
```

**Step 2: Install the update**

```
python scripts/update_otocpa.py --install
```

This command:
- Stops the OtoCPA Windows Service
- Backs up the database and application files
- Downloads and extracts the update
- Runs `migrate_db.py`
- Restarts the service
- Verifies the dashboard responds on port 8787

**Step 3: Verify the version**

Check the version number in the dashboard footer, or:

```
python -c "import json; print(json.load(open('version.json'))['version'])"
```

### What to Do If an Update Fails

1. **Check the install log:** `C:\OtoCPA\install.log`
2. **The update system auto-rolls back** if migrations fail or the dashboard does not respond after the update.
3. **To manually roll back:**

```
python scripts/update_otocpa.py --rollback
```

This restores the most recent application and database backups.

4. If rollback also fails, manually restore from backup (see Section 6).
5. **Contact support** with the error message from the install log: support@otocpa.com

---

## Section 8 --- Security Management

### How to Reset a Forgotten Password

**Owner account:**

If the owner is locked out, reset the password from the command line on the server:

```
python scripts/set_password.py
```

Follow the prompts to set a new password.

**Other users:**

1. Log in as the owner.
2. Navigate to `/users`.
3. Find the user and click "Reset Password".
4. Set a new temporary password.
5. Inform the user of their new password.

### How to Deactivate a Staff Member Who Left

1. Log in to the dashboard as the owner.
2. Navigate to `/users`.
3. Find the user's entry.
4. Click **"Deactivate"**.
5. The user's account is disabled --- they can no longer log in.
6. All documents and activity associated with the user are preserved in the audit log.

### How to Review Security Events

1. Navigate to `/audit` in the dashboard.
2. Use the filters to search for specific event types:
   - `login_failed` --- failed login attempts
   - `invalid_state_blocked` --- blocked suspicious activity
   - `login_success` --- successful logins
   - `password_changed` --- password changes
3. Look for unusual patterns:
   - Multiple failed logins from the same user or IP
   - Logins at unusual hours
   - Activity from unknown IP addresses

### How to Handle a Suspected Security Breach

**Step 1: Change all passwords immediately**

1. Navigate to `/users`.
2. Reset passwords for every user account, starting with the owner.

**Step 2: Review the audit log**

1. Navigate to `/audit`.
2. Filter by the time period of suspected breach.
3. Look for: unauthorized logins, unusual document access, unexpected postings.

**Step 3: Check for unauthorized postings**

1. Review recent postings in the document queue.
2. Verify no unauthorized transactions were posted to QuickBooks.

**Step 4: Contact support**

Email support@otocpa.com with:
- Time of suspected breach
- What was observed
- Audit log export (if available)

### How to Rotate API Keys

1. Generate new API keys from your AI providers:
   - **DeepSeek:** `https://platform.deepseek.com/` > API Keys
   - **Anthropic:** `https://console.anthropic.com/` > API Keys
2. Update `otocpa.config.json` with the new keys in the `ai_router` section.
3. Restart the Windows Service:

```
sc stop OtoCPA
sc start OtoCPA
```

4. Verify AI extraction still works by uploading a test document.
5. Revoke the old keys from the provider dashboards.

---

## Section 9 --- Performance Troubleshooting

### Dashboard Is Slow

**Check database size:**

1. Navigate to `/troubleshoot` --- the page displays the current database size.
2. If the database is very large (> 500 MB), run VACUUM to reclaim space:

```
python -c "import sqlite3; conn=sqlite3.connect('data/otocpa_agent.db'); conn.execute('VACUUM'); conn.close()"
```

This can take several minutes for large databases. Stop the service first to avoid lock conflicts:

```
sc stop OtoCPA
python -c "import sqlite3; conn=sqlite3.connect('data/otocpa_agent.db'); conn.execute('VACUUM'); conn.close()"
sc start OtoCPA
```

**Check disk space:**

Ensure at least 5 GB of free space on the drive containing `data/otocpa_agent.db`. Full disks cause severe performance degradation with SQLite.

**Check RAM usage:**

Open Task Manager (Ctrl+Shift+Esc) and check if the system is running low on memory. OtoCPA typically uses 200--500 MB of RAM. If the system has only 4 GB total, consider upgrading to 8 GB.

### Tests Taking Too Long

Use the fast test command that skips long-running test suites:

```
python -m pytest tests/ -q --ignore=tests/test_generate_test_data.py --ignore=tests/test_stress_test.py --ignore=tests/test_accelerate_learning.py
```

---

## Section 10 --- Common Error Messages

### "Port 8787 is already in use" / "Port 8788 is already in use"

**Cause:** Another process is using the port that OtoCPA needs.

**Fix:**

1. Find what is using the port:

```
netstat -ano | findstr :8787
```

2. Note the PID (last column).
3. Identify the process:

```
tasklist /FI "PID eq 12345"
```

4. Stop the conflicting process, or change OtoCPA's port in the dashboard script.
5. Alternatively, run autofix which can detect and offer to kill conflicting processes:

```
python scripts/autofix.py --lang en
```

### "Database is locked"

**Cause:** Multiple processes are trying to write to the SQLite database simultaneously, or a crashed process left a lock file.

**Fix:**

1. Stop all OtoCPA processes:

```
sc stop OtoCPA
```

2. Check for any remaining Python processes:

```
tasklist | findstr python
```

3. Kill any stale Python processes related to OtoCPA.
4. Delete any `-journal` or `-wal` files next to the database (only if the service is fully stopped):

```
del data\otocpa_agent.db-journal
del data\otocpa_agent.db-wal
```

5. Restart the service:

```
sc start OtoCPA
```

### "API key invalid" / "Invalid API key"

**Cause:** The AI provider API key is incorrect, expired, or has been revoked.

**Fix:**

1. Verify the API key in `otocpa.config.json` under the `ai_router` section.
2. Log in to the provider's dashboard and verify the key is active:
   - DeepSeek: `https://platform.deepseek.com/`
   - Anthropic: `https://console.anthropic.com/`
3. Check that the key has available credits/balance.
4. If the key was rotated, update `otocpa.config.json` and restart the service.

### "SMTP authentication failed"

**Cause:** Email credentials are incorrect or the email provider requires app-specific passwords.

**Fix:**

1. Verify SMTP settings in `otocpa.config.json`.
2. For Gmail: you must use an **App Password**, not your regular Google password. Enable 2-Step Verification first, then generate an App Password.
3. For Outlook/M365: check if your organization requires OAuth instead of basic auth.
4. Test the credentials by sending a test email from the setup wizard or `/admin` settings.

### "Cloudflare Tunnel not connected"

**Cause:** The `cloudflared` service is not running or the tunnel configuration is invalid.

**Fix:**

1. Check the service:

```
sc query cloudflared
```

2. If stopped, start it:

```
sc start cloudflared
```

3. If it fails to start, check the log:

```
type cloudflare\cloudflared.log
```

4. Common issues:
   - Expired certificate: re-run `cloudflared login`
   - Invalid config: verify `cloudflare/config.yml`
   - DNS not pointing to tunnel: check Cloudflare dashboard DNS records

### "License expired"

**Cause:** The license key's expiry date has passed.

**Fix:**

1. Generate a new license key with a renewed period (see Section 4).
2. Send the new key to the client.
3. Activate via `/license` in the dashboard.
4. There is a 30-day grace period after expiry during which the system still functions with warnings.

### "Disk full" / "No space left on device"

**Cause:** The drive containing OtoCPA has run out of free space.

**Fix:**

1. Check disk space: right-click the drive in File Explorer > Properties.
2. Free up space:
   - Delete old backups: `data/backups/` (keep last 7 days minimum)
   - Run VACUUM on the database (see Section 9)
   - Delete temporary files from `%TEMP%`
   - Move the `exports/` folder to an external drive
3. Consider moving OtoCPA to a larger drive.

### "Python version too old"

**Cause:** The installed Python version is below 3.11.

**Fix:**

1. Check current version: `python --version`
2. Download Python 3.12 from `https://www.python.org/downloads/`
3. Install with "Add to PATH" checked.
4. Restart the OtoCPA service.

### "Invalid license key format"

**Cause:** The license key does not start with `LLAI-` or is corrupted.

**Fix:**

1. Verify the key was copied completely (no truncation).
2. Ensure it starts with `LLAI-`.
3. If the key appears corrupted, request a new one from your administrator.

### "License signature mismatch"

**Cause:** The signing secret on the client machine does not match the one used to generate the key.

**Fix:**

1. Verify that `OTOCPA_SIGNING_SECRET` in the client's `.env` file matches the secret used when generating the key.
2. If they do not match, either:
   - Update the client's `.env` with the correct secret, or
   - Re-generate the key using the secret that matches the client's `.env`.

---

## Section 11 --- Support Escalation

### What You Can Fix Yourself (with autofix.py)

The autofix script can automatically resolve:

- Missing database columns (runs `migrate_db.py`)
- Missing database tables (recreates them)
- Orphaned/expired sessions (deletes them)
- Missing config file (regenerates with defaults)
- Corrupted config file (backs up and regenerates)
- Missing Python dependencies (runs `pip install`)
- Port conflicts (offers to kill conflicting processes)
- Stopped Cloudflare tunnel (restarts the service)
- Missing inbox folder (creates it)

Run it with:

```
python scripts/autofix.py --lang en
```

### What Requires Remote Support

- Cloudflare tunnel initial setup or re-configuration
- Complex database corruption that autofix cannot repair
- API key configuration issues
- QuickBooks re-authorization
- License key issues (generation, activation)
- Custom SMTP configuration
- Microsoft 365 integration setup

### What Requires a Site Visit

- Hardware failures (disk failure, RAM issues)
- Windows OS reinstallation
- Network infrastructure issues (firewall, router)
- Initial installation on machines without internet access
- Physical server setup

### How to Collect Diagnostic Information Before Contacting Support

1. **Run autofix and save the output:**

```
python scripts/autofix.py --lang en > diagnostic_report.txt 2>&1
```

2. **Download a database backup** from `/troubleshoot` > "Download DB Backup".

3. **Note the exact error message** and the steps to reproduce the issue.

4. **Check the version:**

```
type version.json
```

5. **Send all of the above** to support@otocpa.com.

### Support Contact

- **Email:** support@otocpa.com
- **Response time:** Within 4 business hours on business days (Mon--Fri, 9:00--17:00 Eastern Time).

---

## Section 12 --- Multi-Machine Management

### Machine Limits

Each license key supports up to **3 machines** per firm. The system tracks machine activations via a unique machine ID derived from the computer name and disk serial number.

### How to Install on a Second Machine at the Same Firm

1. Follow the same bootstrap installation process on the second machine (Section 2 or Section 3).
2. Use the **same license key** as the first machine.
3. Activate the license on the second machine via `/license`.
4. The system registers the new machine and displays the total count (e.g., "2 of 3 machines").

**Database sharing options:**

- **Same network:** Both machines can share the same database if the database file is on a shared network drive. Point both installations to the same `data/otocpa_agent.db` path.
- **Standalone:** Each machine has its own database. Documents and users are independent.

### How to Migrate to a New Computer

**Step 1: Install OtoCPA on the new computer**

Follow the full installation process (Section 2 for Windows, Section 3 for Mac).

**Step 2: Copy the database from the old machine**

```
copy \\OLD_MACHINE\C$\Program Files\OtoCPA\data\otocpa_agent.db "C:\Program Files\OtoCPA\data\otocpa_agent.db"
```

Or transfer via USB drive, network share, etc.

**Step 3: Copy the configuration file**

```
copy \\OLD_MACHINE\C$\Program Files\OtoCPA\otocpa.config.json "C:\Program Files\OtoCPA\otocpa.config.json"
```

**Step 4: Run database migrations on the new computer**

```
python scripts/migrate_db.py
```

**Step 5: Activate the license on the new machine**

1. Open the dashboard: `http://127.0.0.1:8787/license`
2. The existing license key from the config file should auto-load.
3. Click Activate to register the new machine.

**Step 6: Deactivate the old machine**

If the old machine is still running OtoCPA:

1. Stop the service on the old machine: `sc stop OtoCPA`
2. Uninstall the service: `sc delete OtoCPA`
3. Optionally delete the installation directory.

The old machine's activation slot is freed automatically when the license is next validated and that machine has not checked in recently.

### Viewing Registered Machines

Administrators can view all machines registered under their license via the `/license` page in the dashboard. The page shows:

- Machine name (computer hostname)
- Machine ID (truncated for security)
- First activation date
- Last seen date
- Whether this is the current machine

---

*End of OtoCPA System Administrator Guide*
*For the latest version of this document, check the `docs/` folder in your OtoCPA installation.*
*Support: support@otocpa.com*
