#!/usr/bin/env bash
# ============================================================
# LedgerLink AI — One-Line macOS Installer
# ============================================================
# Usage:
#   curl -fsSL https://install.ledgerlink.ca/install.sh | bash
#
# With license key:
#   LEDGERLINK_KEY="LLAI-XXXX" curl -fsSL https://install.ledgerlink.ca/install.sh | bash
# ============================================================

set -euo pipefail

INSTALL_DIR="$HOME/LedgerLink"
RELEASE_URL="https://releases.ledgerlink.ai/latest/ledgerlink-latest.tar.gz"
RELEASE_MIRROR1="https://cdn.ledgerlink.ai/releases/ledgerlink-latest.tar.gz"
RELEASE_MIRROR2="https://github.com/ledgerlink/releases/releases/latest/download/ledgerlink-latest.tar.gz"
PLIST_LABEL="ca.ledgerlink.dashboard"
PLIST_PATH="$HOME/Library/LaunchAgents/${PLIST_LABEL}.plist"
DASH_PORT=8787

# ============================================================
# Banner
# ============================================================
echo ""
echo "  +======================================================"
echo "  |"
echo "  |   LedgerLink AI — macOS Installer"
echo "  |   Installation automatique / Auto Install"
echo "  |"
echo "  +======================================================"
echo ""

# ============================================================
# Step 1: Install Homebrew if missing
# ============================================================
echo "  [1/8] Checking Homebrew..."

if ! command -v brew &>/dev/null; then
    echo "         Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

    # Add brew to PATH for Apple Silicon
    if [[ -f /opt/homebrew/bin/brew ]]; then
        eval "$(/opt/homebrew/bin/brew shellenv)"
    fi
    echo "         Homebrew installed"
else
    echo "         Homebrew already installed"
fi

# ============================================================
# Step 2: Install Python 3.11 via Homebrew
# ============================================================
echo "  [2/8] Checking Python 3.11..."

if ! command -v python3.11 &>/dev/null; then
    echo "         Installing Python 3.11..."
    brew install python@3.11
    echo "         Python 3.11 installed"
else
    echo "         Python 3.11 already installed"
fi

PYTHON="$(command -v python3.11 || command -v python3)"
echo "         Using: $PYTHON"

# ============================================================
# Step 3: Download and extract LedgerLink
# ============================================================
echo "  [3/8] Downloading LedgerLink..."

mkdir -p "$INSTALL_DIR"
ARCHIVE="$INSTALL_DIR/ledgerlink-latest.tar.gz"

downloaded=false
for url in "$RELEASE_URL" "$RELEASE_MIRROR1" "$RELEASE_MIRROR2"; do
    if [ "$downloaded" = false ]; then
        if curl -fsSL --connect-timeout 30 --max-time 120 -o "$ARCHIVE" "$url" 2>/dev/null; then
            if [ -f "$ARCHIVE" ] && [ -s "$ARCHIVE" ]; then
                downloaded=true
                echo "         Downloaded from $url"
            fi
        else
            echo "         Mirror failed: $url"
        fi
    fi
done

if [ "$downloaded" = false ]; then
    echo "  ERROR: Could not download LedgerLink."
    echo "  Check your internet connection and try again."
    echo "  Contact: support@ledgerlink.ca"
    exit 1
fi

echo "         Extracting..."
tar -xzf "$ARCHIVE" -C "$INSTALL_DIR" 2>/dev/null || {
    echo "  ERROR: Failed to extract archive."
    exit 1
}
echo "         Extraction complete"

# ============================================================
# Step 4: Install Python dependencies
# ============================================================
echo "  [4/8] Installing Python packages..."

cd "$INSTALL_DIR"

if [ -f requirements.txt ]; then
    "$PYTHON" -m pip install --upgrade pip --quiet 2>/dev/null || true
    "$PYTHON" -m pip install -r requirements.txt --quiet
    echo "         Packages installed"
else
    echo "         No requirements.txt found — skipping"
fi

# ============================================================
# Step 5: Initialize database
# ============================================================
echo "  [5/8] Initializing database..."

mkdir -p "$INSTALL_DIR/data"

if [ -f scripts/migrate_db.py ]; then
    "$PYTHON" scripts/migrate_db.py 2>/dev/null || true
    echo "         Database initialized"
else
    echo "         migrate_db.py not found — skipping"
fi

# ============================================================
# Step 6: Create launchd plist for auto-start
# ============================================================
echo "  [6/8] Setting up auto-start..."

mkdir -p "$HOME/Library/LaunchAgents"

cat > "$PLIST_PATH" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${PLIST_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>${PYTHON}</string>
        <string>${INSTALL_DIR}/scripts/review_dashboard.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${INSTALL_DIR}</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>${INSTALL_DIR}/data/dashboard_stdout.log</string>
    <key>StandardErrorPath</key>
    <string>${INSTALL_DIR}/data/dashboard_stderr.log</string>
</dict>
</plist>
PLIST

launchctl load "$PLIST_PATH" 2>/dev/null || true
echo "         LaunchAgent created and loaded"

# ============================================================
# Step 7: Install Cloudflare via Homebrew
# ============================================================
echo "  [7/8] Setting up Cloudflare tunnel..."

if ! command -v cloudflared &>/dev/null; then
    brew install cloudflare/cloudflare/cloudflared 2>/dev/null || {
        echo "         Could not install cloudflared via Homebrew"
        echo "         You can install it manually later"
    }
    if command -v cloudflared &>/dev/null; then
        echo "         Cloudflared installed"
    fi
else
    echo "         Cloudflared already installed"
fi

# ============================================================
# Step 8: Save license key and open setup wizard
# ============================================================
echo "  [8/8] Finishing up..."

# Save license key if provided
if [ -n "${LEDGERLINK_KEY:-}" ]; then
    echo "         License key detected: $LEDGERLINK_KEY"
    CONFIG_FILE="$INSTALL_DIR/ledgerlink.config.json"
    if [ -f "$CONFIG_FILE" ] && command -v python3 &>/dev/null; then
        python3 -c "
import json
try:
    cfg = json.loads(open('$CONFIG_FILE').read())
    cfg['license_key'] = '$LEDGERLINK_KEY'
    open('$CONFIG_FILE', 'w').write(json.dumps(cfg, indent=2))
    print('         License key saved to config')
except Exception as e:
    print(f'         Could not save license key: {e}')
"
    fi
fi

# Wait for dashboard to start
echo "         Waiting for dashboard to start..."
sleep 3

# Open setup wizard in browser
if command -v open &>/dev/null; then
    open "http://localhost:${DASH_PORT}"
    echo "         Setup wizard opened in browser"
fi

echo ""
echo "  +======================================================"
echo "  |"
echo "  |   Installation complete / Installation terminee"
echo "  |"
echo "  |   Dashboard: http://localhost:${DASH_PORT}"
echo "  |   Support:   support@ledgerlink.ca"
echo "  |"
echo "  +======================================================"
echo ""
