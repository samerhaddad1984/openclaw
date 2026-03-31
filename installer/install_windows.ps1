# ============================================================
# OtoCPA — One-Line Windows Installer
# ============================================================
# Usage:
#   powershell -ExecutionPolicy Bypass -c "iwr https://install.otocpa.com/install.ps1 | iex"
#
# With license key:
#   powershell -ExecutionPolicy Bypass -c "$key='LLAI-XXXX'; iwr https://install.otocpa.com/install.ps1 | iex"
# ============================================================

$ErrorActionPreference = "Stop"
$InstallDir = "C:\OtoCPA"
$ReleaseUrl = "https://releases.otocpa.ai/latest/otocpa-latest.zip"
$ReleaseMirror1 = "https://cdn.otocpa.ai/releases/otocpa-latest.zip"
$ReleaseMirror2 = "https://github.com/otocpa/releases/releases/latest/download/otocpa-latest.zip"
$InstallerBatUrl = "https://install.otocpa.com/INSTALL_SMART.bat"

# ============================================================
# Banner
# ============================================================
Write-Host ""
Write-Host "  +======================================================+" -ForegroundColor Cyan
Write-Host "  |                                                        |" -ForegroundColor Cyan
Write-Host "  |        OtoCPA - Windows Installer               |" -ForegroundColor Cyan
Write-Host "  |        Installation automatique / Auto Install         |" -ForegroundColor Cyan
Write-Host "  |                                                        |" -ForegroundColor Cyan
Write-Host "  +======================================================+" -ForegroundColor Cyan
Write-Host ""

# ============================================================
# Check for admin privileges
# ============================================================
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "  Elevation required / Elevation requise..." -ForegroundColor Yellow
    Write-Host "  Please click Yes when Windows asks for permission." -ForegroundColor Yellow
    Write-Host ""

    # Re-launch as admin, passing license key if present
    $scriptBlock = "Set-ExecutionPolicy Bypass -Scope Process -Force; "
    if ($key) {
        $scriptBlock += "`$key='$key'; "
    }
    $scriptBlock += "iwr https://install.otocpa.com/install.ps1 | iex"

    Start-Process powershell -Verb RunAs -ArgumentList "-ExecutionPolicy", "Bypass", "-Command", $scriptBlock
    exit 0
}

Write-Host "  [1/5] Administrator privileges OK" -ForegroundColor Green

# ============================================================
# Step 1: Create install directory
# ============================================================
Write-Host "  [2/5] Creating $InstallDir..." -ForegroundColor White

if (-not (Test-Path $InstallDir)) {
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
}
if (-not (Test-Path "$InstallDir\data")) {
    New-Item -ItemType Directory -Path "$InstallDir\data" -Force | Out-Null
}

Write-Host "         Directory ready" -ForegroundColor Green

# ============================================================
# Step 2: Download OtoCPA ZIP
# ============================================================
Write-Host "  [3/5] Downloading OtoCPA..." -ForegroundColor White

$zipPath = "$InstallDir\otocpa-latest.zip"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$downloaded = $false
foreach ($url in @($ReleaseUrl, $ReleaseMirror1, $ReleaseMirror2)) {
    if (-not $downloaded) {
        try {
            Invoke-WebRequest -Uri $url -OutFile $zipPath -UseBasicParsing -TimeoutSec 120
            if (Test-Path $zipPath) {
                $downloaded = $true
                Write-Host "         Downloaded from $url" -ForegroundColor Green
            }
        } catch {
            Write-Host "         Mirror failed: $url" -ForegroundColor Yellow
        }
    }
}

if (-not $downloaded) {
    Write-Host "  ERROR: Could not download OtoCPA." -ForegroundColor Red
    Write-Host "  Check your internet connection and try again." -ForegroundColor Red
    Write-Host "  Contact: support@otocpa.com" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

# Extract ZIP
Write-Host "         Extracting..." -ForegroundColor White
try {
    Expand-Archive -Path $zipPath -DestinationPath $InstallDir -Force
    Write-Host "         Extraction complete" -ForegroundColor Green
} catch {
    Write-Host "  ERROR: Failed to extract ZIP." -ForegroundColor Red
    exit 1
}

# ============================================================
# Step 3: Download INSTALL_SMART.bat
# ============================================================
Write-Host "  [4/5] Downloading installer script..." -ForegroundColor White

$batPath = "$InstallDir\INSTALL_SMART.bat"
try {
    Invoke-WebRequest -Uri $InstallerBatUrl -OutFile $batPath -UseBasicParsing -TimeoutSec 60
    Write-Host "         INSTALL_SMART.bat ready" -ForegroundColor Green
} catch {
    # If the BAT is already in the ZIP, use that
    if (Test-Path "$InstallDir\installer\INSTALL_SMART.bat") {
        Copy-Item "$InstallDir\installer\INSTALL_SMART.bat" $batPath -Force
        Write-Host "         Using bundled INSTALL_SMART.bat" -ForegroundColor Green
    } else {
        Write-Host "  WARNING: Could not download INSTALL_SMART.bat" -ForegroundColor Yellow
    }
}

# ============================================================
# Step 4: Write license key to config if provided
# ============================================================
if ($key) {
    Write-Host "         License key detected: $key" -ForegroundColor Green
    $configPath = "$InstallDir\otocpa.config.json"
    if (Test-Path $configPath) {
        try {
            $config = Get-Content $configPath -Raw | ConvertFrom-Json
            $config | Add-Member -NotePropertyName "license_key" -NotePropertyValue $key -Force
            $config | ConvertTo-Json -Depth 10 | Set-Content $configPath -Encoding UTF8
            Write-Host "         License key saved to config" -ForegroundColor Green
        } catch {
            Write-Host "         Could not save license key to config" -ForegroundColor Yellow
        }
    }

    # Also set environment variable for setup wizard
    [System.Environment]::SetEnvironmentVariable("OTOCPA_LICENSE_KEY", $key, "Process")
}

# ============================================================
# Step 5: Run INSTALL_SMART.bat
# ============================================================
Write-Host "  [5/5] Running INSTALL_SMART.bat..." -ForegroundColor White
Write-Host ""
Write-Host "  The installer will now run. Follow any prompts." -ForegroundColor Cyan
Write-Host ""

if (Test-Path $batPath) {
    Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "`"$batPath`"" -Verb RunAs -Wait
    Write-Host ""
    Write-Host "  +======================================================+" -ForegroundColor Green
    Write-Host "  |                                                        |" -ForegroundColor Green
    Write-Host "  |       Installation complete / Installation terminee    |" -ForegroundColor Green
    Write-Host "  |                                                        |" -ForegroundColor Green
    Write-Host "  |   Dashboard: http://localhost:8787                      |" -ForegroundColor Green
    Write-Host "  |   Support:   support@otocpa.com                     |" -ForegroundColor Green
    Write-Host "  |                                                        |" -ForegroundColor Green
    Write-Host "  +======================================================+" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "  ERROR: INSTALL_SMART.bat not found at $batPath" -ForegroundColor Red
    Write-Host "  Please run the installer manually." -ForegroundColor Red
    exit 1
}
