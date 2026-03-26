param(
    [string]$InstallRoot = "C:\LedgerLinkAI",
    [string]$PythonCommand = "python",
    [switch]$SkipVenv,
    [switch]$SkipDependencies,
    [switch]$SkipDesktopShortcut
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host $Message -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
}

function Ensure-Directory {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

function Remove-IfExists {
    param([string]$Path)
    if (Test-Path $Path) {
        Remove-Item -Path $Path -Recurse -Force
    }
}

function Find-Python {
    param([string]$PreferredCommand)

    try {
        $null = & $PreferredCommand --version 2>$null
        return $PreferredCommand
    }
    catch {
    }

    $candidates = @(
        "py",
        "python",
        "C:\Python311\python.exe",
        "C:\Program Files\Python311\python.exe",
        "C:\Users\$env:USERNAME\AppData\Local\Programs\Python\Python311\python.exe",
        "C:\Users\$env:USERNAME\AppData\Local\Programs\Python\Python312\python.exe"
    )

    foreach ($candidate in $candidates) {
        try {
            if ($candidate -eq "py") {
                $null = & py -3 --version 2>$null
                return "py -3"
            }
            else {
                $null = & $candidate --version 2>$null
                return $candidate
            }
        }
        catch {
        }
    }

    throw "Python was not found. Install Python first, then run this installer again."
}

function Invoke-Python {
    param(
        [string]$Command,
        [string[]]$Args
    )

    if ($Command -eq "py -3") {
        & py -3 @Args
    }
    else {
        & $Command @Args
    }
}

function Copy-Directory-Contents {
    param(
        [string]$SourceDir,
        [string]$DestinationDir
    )

    if (-not (Test-Path $SourceDir)) {
        throw "Source directory not found: $SourceDir"
    }

    Ensure-Directory $DestinationDir

    Get-ChildItem -Path $SourceDir -Force | ForEach-Object {
        $targetPath = Join-Path $DestinationDir $_.Name
        if ($_.PSIsContainer) {
            Copy-Item -Path $_.FullName -Destination $targetPath -Recurse -Force
        }
        else {
            Copy-Item -Path $_.FullName -Destination $targetPath -Force
        }
    }
}

function Write-ConfigTemplate {
    param([string]$ConfigPath)

    $json = @"
{
  "app_name": "LedgerLink AI",
  "environment": "sandbox",
  "client_name": "CHANGE_ME",
  "company_code": "CHANGE_ME",
  "data_root": "$InstallRoot\data",
  "documents_inbox": "$InstallRoot\client_drop",
  "exports_dir": "$InstallRoot\exports",
  "logs_dir": "$InstallRoot\logs",
  "dashboard_url": "http://127.0.0.1:8787/",
  "qbo": {
    "environment": "sandbox",
    "minor_version": "75",
    "auto_create_vendors": true
  }
}
"@
    Set-Content -Path $ConfigPath -Value $json -Encoding UTF8
}

function Write-QBOConfigTemplate {
    param([string]$ConfigPath)

    $json = @"
{
  "environment": "sandbox",
  "minor_version": "75",
  "auto_create_vendors": true
}
"@
    Set-Content -Path $ConfigPath -Value $json -Encoding UTF8
}

function Write-RunLauncher {
    param([string]$LauncherPath, [string]$InstallRootPath)

    $content = @"
@echo off
cd /d "$InstallRootPath"
call .venv\Scripts\activate.bat
python scripts\review_dashboard.py
pause
"@
    Set-Content -Path $LauncherPath -Value $content -Encoding ASCII
}

function Write-QueueLauncher {
    param([string]$LauncherPath, [string]$InstallRootPath)

    $content = @"
@echo off
cd /d "$InstallRootPath"
call .venv\Scripts\activate.bat
python scripts\run_openclaw_queue.py --limit 20
pause
"@
    Set-Content -Path $LauncherPath -Value $content -Encoding ASCII
}

function Write-DoctorLauncher {
    param([string]$LauncherPath, [string]$InstallRootPath)

    $content = @"
@echo off
cd /d "$InstallRootPath"
call .venv\Scripts\activate.bat
python -m src.agents.core.software_doctor
pause
"@
    Set-Content -Path $LauncherPath -Value $content -Encoding ASCII
}

function Write-EnvTemplate {
    param([string]$EnvPath)

    $content = @"
QBO_ACCESS_TOKEN=
QBO_REALM_ID=
QBO_ENVIRONMENT=sandbox
"@
    Set-Content -Path $EnvPath -Value $content -Encoding ASCII
}

function Create-DesktopShortcut {
    param(
        [string]$TargetPath,
        [string]$ShortcutName
    )

    $desktop = [Environment]::GetFolderPath("Desktop")
    $shortcutPath = Join-Path $desktop $ShortcutName

    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($shortcutPath)
    $shortcut.TargetPath = $TargetPath
    $shortcut.WorkingDirectory = Split-Path $TargetPath
    $shortcut.Save()
}

Write-Step "LedgerLink AI Client Installer Started"

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "Project root: $ProjectRoot"
Write-Host "Install root: $InstallRoot"

$requiredProjectDirs = @(
    (Join-Path $ProjectRoot "src"),
    (Join-Path $ProjectRoot "scripts"),
    (Join-Path $ProjectRoot "data")
)

foreach ($requiredDir in $requiredProjectDirs) {
    if (-not (Test-Path $requiredDir)) {
        throw "Required project directory not found: $requiredDir"
    }
}

Write-Step "Preparing install directories"

Ensure-Directory $InstallRoot
Ensure-Directory (Join-Path $InstallRoot "data")
Ensure-Directory (Join-Path $InstallRoot "exports")
Ensure-Directory (Join-Path $InstallRoot "logs")
Ensure-Directory (Join-Path $InstallRoot "client_drop")

Remove-IfExists (Join-Path $InstallRoot "src")
Remove-IfExists (Join-Path $InstallRoot "scripts")
Remove-IfExists (Join-Path $InstallRoot "tests")

Write-Step "Copying application files"

Copy-Directory-Contents -SourceDir (Join-Path $ProjectRoot "src") -DestinationDir (Join-Path $InstallRoot "src")
Copy-Directory-Contents -SourceDir (Join-Path $ProjectRoot "scripts") -DestinationDir (Join-Path $InstallRoot "scripts")

if (Test-Path (Join-Path $ProjectRoot "tests")) {
    Copy-Directory-Contents -SourceDir (Join-Path $ProjectRoot "tests") -DestinationDir (Join-Path $InstallRoot "tests")
}

if (Test-Path (Join-Path $ProjectRoot "data\ledgerlink_agent.db")) {
    Copy-Item `
        -Path (Join-Path $ProjectRoot "data\ledgerlink_agent.db") `
        -Destination (Join-Path $InstallRoot "data\ledgerlink_agent.db") `
        -Force
}

if (Test-Path (Join-Path $ProjectRoot "src\agents\data\rules\qbo_mappings.json")) {
    Ensure-Directory (Join-Path $InstallRoot "src\agents\data\rules")
    Copy-Item `
        -Path (Join-Path $ProjectRoot "src\agents\data\rules\qbo_mappings.json") `
        -Destination (Join-Path $InstallRoot "src\agents\data\rules\qbo_mappings.json") `
        -Force
}

Write-Step "Creating config templates"

Write-ConfigTemplate -ConfigPath (Join-Path $InstallRoot "client_config.json")
Write-QBOConfigTemplate -ConfigPath (Join-Path $InstallRoot "data\qbo_config.json")
Write-EnvTemplate -EnvPath (Join-Path $InstallRoot ".env.template")

Write-Step "Creating launcher files"

Write-RunLauncher -LauncherPath (Join-Path $InstallRoot "run_dashboard.bat") -InstallRootPath $InstallRoot
Write-QueueLauncher -LauncherPath (Join-Path $InstallRoot "run_queue.bat") -InstallRootPath $InstallRoot
Write-DoctorLauncher -LauncherPath (Join-Path $InstallRoot "run_doctor.bat") -InstallRootPath $InstallRoot

if (-not $SkipVenv) {
    Write-Step "Preparing Python environment"

    $python = Find-Python -PreferredCommand $PythonCommand
    Write-Host "Using Python command: $python"

    Remove-IfExists (Join-Path $InstallRoot ".venv")
    Invoke-Python -Command $python -Args @("-m", "venv", (Join-Path $InstallRoot ".venv"))

    $venvPython = Join-Path $InstallRoot ".venv\Scripts\python.exe"

    if (-not (Test-Path $venvPython)) {
        throw "Virtual environment was not created correctly."
    }

    if (-not $SkipDependencies) {
        Write-Step "Installing dependencies"

        & $venvPython -m pip install --upgrade pip
        & $venvPython -m pip install requests openpyxl
    }
}
else {
    Write-Step "Skipping virtual environment creation"
}

if (-not $SkipDesktopShortcut) {
    Write-Step "Creating desktop shortcuts"

    Create-DesktopShortcut -TargetPath (Join-Path $InstallRoot "run_dashboard.bat") -ShortcutName "LedgerLink Dashboard.lnk"
    Create-DesktopShortcut -TargetPath (Join-Path $InstallRoot "run_queue.bat") -ShortcutName "LedgerLink Queue.lnk"
    Create-DesktopShortcut -TargetPath (Join-Path $InstallRoot "run_doctor.bat") -ShortcutName "LedgerLink Doctor.lnk"
}

Write-Step "Verifying install structure"

$mustExist = @(
    (Join-Path $InstallRoot "src\agents\core"),
    (Join-Path $InstallRoot "scripts\review_dashboard.py"),
    (Join-Path $InstallRoot "scripts\run_openclaw_queue.py"),
    (Join-Path $InstallRoot "data\ledgerlink_agent.db"),
    (Join-Path $InstallRoot ".venv\Scripts\python.exe")
)

foreach ($path in $mustExist) {
    if (-not (Test-Path $path)) {
        throw "Install verification failed. Missing expected path: $path"
    }
}

Write-Step "Installation completed"

Write-Host "Installed to: $InstallRoot" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Open $InstallRoot\data\qbo_config.json"
Write-Host "2. Keep sandbox or change only when you are production-ready"
Write-Host "3. Set QBO token variables in the session when testing"
Write-Host "4. Drop client documents into: $InstallRoot\client_drop"
Write-Host "5. Run: $InstallRoot\run_doctor.bat"
Write-Host "6. Run: $InstallRoot\run_dashboard.bat"
Write-Host ""
Write-Host "Installer finished successfully." -ForegroundColor Green
