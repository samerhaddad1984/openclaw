@echo off
chcp 65001 >nul 2>&1
setlocal EnableDelayedExpansion

:: ============================================================
:: OtoCPA — Smart Zero-Touch Installer
:: ============================================================
:: One-click install: admin elevation, Python, packages, DB,
:: firewall, service, autofix, Cloudflare, wizard, shortcuts.
:: Bilingual FR/EN throughout. No technician needed.
:: ============================================================

set "INSTALL_DIR=C:\OtoCPA"
set "LOG_FILE=C:\OtoCPA\install.log"
set "PYTHON_VERSION=3.11.9"
set "PYTHON_URL=https://www.python.org/ftp/python/%PYTHON_VERSION%/python-%PYTHON_VERSION%-amd64.exe"
set "PYTHON_URL_MIRROR1=https://www.python.org/ftp/python/3.11.8/python-3.11.8-amd64.exe"
set "PYTHON_URL_MIRROR2=https://www.python.org/ftp/python/3.12.3/python-3.12.3-amd64.exe"
set "RELEASE_URL=https://releases.otocpa.ai/latest/otocpa-latest.zip"
set "RELEASE_MIRROR1=https://cdn.otocpa.ai/releases/otocpa-latest.zip"
set "RELEASE_MIRROR2=https://github.com/otocpa/releases/releases/latest/download/otocpa-latest.zip"
set "CLOUDFLARED_URL=https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"
set "SUPPORT_EMAIL=support@otocpa.ai"
set "DASH_PORT=8787"
set "PORTAL_PORT=8788"
set "WIZARD_PORT=8790"
set "SERVICE_NAME=OtoCPA"
set "STEP_TOTAL=12"
set "ERRORS=0"

:: ============================================================
:: STEP 0 — Banner
:: ============================================================
title OtoCPA — Installation
color 1F
cls
echo.
echo  ╔══════════════════════════════════════════════════════════╗
echo  ║                                                          ║
echo  ║          OtoCPA — Installation automatique        ║
echo  ║          OtoCPA — Automatic Installation          ║
echo  ║                                                          ║
echo  ╚══════════════════════════════════════════════════════════╝
echo.

:: ============================================================
:: STEP 1 — Self-elevate to Administrator
:: ============================================================
echo  [1/%STEP_TOTAL%] Verification des privileges / Checking privileges...

net session >nul 2>&1
if %errorlevel% equ 0 goto :admin_ok

echo.
echo   Elevation requise — veuillez cliquer Oui quand Windows demande
echo   Elevation required — please click Yes when Windows asks
echo.

:: Try PowerShell elevation
powershell -Command "try { Start-Process -FilePath '%~f0' -Verb RunAs -Wait } catch { exit 1 }" 2>nul
if %errorlevel% equ 0 (
    exit /b 0
)

:: If PowerShell elevation failed, show friendly message
echo.
echo  ┌──────────────────────────────────────────────────────────┐
echo  │  Veuillez cliquer Oui / Please click Yes                 │
echo  │  quand Windows demande la permission.                    │
echo  │  when Windows asks for permission.                       │
echo  │                                                          │
echo  │  Si le probleme persiste / If this persists:             │
echo  │  Clic droit sur ce fichier ^> Executer en administrateur  │
echo  │  Right-click this file ^> Run as administrator            │
echo  └──────────────────────────────────────────────────────────┘
echo.
pause
exit /b 1

:admin_ok
echo   OK — Administrateur / Administrator

:: Create install directory and start logging
if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%"
if not exist "%INSTALL_DIR%\data" mkdir "%INSTALL_DIR%\data"

echo ============================================================ > "%LOG_FILE%"
echo OtoCPA — Smart Installer Log >> "%LOG_FILE%"
echo Date: %date% %time% >> "%LOG_FILE%"
echo Machine: %COMPUTERNAME% >> "%LOG_FILE%"
echo OS: %OS% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

echo. >> "%LOG_FILE%"
echo [STEP 1] Admin elevation: OK >> "%LOG_FILE%"
echo.

:: ============================================================
:: STEP 2 — Detect and fix Python automatically
:: ============================================================
echo  [2/%STEP_TOTAL%] Detection de Python / Detecting Python...
echo [STEP 2] Detecting Python... >> "%LOG_FILE%"

set "PYTHON_CMD="
set "PYTHON_OK=0"

:: Check python in PATH
where python >nul 2>&1
if %errorlevel% equ 0 (
    for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set "PY_VER=%%v"
    echo   Trouve / Found: Python !PY_VER! >> "%LOG_FILE%"
    for /f "tokens=1,2 delims=." %%a in ("!PY_VER!") do (
        if %%a geq 3 if %%b geq 11 (
            set "PYTHON_CMD=python"
            set "PYTHON_OK=1"
        )
    )
)

:: Check python3 in PATH
if !PYTHON_OK! equ 0 (
    where python3 >nul 2>&1
    if !errorlevel! equ 0 (
        for /f "tokens=2 delims= " %%v in ('python3 --version 2^>^&1') do set "PY_VER=%%v"
        for /f "tokens=1,2 delims=." %%a in ("!PY_VER!") do (
            if %%a geq 3 if %%b geq 11 (
                set "PYTHON_CMD=python3"
                set "PYTHON_OK=1"
            )
        )
    )
)

:: Check common install locations
if !PYTHON_OK! equ 0 (
    for %%p in (
        "C:\Program Files\Python311\python.exe"
        "C:\Program Files\Python312\python.exe"
        "C:\Python311\python.exe"
        "C:\Python312\python.exe"
        "%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
        "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
    ) do (
        if exist %%p (
            for /f "tokens=2 delims= " %%v in ('%%p --version 2^>^&1') do set "PY_VER=%%v"
            set "PYTHON_CMD=%%~p"
            set "PYTHON_OK=1"
            echo   Found at %%p >> "%LOG_FILE%"
            goto :python_found
        )
    )
)
:python_found

:: If Python not found or wrong version, download and install
if !PYTHON_OK! equ 0 (
    echo   Python 3.11+ non trouve / not found — telechargement...
    echo   Python not found — downloading installer... >> "%LOG_FILE%"

    set "PY_INSTALLER=%TEMP%\python-installer.exe"
    set "PY_DOWNLOADED=0"

    :: Try main URL (up to 3 attempts)
    for /L %%i in (1,1,3) do (
        if !PY_DOWNLOADED! equ 0 (
            echo   Tentative %%i/3 — %PYTHON_URL% >> "%LOG_FILE%"
            powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; try { Invoke-WebRequest -Uri '%PYTHON_URL%' -OutFile '!PY_INSTALLER!' -UseBasicParsing -TimeoutSec 120; exit 0 } catch { exit 1 }" 2>nul
            if !errorlevel! equ 0 if exist "!PY_INSTALLER!" set "PY_DOWNLOADED=1"
        )
    )

    :: Try mirror 1
    if !PY_DOWNLOADED! equ 0 (
        echo   Trying mirror 1... >> "%LOG_FILE%"
        powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; try { Invoke-WebRequest -Uri '%PYTHON_URL_MIRROR1%' -OutFile '!PY_INSTALLER!' -UseBasicParsing -TimeoutSec 120; exit 0 } catch { exit 1 }" 2>nul
        if !errorlevel! equ 0 if exist "!PY_INSTALLER!" set "PY_DOWNLOADED=1"
    )

    :: Try mirror 2
    if !PY_DOWNLOADED! equ 0 (
        echo   Trying mirror 2... >> "%LOG_FILE%"
        powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; try { Invoke-WebRequest -Uri '%PYTHON_URL_MIRROR2%' -OutFile '!PY_INSTALLER!' -UseBasicParsing -TimeoutSec 120; exit 0 } catch { exit 1 }" 2>nul
        if !errorlevel! equ 0 if exist "!PY_INSTALLER!" set "PY_DOWNLOADED=1"
    )

    if !PY_DOWNLOADED! equ 0 (
        echo.
        echo   ERREUR: Impossible de telecharger Python.
        echo   ERROR: Could not download Python.
        echo   Verifiez votre connexion Internet / Check your Internet connection.
        echo   Contact: %SUPPORT_EMAIL%
        echo [STEP 2] FAILED — could not download Python >> "%LOG_FILE%"
        set /a ERRORS+=1
        goto :step3
    )

    echo   Installation silencieuse de Python / Installing Python silently...
    echo   Installing Python silently... >> "%LOG_FILE%"

    "!PY_INSTALLER!" /quiet InstallAllUsers=1 PrependPath=1 Include_pip=1 Include_test=0
    if !errorlevel! neq 0 (
        echo   WARNING: Python installer returned error !errorlevel! >> "%LOG_FILE%"
    )

    :: Refresh PATH to pick up new Python
    set "PATH=%PATH%;C:\Program Files\Python311;C:\Program Files\Python311\Scripts;C:\Program Files\Python312;C:\Program Files\Python312\Scripts"

    :: Verify installation
    set "PYTHON_CMD="
    for %%p in (
        "C:\Program Files\Python311\python.exe"
        "C:\Program Files\Python312\python.exe"
        "%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
        "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
    ) do (
        if exist %%p (
            set "PYTHON_CMD=%%~p"
            goto :python_installed
        )
    )

    :: Try from PATH
    where python >nul 2>&1
    if !errorlevel! equ 0 (
        set "PYTHON_CMD=python"
        goto :python_installed
    )

    echo   ERREUR: Python installe mais introuvable.
    echo   ERROR: Python installed but not found.
    echo   Redemarrez l'ordinateur et relancez / Restart and run again.
    echo [STEP 2] FAILED — Python installed but not found >> "%LOG_FILE%"
    set /a ERRORS+=1
    goto :step3

    :python_installed
    for /f "tokens=2 delims= " %%v in ('"!PYTHON_CMD!" --version 2^>^&1') do set "PY_VER=%%v"
    echo   Python !PY_VER! installe avec succes >> "%LOG_FILE%"

    :: Clean up installer
    del "!PY_INSTALLER!" >nul 2>&1
)

echo   Python !PY_VER! — OK
echo   Python: !PY_VER! — !PYTHON_CMD! >> "%LOG_FILE%"
echo.

:: ============================================================
:: STEP 3 — Download latest OtoCPA
:: ============================================================
:step3
echo  [3/%STEP_TOTAL%] Telechargement de OtoCPA / Downloading OtoCPA...
echo [STEP 3] Downloading OtoCPA... >> "%LOG_FILE%"

set "RELEASE_ZIP=%TEMP%\otocpa-release.zip"
set "DL_OK=0"

:: Try main URL (3 attempts)
for /L %%i in (1,1,3) do (
    if !DL_OK! equ 0 (
        echo   Tentative %%i/3 — URL principale >> "%LOG_FILE%"
        powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; try { Invoke-WebRequest -Uri '%RELEASE_URL%' -OutFile '%RELEASE_ZIP%' -UseBasicParsing -TimeoutSec 180; exit 0 } catch { Write-Host $_.Exception.Message; exit 1 }" 2>nul
        if !errorlevel! equ 0 if exist "%RELEASE_ZIP%" set "DL_OK=1"
    )
)

:: Try mirror 1
if !DL_OK! equ 0 (
    echo   Trying mirror 1... >> "%LOG_FILE%"
    powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; try { Invoke-WebRequest -Uri '%RELEASE_MIRROR1%' -OutFile '%RELEASE_ZIP%' -UseBasicParsing -TimeoutSec 180; exit 0 } catch { exit 1 }" 2>nul
    if !errorlevel! equ 0 if exist "%RELEASE_ZIP%" set "DL_OK=1"
)

:: Try mirror 2
if !DL_OK! equ 0 (
    echo   Trying mirror 2... >> "%LOG_FILE%"
    powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; try { Invoke-WebRequest -Uri '%RELEASE_MIRROR2%' -OutFile '%RELEASE_ZIP%' -UseBasicParsing -TimeoutSec 180; exit 0 } catch { exit 1 }" 2>nul
    if !errorlevel! equ 0 if exist "%RELEASE_ZIP%" set "DL_OK=1"
)

if !DL_OK! equ 0 (
    echo   WARN: Telechargement echoue — utilisation des fichiers locaux
    echo   WARN: Download failed — using local files if available
    echo [STEP 3] WARNING — download failed, checking local files >> "%LOG_FILE%"

    :: If we're running from within the project directory, copy files locally
    if exist "%~dp0\..\scripts\setup_wizard.py" (
        echo   Copie des fichiers locaux / Copying local files...
        echo   Copying local project files to %INSTALL_DIR% >> "%LOG_FILE%"
        xcopy "%~dp0\.." "%INSTALL_DIR%\" /E /I /Y /Q >nul 2>&1
        set "DL_OK=2"
    )

    if !DL_OK! equ 0 (
        echo.
        echo   ERREUR: Impossible de telecharger OtoCPA.
        echo   ERROR: Could not download OtoCPA.
        echo   Verifiez votre connexion / Check your Internet connection.
        echo   Contact: %SUPPORT_EMAIL%
        echo [STEP 3] FAILED — no download, no local files >> "%LOG_FILE%"
        set /a ERRORS+=1
        goto :step4
    )
) else (
    :: Extract ZIP
    echo   Extraction vers %INSTALL_DIR%...
    echo   Extracting to %INSTALL_DIR%... >> "%LOG_FILE%"
    powershell -Command "try { Expand-Archive -Path '%RELEASE_ZIP%' -DestinationPath '%INSTALL_DIR%' -Force; exit 0 } catch { Write-Host $_.Exception.Message; exit 1 }" 2>nul
    if !errorlevel! neq 0 (
        echo   ERREUR: Extraction echouee / ERROR: Extraction failed
        echo [STEP 3] FAILED — extraction error >> "%LOG_FILE%"
        set /a ERRORS+=1
        goto :step4
    )

    :: Verify extraction
    if not exist "%INSTALL_DIR%\scripts" (
        :: Check if extracted into a subdirectory
        for /d %%d in ("%INSTALL_DIR%\*") do (
            if exist "%%d\scripts" (
                echo   Moving files from subdirectory... >> "%LOG_FILE%"
                xcopy "%%d\*" "%INSTALL_DIR%\" /E /Y /Q >nul 2>&1
                rmdir "%%d" /S /Q >nul 2>&1
                goto :extract_done
            )
        )
    )
    :extract_done

    :: Clean up ZIP
    del "%RELEASE_ZIP%" >nul 2>&1
)

echo   OK
echo [STEP 3] OK >> "%LOG_FILE%"
echo.

:: ============================================================
:: STEP 4 — Install all Python packages
:: ============================================================
:step4
echo  [4/%STEP_TOTAL%] Installation des paquets / Installing packages...
echo [STEP 4] Installing Python packages... >> "%LOG_FILE%"

cd /d "%INSTALL_DIR%"

:: Upgrade pip first
"!PYTHON_CMD!" -m pip install --upgrade pip --quiet --no-warn-script-location >> "%LOG_FILE%" 2>&1

:: Try bulk install first
if exist "%INSTALL_DIR%\requirements.txt" (
    "!PYTHON_CMD!" -m pip install -r requirements.txt --quiet --no-warn-script-location >> "%LOG_FILE%" 2>&1
    if !errorlevel! equ 0 (
        echo   Tous les paquets installes / All packages installed
        echo [STEP 4] Bulk pip install OK >> "%LOG_FILE%"
        goto :pip_done
    )

    :: Bulk failed — install individually
    echo   Installation individuelle des paquets / Installing packages individually...
    echo [STEP 4] Bulk install failed — installing individually >> "%LOG_FILE%"

    for /f "usebackq tokens=* eol=#" %%p in ("%INSTALL_DIR%\requirements.txt") do (
        set "PKG=%%p"
        :: Skip empty lines and comments
        if not "!PKG!"=="" (
            echo n | findstr /r "^#" >nul 2>&1
            "!PYTHON_CMD!" -m pip install !PKG! --quiet --no-warn-script-location >> "%LOG_FILE%" 2>&1
            if !errorlevel! equ 0 (
                echo   OK: !PKG! >> "%LOG_FILE%"
            ) else (
                echo   FAIL: !PKG! >> "%LOG_FILE%"
                set /a ERRORS+=1
            )
        )
    )
) else (
    echo   requirements.txt non trouve — installation des paquets essentiels
    echo [STEP 4] No requirements.txt — installing essentials >> "%LOG_FILE%"
    for %%p in (bcrypt pdfplumber Pillow requests reportlab psutil watchdog) do (
        "!PYTHON_CMD!" -m pip install %%p --quiet --no-warn-script-location >> "%LOG_FILE%" 2>&1
        if !errorlevel! equ 0 (
            echo   OK: %%p >> "%LOG_FILE%"
        ) else (
            echo   FAIL: %%p >> "%LOG_FILE%"
            set /a ERRORS+=1
        )
    )
)
:pip_done
echo   OK
echo [STEP 4] OK >> "%LOG_FILE%"
echo.

:: ============================================================
:: STEP 5 — Initialize database
:: ============================================================
echo  [5/%STEP_TOTAL%] Initialisation de la base de donnees / Initializing database...
echo [STEP 5] Initializing database... >> "%LOG_FILE%"

if not exist "%INSTALL_DIR%\data" mkdir "%INSTALL_DIR%\data"

if exist "%INSTALL_DIR%\scripts\migrate_db.py" (
    "!PYTHON_CMD!" "%INSTALL_DIR%\scripts\migrate_db.py" >> "%LOG_FILE%" 2>&1
    if !errorlevel! equ 0 (
        echo   Base de donnees initialisee / Database initialized
        echo [STEP 5] OK >> "%LOG_FILE%"
    ) else (
        echo   AVERTISSEMENT: La migration a rencontre des problemes
        echo   WARNING: Migration encountered issues
        echo   Les details sont dans / Details in: %LOG_FILE%
        echo [STEP 5] WARNING — migrate_db.py returned error >> "%LOG_FILE%"
    )
) else (
    echo   migrate_db.py non trouve — la BD sera creee au premier lancement
    echo   migrate_db.py not found — DB will be created on first run
    echo [STEP 5] SKIP — migrate_db.py not found >> "%LOG_FILE%"
)
echo   OK
echo.

:: ============================================================
:: STEP 6 — Configure Windows Firewall
:: ============================================================
echo  [6/%STEP_TOTAL%] Configuration du pare-feu / Configuring firewall...
echo [STEP 6] Configuring Windows Firewall... >> "%LOG_FILE%"

:: Remove old rules first (ignore errors if they don't exist)
netsh advfirewall firewall delete rule name="OtoCPA Dashboard" >nul 2>&1
netsh advfirewall firewall delete rule name="OtoCPA Portal" >nul 2>&1
netsh advfirewall firewall delete rule name="OtoCPA Wizard" >nul 2>&1

:: Add new rules
netsh advfirewall firewall add rule name="OtoCPA Dashboard" protocol=TCP dir=in localport=%DASH_PORT% action=allow >nul 2>&1
if !errorlevel! equ 0 (
    echo   Port %DASH_PORT% (Dashboard) — OK >> "%LOG_FILE%"
) else (
    echo   Port %DASH_PORT% — FAILED >> "%LOG_FILE%"
)

netsh advfirewall firewall add rule name="OtoCPA Portal" protocol=TCP dir=in localport=%PORTAL_PORT% action=allow >nul 2>&1
if !errorlevel! equ 0 (
    echo   Port %PORTAL_PORT% (Portal) — OK >> "%LOG_FILE%"
) else (
    echo   Port %PORTAL_PORT% — FAILED >> "%LOG_FILE%"
)

netsh advfirewall firewall add rule name="OtoCPA Wizard" protocol=TCP dir=in localport=%WIZARD_PORT% action=allow >nul 2>&1

echo   Pare-feu configure / Firewall configured
echo [STEP 6] OK >> "%LOG_FILE%"
echo.

:: ============================================================
:: STEP 7 — Install and start Windows Service
:: ============================================================
echo  [7/%STEP_TOTAL%] Installation du service / Installing service...
echo [STEP 7] Installing Windows service... >> "%LOG_FILE%"

:: Stop and remove existing service if present
sc query %SERVICE_NAME% >nul 2>&1
if !errorlevel! equ 0 (
    echo   Arret du service existant / Stopping existing service... >> "%LOG_FILE%"
    sc stop %SERVICE_NAME% >nul 2>&1
    timeout /t 3 /nobreak >nul
    sc delete %SERVICE_NAME% >nul 2>&1
    timeout /t 2 /nobreak >nul
)

:: Use service_wrapper.py if available
if exist "%INSTALL_DIR%\installer\service_wrapper.py" (
    "!PYTHON_CMD!" "%INSTALL_DIR%\installer\service_wrapper.py" install >> "%LOG_FILE%" 2>&1
    if !errorlevel! equ 0 (
        echo   Service installe / Service installed >> "%LOG_FILE%"
        "!PYTHON_CMD!" "%INSTALL_DIR%\installer\service_wrapper.py" start >> "%LOG_FILE%" 2>&1
    ) else (
        echo   service_wrapper.py install failed — using fallback >> "%LOG_FILE%"
        goto :service_fallback
    )
) else (
    :service_fallback
    :: Create a startup batch file
    echo @echo off > "%INSTALL_DIR%\run_otocpa.bat"
    echo cd /d "%INSTALL_DIR%" >> "%INSTALL_DIR%\run_otocpa.bat"
    echo "!PYTHON_CMD!" "%INSTALL_DIR%\scripts\review_dashboard.py" >> "%INSTALL_DIR%\run_otocpa.bat"

    :: Create service via sc
    sc create %SERVICE_NAME% binPath= "cmd.exe /c \"%INSTALL_DIR%\run_otocpa.bat\"" start= auto DisplayName= "OtoCPA Accounting" >nul 2>&1
    sc description %SERVICE_NAME% "OtoCPA — Intelligent Accounting Platform" >nul 2>&1
    sc start %SERVICE_NAME% >nul 2>&1
)

:: Verify service is running
timeout /t 3 /nobreak >nul
sc query %SERVICE_NAME% | findstr /i "RUNNING" >nul 2>&1
if !errorlevel! equ 0 (
    echo   Service demarre / Service running
    echo [STEP 7] OK — service running >> "%LOG_FILE%"
) else (
    echo   AVERTISSEMENT: Le service n'a pas demarre automatiquement
    echo   WARNING: Service did not start automatically
    echo   Le tableau de bord peut etre lance manuellement
    echo   Dashboard can be started manually
    echo [STEP 7] WARNING — service not running >> "%LOG_FILE%"

    :: Start dashboard directly as a fallback
    echo   Lancement direct du tableau de bord / Starting dashboard directly... >> "%LOG_FILE%"
    start "" /MIN "!PYTHON_CMD!" "%INSTALL_DIR%\scripts\review_dashboard.py"
)
echo.

:: ============================================================
:: STEP 8 — Run autofix to verify everything
:: ============================================================
echo  [8/%STEP_TOTAL%] Verification automatique / Running diagnostics...
echo [STEP 8] Running autofix... >> "%LOG_FILE%"

if exist "%INSTALL_DIR%\scripts\autofix.py" (
    "!PYTHON_CMD!" "%INSTALL_DIR%\scripts\autofix.py" --quiet >> "%LOG_FILE%" 2>&1
    if !errorlevel! equ 0 (
        echo   Toutes les verifications passees / All checks passed
        echo [STEP 8] OK — autofix passed >> "%LOG_FILE%"
    ) else (
        echo   Certains problemes detectes — correction automatique tentee
        echo   Some issues detected — automatic fix attempted
        echo [STEP 8] WARNING — autofix found issues (see log) >> "%LOG_FILE%"
    )
) else (
    echo   autofix.py non trouve — verification ignoree
    echo [STEP 8] SKIP — autofix.py not found >> "%LOG_FILE%"
)

:: Register health monitor as scheduled task
echo   Enregistrement du moniteur de sante / Registering health monitor...
echo [STEP 8b] Registering health monitor scheduled task... >> "%LOG_FILE%"
schtasks /create /tn "OtoCPA Health Monitor" /tr "python C:\OtoCPA\scripts\service_health.py" /sc minute /mo 5 /ru SYSTEM /f >> "%LOG_FILE%" 2>&1
if !errorlevel! equ 0 (
    echo   Health monitor registered — runs every 5 minutes
    echo [STEP 8b] OK — health monitor registered >> "%LOG_FILE%"
) else (
    echo   Impossible d'enregistrer le moniteur — non critique
    echo   Could not register health monitor — non-critical
    echo [STEP 8b] WARNING — health monitor registration failed >> "%LOG_FILE%"
)
echo.

:: ============================================================
:: STEP 9 — Install Cloudflare Tunnel
:: ============================================================
echo  [9/%STEP_TOTAL%] Installation de Cloudflare Tunnel...
echo [STEP 9] Installing Cloudflare Tunnel... >> "%LOG_FILE%"

set "CF_INSTALLED=0"

:: Check if already installed
where cloudflared >nul 2>&1
if !errorlevel! equ 0 (
    echo   Cloudflare deja installe / Cloudflare already installed
    set "CF_INSTALLED=1"
    goto :cf_configure
)

:: Try winget first
where winget >nul 2>&1
if !errorlevel! equ 0 (
    echo   Installation via winget... >> "%LOG_FILE%"
    winget install --id Cloudflare.cloudflared --silent --accept-package-agreements --accept-source-agreements >nul 2>&1
    if !errorlevel! equ 0 (
        set "CF_INSTALLED=1"
        echo   Cloudflare installe via winget >> "%LOG_FILE%"
        goto :cf_configure
    )
)

:: Fallback: download directly
echo   Telechargement direct de cloudflared / Downloading cloudflared...
echo   Downloading cloudflared from GitHub... >> "%LOG_FILE%"

set "CF_EXE=%INSTALL_DIR%\tools\cloudflared.exe"
if not exist "%INSTALL_DIR%\tools" mkdir "%INSTALL_DIR%\tools"

powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; try { Invoke-WebRequest -Uri '%CLOUDFLARED_URL%' -OutFile '%CF_EXE%' -UseBasicParsing -TimeoutSec 120; exit 0 } catch { exit 1 }" 2>nul
if !errorlevel! equ 0 if exist "%CF_EXE%" (
    set "CF_INSTALLED=1"
    set "PATH=%PATH%;%INSTALL_DIR%\tools"
    echo   cloudflared downloaded to %CF_EXE% >> "%LOG_FILE%"
)

:cf_configure
if !CF_INSTALLED! equ 1 (
    :: Generate a random tunnel ID
    for /f %%i in ('powershell -Command "[guid]::NewGuid().ToString().Substring(0,8)"') do set "TUNNEL_ID=%%i"

    :: Create tunnel (this requires Cloudflare auth — skip if not authenticated)
    echo   Configuration du tunnel... >> "%LOG_FILE%"

    :: Check if already authenticated
    cloudflared tunnel list >nul 2>&1
    if !errorlevel! equ 0 (
        cloudflared tunnel create "otocpa-!TUNNEL_ID!" >> "%LOG_FILE%" 2>&1
        if !errorlevel! equ 0 (
            echo   Tunnel cree: otocpa-!TUNNEL_ID! >> "%LOG_FILE%"

            :: Install as Windows service
            cloudflared service install >> "%LOG_FILE%" 2>&1

            echo   Tunnel configure / Tunnel configured
        ) else (
            echo   Le tunnel sera configure dans l'assistant
            echo   Tunnel will be configured in the setup wizard
        )
    ) else (
        echo   Cloudflare non authentifie — configuration via l'assistant
        echo   Cloudflare not authenticated — configure via setup wizard
        echo   Cloudflare not authenticated — will configure in wizard >> "%LOG_FILE%"
    )
) else (
    echo   AVERTISSEMENT: cloudflared non installe
    echo   WARNING: cloudflared not installed
    echo   Configurez manuellement ou via l'assistant de configuration
    echo   Configure manually or via the setup wizard
    echo [STEP 9] WARNING — cloudflared not installed >> "%LOG_FILE%"
)
echo [STEP 9] Done >> "%LOG_FILE%"
echo.

:: ============================================================
:: STEP 10 — Open setup wizard
:: ============================================================
echo  [10/%STEP_TOTAL%] Lancement de l'assistant / Starting setup wizard...
echo [STEP 10] Starting setup wizard... >> "%LOG_FILE%"

if exist "%INSTALL_DIR%\scripts\setup_wizard.py" (
    start "" "!PYTHON_CMD!" "%INSTALL_DIR%\scripts\setup_wizard.py"

    :: Wait for wizard to be ready (up to 30 seconds)
    echo   Attente du demarrage / Waiting for wizard to start...
    set "WIZARD_READY=0"
    for /L %%i in (1,1,30) do (
        if !WIZARD_READY! equ 0 (
            powershell -Command "try { $c = New-Object System.Net.Sockets.TcpClient; $c.Connect('127.0.0.1', %WIZARD_PORT%); $c.Close(); exit 0 } catch { exit 1 }" >nul 2>&1
            if !errorlevel! equ 0 (
                set "WIZARD_READY=1"
            ) else (
                timeout /t 1 /nobreak >nul
            )
        )
    )

    if !WIZARD_READY! equ 1 (
        echo   Le navigateur va s'ouvrir automatiquement...
        echo   Browser opening automatically...
        start "" "http://127.0.0.1:%WIZARD_PORT%/"
        echo [STEP 10] OK — wizard started, browser opened >> "%LOG_FILE%"
    ) else (
        echo   L'assistant demarre mais le navigateur n'a pas pu s'ouvrir
        echo   Wizard starting but browser could not open
        echo   Ouvrez manuellement / Open manually: http://127.0.0.1:%WIZARD_PORT%/
        echo [STEP 10] WARNING — wizard port not responding >> "%LOG_FILE%"
    )
) else (
    echo   setup_wizard.py non trouve / not found
    echo [STEP 10] SKIP — setup_wizard.py not found >> "%LOG_FILE%"
)
echo.

:: ============================================================
:: STEP 11 — Create desktop shortcuts
:: ============================================================
echo  [11/%STEP_TOTAL%] Creation des raccourcis / Creating shortcuts...
echo [STEP 11] Creating desktop shortcuts... >> "%LOG_FILE%"

:: Get desktop path
for /f "tokens=*" %%d in ('powershell -Command "[Environment]::GetFolderPath(\"Desktop\")"') do set "DESKTOP=%%d"
if not defined DESKTOP set "DESKTOP=%USERPROFILE%\Desktop"

:: Also get public desktop for all users
set "PUB_DESKTOP=%PUBLIC%\Desktop"

:: Dashboard shortcut
(
echo [InternetShortcut]
echo URL=http://127.0.0.1:%DASH_PORT%/
echo IconIndex=0
) > "%DESKTOP%\OtoCPA Dashboard.url"

:: Portal shortcut
(
echo [InternetShortcut]
echo URL=http://127.0.0.1:%PORTAL_PORT%/
echo IconIndex=0
) > "%DESKTOP%\OtoCPA Portail Client.url"

:: Setup wizard shortcut
(
echo [InternetShortcut]
echo URL=http://127.0.0.1:%WIZARD_PORT%/
echo IconIndex=0
) > "%DESKTOP%\OtoCPA Setup.url"

:: Copy to public desktop too (for all users)
copy "%DESKTOP%\OtoCPA Dashboard.url" "%PUB_DESKTOP%\" >nul 2>&1
copy "%DESKTOP%\OtoCPA Portail Client.url" "%PUB_DESKTOP%\" >nul 2>&1

:: Add to Windows startup (run dashboard on login)
set "STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
(
echo @echo off
echo cd /d "%INSTALL_DIR%"
echo start "" /MIN "!PYTHON_CMD!" "%INSTALL_DIR%\scripts\review_dashboard.py"
) > "%STARTUP%\OtoCPA.bat"

echo   Raccourcis crees / Shortcuts created
echo   Demarrage automatique configure / Auto-start configured
echo [STEP 11] OK >> "%LOG_FILE%"
echo.

:: ============================================================
:: STEP 12 — Show success
:: ============================================================
echo [STEP 12] Installation complete >> "%LOG_FILE%"
echo Installation finished with %ERRORS% error(s) >> "%LOG_FILE%"
echo Finished: %date% %time% >> "%LOG_FILE%"

cls
color 2F
echo.
echo  ╔══════════════════════════════════════════════════════════╗
echo  ║                                                          ║
echo  ║           Installation terminee avec succes!             ║
echo  ║           Installation completed successfully!           ║
echo  ║                                                          ║
echo  ╚══════════════════════════════════════════════════════════╝
echo.

if !ERRORS! gtr 0 (
    color 6F
    echo  ┌──────────────────────────────────────────────────────────┐
    echo  │  AVERTISSEMENT: %ERRORS% probleme(s) detecte(s)                    │
    echo  │  WARNING: %ERRORS% issue(s) detected                               │
    echo  │  Consultez le journal / Check log: %LOG_FILE%            │
    echo  └──────────────────────────────────────────────────────────┘
    echo.
)

echo  ┌──────────────────────────────────────────────────────────┐
echo  │                                                          │
echo  │  Tableau de bord / Dashboard:                            │
echo  │    http://127.0.0.1:%DASH_PORT%/                              │
echo  │                                                          │
echo  │  Portail client / Client Portal:                         │
echo  │    http://127.0.0.1:%PORTAL_PORT%/                              │
echo  │                                                          │
echo  │  Assistant de configuration / Setup Wizard:              │
echo  │    http://127.0.0.1:%WIZARD_PORT%/                              │
echo  │                                                          │
echo  │  Journal / Log: %LOG_FILE%                     │
echo  │  Support: %SUPPORT_EMAIL%                        │
echo  │                                                          │
echo  └──────────────────────────────────────────────────────────┘
echo.
echo  L'assistant de configuration est ouvert dans votre navigateur.
echo  The setup wizard is open in your browser.
echo  Completez les etapes pour terminer la configuration.
echo  Complete the steps to finish configuration.
echo.
echo  ┌──────────────────────────────────────────────────────────┐
echo  │  Des raccourcis ont ete crees sur votre bureau:          │
echo  │  Shortcuts have been created on your desktop:            │
echo  │                                                          │
echo  │    - OtoCPA Dashboard                                │
echo  │    - OtoCPA Portail Client                           │
echo  │    - OtoCPA Setup                                    │
echo  │                                                          │
echo  │  OtoCPA demarrera automatiquement avec Windows.      │
echo  │  OtoCPA will start automatically with Windows.       │
echo  └──────────────────────────────────────────────────────────┘
echo.
echo  Appuyez sur une touche pour fermer / Press any key to close...
pause >nul
