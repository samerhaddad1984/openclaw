@echo off
setlocal EnableDelayedExpansion

:: ============================================================
:: OtoCPA - Windows Installer
:: ============================================================

:: Request administrator elevation
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo Requesting administrator privileges...
    powershell -Command "Start-Process '%~f0' -Verb RunAs"
    exit /b
)

:: Set install directory
set "INSTALL_DIR=%~dp0"
set "LOG_FILE=C:\OtoCPA\install.log"

:: Create log directory
if not exist "C:\OtoCPA" mkdir "C:\OtoCPA"

:: Start logging
echo ============================================================ > "%LOG_FILE%"
echo OtoCPA - Installation Log >> "%LOG_FILE%"
echo Date: %date% %time% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

echo.
echo ============================================================
echo   OtoCPA - Installation en cours / Installing...
echo ============================================================
echo.

:: -----------------------------------------------------------
:: Step 1: Check Python 3.11+
:: -----------------------------------------------------------
echo [1/8] Checking Python... >> "%LOG_FILE%"
echo [1/8] Verification de Python / Checking Python...

set "PYTHON_CMD="
where python >nul 2>&1
if %errorlevel% equ 0 (
    for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set "PY_VER=%%v"
    for /f "tokens=1,2 delims=." %%a in ("!PY_VER!") do (
        if %%a geq 3 if %%b geq 11 set "PYTHON_CMD=python"
    )
)

if not defined PYTHON_CMD (
    where python3 >nul 2>&1
    if %errorlevel% equ 0 (
        for /f "tokens=2 delims= " %%v in ('python3 --version 2^>^&1') do set "PY_VER=%%v"
        for /f "tokens=1,2 delims=." %%a in ("!PY_VER!") do (
            if %%a geq 3 if %%b geq 11 set "PYTHON_CMD=python3"
        )
    )
)

if not defined PYTHON_CMD (
    echo Python 3.11+ not found. Downloading... >> "%LOG_FILE%"
    echo   Python 3.11+ non trouve / Python 3.11+ not found.
    echo   Telechargement en cours / Downloading...
    powershell -Command "Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe' -OutFile '%TEMP%\python-installer.exe'"
    echo   Installation de Python / Installing Python...
    "%TEMP%\python-installer.exe" /quiet InstallAllUsers=1 PrependPath=1 Include_pip=1
    set "PYTHON_CMD=python"
    :: Refresh PATH
    set "PATH=%PATH%;C:\Program Files\Python311;C:\Program Files\Python311\Scripts"
    echo Python installed. >> "%LOG_FILE%"
)

echo   Python: !PY_VER! >> "%LOG_FILE%"
echo   Python OK: !PY_VER!
echo.

:: -----------------------------------------------------------
:: Step 2: Install dependencies
:: -----------------------------------------------------------
echo [2/8] Installing dependencies... >> "%LOG_FILE%"
echo [2/8] Installation des dependances / Installing dependencies...
cd /d "%INSTALL_DIR%"
!PYTHON_CMD! -m pip install -r requirements.txt --quiet >> "%LOG_FILE%" 2>&1
if %errorlevel% neq 0 (
    echo   ERREUR: pip install a echoue / ERROR: pip install failed >> "%LOG_FILE%"
    echo   ERREUR / ERROR: pip install failed. See %LOG_FILE%
)
echo   Dependencies installed. >> "%LOG_FILE%"
echo   OK
echo.

:: -----------------------------------------------------------
:: Step 3: Database migration
:: -----------------------------------------------------------
echo [3/8] Database migration... >> "%LOG_FILE%"
echo [3/8] Migration de la base de donnees / Database migration...
!PYTHON_CMD! scripts/migrate_db.py >> "%LOG_FILE%" 2>&1
echo   OK
echo.

:: -----------------------------------------------------------
:: Step 4: Install Windows service
:: -----------------------------------------------------------
echo [4/8] Installing service... >> "%LOG_FILE%"
echo [4/8] Installation du service / Installing service...
!PYTHON_CMD! installer/service_wrapper.py install >> "%LOG_FILE%" 2>&1
echo   OK
echo.

:: -----------------------------------------------------------
:: Step 5: Start service
:: -----------------------------------------------------------
echo [5/8] Starting service... >> "%LOG_FILE%"
echo [5/8] Demarrage du service / Starting service...
!PYTHON_CMD! installer/service_wrapper.py start >> "%LOG_FILE%" 2>&1
echo   OK
echo.

:: -----------------------------------------------------------
:: Step 6: Create desktop shortcuts
:: -----------------------------------------------------------
echo [6/8] Creating shortcuts... >> "%LOG_FILE%"
echo [6/8] Creation des raccourcis / Creating shortcuts...

:: OtoCPA shortcut
powershell -Command "$ws = New-Object -ComObject WScript.Shell; $s = $ws.CreateShortcut([Environment]::GetFolderPath('Desktop') + '\OtoCPA.url'); $s.TargetPath = 'http://127.0.0.1:8787/'; $s.Save()"

:: Setup Wizard shortcut
powershell -Command "$ws = New-Object -ComObject WScript.Shell; $s = $ws.CreateShortcut([Environment]::GetFolderPath('Desktop') + '\OtoCPA Setup.url'); $s.TargetPath = 'http://127.0.0.1:8790/'; $s.Save()"

echo   OK
echo.

:: -----------------------------------------------------------
:: Step 7: Start setup wizard
:: -----------------------------------------------------------
echo [7/8] Starting setup wizard... >> "%LOG_FILE%"
echo [7/8] Demarrage de l'assistant / Starting setup wizard...
start "" !PYTHON_CMD! scripts/setup_wizard.py
timeout /t 3 /nobreak >nul

:: -----------------------------------------------------------
:: Step 8: Open browser
:: -----------------------------------------------------------
echo [8/8] Opening browser... >> "%LOG_FILE%"
echo [8/8] Ouverture du navigateur / Opening browser...
start "" "http://127.0.0.1:8790/"
echo.

:: -----------------------------------------------------------
:: Done
:: -----------------------------------------------------------
echo ============================================================
echo   Installation terminee! / Installation complete!
echo ============================================================
echo.
echo   OtoCPA:    http://127.0.0.1:8787/
echo   Setup Wizard:     http://127.0.0.1:8790/
echo.
echo   Log: %LOG_FILE%
echo ============================================================
echo Installation complete: %date% %time% >> "%LOG_FILE%"

pause
