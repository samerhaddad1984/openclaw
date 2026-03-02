@echo off
setlocal
cd /d "%~dp0\.."
if exist ".\.venv\Scripts\python.exe" (
  ".\.venv\Scripts\python.exe" ".\.ledgerlink_system\doctor.py"
  exit /b %errorlevel%
) else (
  echo [FAIL] Missing .venv. Run setup_portable.bat first.
  exit /b 2
)
