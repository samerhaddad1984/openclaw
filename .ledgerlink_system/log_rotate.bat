@echo off
setlocal
cd /d "%~dp0\.."
powershell -NoProfile -ExecutionPolicy Bypass -File ".\.otocpa_system\log_rotate.ps1"
exit /b %errorlevel%
