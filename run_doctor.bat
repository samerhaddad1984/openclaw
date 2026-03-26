@echo off
cd /d "C:\LedgerLinkAI"
call .venv\Scripts\activate.bat
python -m src.agents.core.software_doctor
pause
