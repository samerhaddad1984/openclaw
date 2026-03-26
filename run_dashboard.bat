@echo off
cd /d "C:\LedgerLinkAI"
call .venv\Scripts\activate.bat
python scripts\review_dashboard.py
pause
