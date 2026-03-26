@echo off
cd /d "C:\LedgerLinkAI"
call .venv\Scripts\activate.bat
python scripts\run_openclaw_queue.py --limit 20
pause
