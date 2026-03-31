@echo off
cd /d "C:\OtoCPA"
call .venv\Scripts\activate.bat
python scripts\run_openclaw_queue.py --limit 20
pause
