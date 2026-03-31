@echo off
cd /d "C:\OtoCPA"
call .venv\Scripts\activate.bat
python scripts\review_dashboard.py
pause
