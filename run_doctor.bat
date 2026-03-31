@echo off
cd /d "C:\OtoCPA"
call .venv\Scripts\activate.bat
python -m src.agents.core.software_doctor
pause
