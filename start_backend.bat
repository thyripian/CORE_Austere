@echo off
cd /d "%~dp0"
set API_PORT=31495
set DB_PATH=D:\CORE_Scout\military_reports_db.db
venv\Scripts\python.exe run_app_dynamic.py --db "D:\CORE_Scout\military_reports_db.db" --port 31495
pause