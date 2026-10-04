@echo off
cd /d %~dp0

taskkill /IM python.exe /F >nul 2>&1
taskkill /IM python3.exe /F >nul 2>&1

start "NSE Dashboard Server" /min cmd /k "title NSE Dashboard Server && cd /d %~dp0 && .venv\Scripts\python.exe -m uvicorn main:app --host :: --port 8000"

:wait
timeout /t 1 /nobreak >nul
curl -s -o nul http://localhost:8000/api/status >nul 2>&1
if errorlevel 1 goto wait

start http://localhost:8000
