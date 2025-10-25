@echo off
setlocal

REM ====== 1) Gå till projektroten (mappen där denna .bat ligger) ======
cd /d "%~dp0"

REM ====== 2) Aktivera virtuell miljö om den finns (.venv) ======
if exist ".venv\Scripts\activate.bat" (
  call ".venv\Scripts\activate.bat"
)

REM ====== 3) Kör ETL (main.py) ======
REM .env läses av koden, loggar skrivs enligt LOG_DIR/LOG_FILE
python main.py

REM ====== 4) Avslut ======
endlocal
