@echo off
setlocal enabledelayedexpansion
echo ========================================
echo Installing Ticket Audit Tool dependencies...
echo ========================================
cd /d %~dp0

REM Detect Python
py -3 --version >nul 2>&1
if %errorlevel% equ 0 (
    set PYTHON_CMD=py -3
) else (
    python --version >nul 2>&1
    if %errorlevel% equ 0 (
        set PYTHON_CMD=python
    ) else (
        echo Python is not installed. Please install Python 3 from https://www.python.org/
        pause
        exit /b 1
    )
)

REM Create venv if not exists
if not exist ".venv" (
    echo Creating virtual environment...
    %PYTHON_CMD% -m venv .venv
)

REM Activate venv
call .venv\Scripts\activate.bat

REM Upgrade pip
python -m pip install --upgrade pip

REM Install requirements
if exist requirements.txt (
    echo Installing dependencies...
    pip install -r requirements.txt
) else (
    echo requirements.txt not found!
    pause
    exit /b 1
)

echo ========================================
echo Installation complete!
echo TQA will launch Automatically in browser wait few seconds if not then double-click Run_TQA.bat to start the app.
echo ========================================
pause
