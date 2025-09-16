@echo off
title Data Wrangler - by Mohamad W.
color 0A

echo.
echo ========================================
echo    Data Wrangler Desktop Application
echo           Created by Mohamad W.
echo ========================================
echo.

REM Check if Python is installed
echo [1/4] Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo ❌ ERROR: Python is not installed or not in PATH
    echo.
    echo Please install Python 3.11+ from: https://python.org
    echo Make sure to check "Add Python to PATH" during installation
    echo.
    echo Press any key to open Python download page...
    pause >nul
    start https://python.org/downloads/
    exit /b 1
) else (
    echo ✅ Python found
)

REM Check if we're in the right directory
echo [2/4] Checking application files...
if not exist "desktop_app.py" (
    echo.
    echo ❌ ERROR: desktop_app.py not found
    echo Please run this batch file from the Data Wrangler directory
    echo.
    pause
    exit /b 1
) else (
    echo ✅ Application files found
)

REM Check if dependencies are installed
echo [3/4] Checking dependencies...
python -c "import pandas, chardet, openpyxl, pyarrow, pydantic, PIL" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages...
    echo This may take a few minutes on first run...
    echo.
    pip install -r requirements.txt
    if errorlevel 1 (
        echo.
        echo ❌ ERROR: Failed to install dependencies
        echo.
        echo Possible solutions:
        echo 1. Check your internet connection
        echo 2. Try running as administrator
        echo 3. Update pip: python -m pip install --upgrade pip
        echo.
        pause
        exit /b 1
    ) else (
        echo ✅ Dependencies installed successfully
    )
) else (
    echo ✅ All dependencies found
)

REM Create icon if it doesn't exist
if not exist "data_wrangler.ico" (
    echo Creating application icon...
    python create_icon.py >nul 2>&1
)

echo [4/4] Starting Data Wrangler...
echo.
echo 🚀 Launching desktop application...
echo.

REM Run the desktop application
python desktop_app.py

REM If the application exits, show a message
echo.
echo Data Wrangler has closed.
echo Thank you for using Data Wrangler! 🔧
echo.
pause
