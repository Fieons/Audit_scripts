@echo off
chcp 65001 >nul
echo Starting Audit Voucher Data Conversion Tool...
echo.

REM 检查Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found. Please ensure Python 3.x is installed and added to PATH
    echo Download from: https://www.python.org/downloads/
    pause
    exit /b 1
)

REM Check Python version is 3.x
python -c "import sys; exit(0) if sys.version_info.major == 3 else exit(1)" >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python 3.x required. Current version does not meet requirement
    python --version
    pause
    exit /b 1
)

REM Check virtual environment
if not exist "venv" (
    echo WARNING: Virtual environment not found, creating...
    python -m venv venv
    echo Virtual environment created
)

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Check dependencies
echo Checking dependencies...
python -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo Installing dependencies...
    pip install -r requirements.txt
)

echo.
echo Starting CSV to database conversion...
echo ========================================

REM 运行CSV到数据库转换脚本
python data_conversion\csv_to_db.py

echo.
echo ========================================
echo Conversion completed!
echo.
echo Database file: database\accounting.db
echo Data directory: data\
echo.
pause