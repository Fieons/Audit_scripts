@echo off
chcp 65001 >nul
echo Starting Natural Language SQL Query Tool...
echo.

REM 设置环境变量跳过Streamlit数据收集
set STREAMLIT_GATHER_USAGE_STATS=false
set STREAMLIT_SERVER_ADDRESS=0.0.0.0
set STREAMLIT_SERVER_PORT=8501

REM Check Python
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
python -c "import streamlit, pandas, openai" >nul 2>&1
if errorlevel 1 (
    echo Installing dependencies...
    pip install -r requirements.txt
)

REM Check .env file
if not exist "nl_query\.env" (
    echo WARNING: Configuration file nl_query\.env not found
    echo Please copy nl_query\.env.example to nl_query\.env and configure DeepSeek API key
    echo Command: copy nl_query\.env.example nl_query\.env
    echo Then edit nl_query\.env file to add your API key
    echo.
)

REM Start application
echo.
echo Starting application...
echo Local access: http://localhost:8501
echo External access: http://<server-ip>:8501
echo Press Ctrl+C to stop application
echo.

streamlit run nl_query/app.py ^
    --server.port=8501 ^
    --server.address=0.0.0.0 ^
    --theme.base=light ^
    --browser.serverAddress=localhost

echo.
echo Application stopped. Press any key to exit...
pause