@echo off
echo 启动自然语言SQL查询工具...
echo.

REM 设置环境变量跳过Streamlit数据收集
set STREAMLIT_GATHER_USAGE_STATS=false
set STREAMLIT_SERVER_ADDRESS=localhost
set STREAMLIT_SERVER_PORT=8501

REM 检查Python
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Python，请确保Python已安装并添加到PATH
    pause
    exit /b 1
)

REM 检查依赖
echo 检查依赖...
python -c "import streamlit, pandas, openai" >nul 2>&1
if errorlevel 1 (
    echo 安装依赖...
    pip install -r requirements.txt
)

REM 启动应用
echo.
echo 启动应用...
echo 访问地址: http://localhost:8501
echo 按 Ctrl+C 停止应用
echo.

streamlit run app.py --server.port=8501 --server.address=localhost --theme.base=light --browser.serverAddress=localhost

pause