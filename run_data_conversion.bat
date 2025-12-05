@echo off
echo 启动审计凭证数据转换工具...
echo.

REM 检查Python
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Python，请确保Python已安装并添加到PATH
    pause
    exit /b 1
)

REM 检查依赖
echo 检查依赖...
python -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo 安装依赖...
    pip install -r requirements.txt
)

echo.
echo 可用脚本:
echo 1. csv_to_db.py - CSV转数据库
echo 2. data_consistency_checker.py - 数据一致性检查
echo 3. data_cleaner.py - 数据清洗
echo 4. auxiliary_parser.py - 辅助项解析
echo 5. test_basic.py - 基本测试
echo.
echo 请运行: python data_conversion\脚本名称.py
echo.
pause