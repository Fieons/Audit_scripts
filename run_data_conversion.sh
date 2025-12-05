#!/bin/bash

echo "启动审计凭证数据转换工具..."
echo ""

# 检查Python
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到Python3，请确保Python3已安装"
    echo "在Ubuntu上可以使用: sudo apt install python3 python3-pip"
    exit 1
fi

# 检查虚拟环境
if [ ! -d "venv" ]; then
    echo "警告: 未找到虚拟环境，正在创建..."
    python3 -m venv venv
    echo "虚拟环境创建完成"
fi

# 激活虚拟环境
echo "激活虚拟环境..."
source venv/bin/activate

# 检查依赖
echo "检查依赖..."
if ! python3 -c "import pandas" &> /dev/null; then
    echo "安装依赖..."
    pip install -r requirements.txt
fi

echo ""
echo "开始执行CSV到数据库转换..."
echo "========================================"

# 运行CSV到数据库转换脚本
python3 data_conversion/csv_to_db.py

echo ""
echo "========================================"
echo "转换完成！"
echo ""
echo "数据库文件位置: database/accounting.db"
echo "数据目录: data/"
echo ""
read -p "按回车键退出..."