#!/bin/bash

echo "启动自然语言SQL查询工具..."
echo ""

# 设置环境变量跳过Streamlit数据收集
export STREAMLIT_GATHER_USAGE_STATS=false
export STREAMLIT_SERVER_ADDRESS=0.0.0.0  # 监听所有网络接口
export STREAMLIT_SERVER_PORT=8501

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
if ! python3 -c "import streamlit, pandas, openai" &> /dev/null; then
    echo "安装依赖..."
    pip install -r requirements.txt
fi

# 检查.env文件
if [ ! -f "nl_query/.env" ]; then
    echo "警告: 未找到配置文件 nl_query/.env"
    echo "请复制 nl_query/.env.example 为 nl_query/.env 并配置DeepSeek API密钥"
    echo "执行: cp nl_query/.env.example nl_query/.env"
    echo "然后编辑 nl_query/.env 文件添加你的API密钥"
    echo ""
fi

# 启动应用
echo ""
echo "启动应用..."
echo "本地访问地址: http://localhost:8501"
echo "外部访问地址: http://<服务器IP>:8501"
echo "按 Ctrl+C 停止应用"
echo ""

streamlit run nl_query/app.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --theme.base=light \
    --browser.serverAddress=localhost

echo ""
read -p "应用已停止，按回车键退出..."