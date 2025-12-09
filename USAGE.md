# 独立启动脚本使用说明

## 概述

原统一的 `start_app.py` 文件已被拆分为两个独立的启动脚本，分别用于启动不同的功能：

1. **`start_data_conversion.py`** - 数据转换工具
2. **`start_nl_query.py`** - 自然语言查询系统

## 使用方法

### 1. 数据转换工具

将CSV序时账文件转换为数据库。

```bash
# 基本用法（使用默认参数）
python start_data_conversion.py

# 指定数据目录
python start_data_conversion.py --data-dir ./my_data

# 重置数据库（删除所有表后重新导入）
python start_data_conversion.py --reset-db

# 只验证数据库完整性
python start_data_conversion.py --validate-only

# 显示帮助信息
python start_data_conversion.py --help
```

**参数说明：**
- `--data-dir PATH`: CSV数据目录路径 (默认: `./data`)
- `--db-path PATH`: 数据库文件路径 (默认: `database/accounting.db`)
- `--reset-db`: 重置数据库（删除所有表）
- `--validate-only`: 只验证数据库完整性，不导入数据

### 2. 自然语言查询系统

启动基于Streamlit的Web界面，支持自然语言查询数据库。

```bash
# 基本用法（使用默认参数，自动打开浏览器）
python start_nl_query.py

# 指定端口
python start_nl_query.py --port 8888

# 指定主机地址（允许外部访问）
python start_nl_query.py --host 0.0.0.0

# 不自动打开浏览器
python start_nl_query.py --no-browser

# 显示帮助信息
python start_nl_query.py --help
```

**参数说明：**
- `--port PORT`: Streamlit服务器端口 (默认: `8501`)
- `--host HOST`: Streamlit服务器地址 (默认: `localhost`)
- `--no-browser`: 不自动打开浏览器

## 使用流程

### 首次使用

1. **准备数据**
   - 将CSV序时账文件放入 `data/` 目录

2. **导入数据**
   ```bash
   python start_data_conversion.py
   ```

3. **启动查询系统**
   ```bash
   python start_nl_query.py
   ```

### 日常使用

- **仅使用查询功能**：直接运行 `python start_nl_query.py`
- **更新数据后**：运行 `python start_data_conversion.py` 重新导入
- **重置数据库**：运行 `python start_data_conversion.py --reset-db`

## 注意事项

1. **虚拟环境**：两个脚本都会检查虚拟环境 `venv/` 是否存在
2. **配置文件**：自然语言查询系统需要 `configs/.env` 配置文件
3. **数据库**：查询系统需要 `database/accounting.db` 数据库文件
4. **数据文件**：数据转换工具需要 `data/` 目录中的CSV文件

## 文件结构

```
项目根目录/
├── start_data_conversion.py    # 数据转换启动脚本
├── start_nl_query.py          # 自然语言查询启动脚本
├── data/                      # CSV数据文件目录
├── database/                  # 数据库文件目录
├── configs/                   # 配置文件目录
├── src/                       # 源代码目录
│   ├── data_conversion/       # 数据转换模块
│   └── nl_query/             # 自然语言查询模块
└── venv/                      # Python虚拟环境
```

## 故障排除

### 常见问题

1. **虚拟环境不存在**
   ```
   [错误] 虚拟环境不存在，请先创建虚拟环境
   运行: python -m venv venv
   然后运行: pip install -r requirements.txt
   ```

2. **数据库文件不存在**
   ```
   [警告] 数据库文件不存在: database/accounting.db
   请先运行数据转换工具导入数据
   ```

3. **配置文件不存在**
   ```
   [警告] 未找到配置文件，正在创建...
   [重要] 请编辑 configs/.env 文件，配置您的DeepSeek API密钥
   ```

### 端口冲突

如果端口 `8501` 已被占用，可以指定其他端口：
```bash
python start_nl_query.py --port 8502
```

### 外部访问

如果需要从其他设备访问，可以指定 `0.0.0.0` 作为主机：
```bash
python start_nl_query.py --host 0.0.0.0 --port 8501
```

然后从其他设备访问：`http://<服务器IP>:8501`

## 跨平台说明

### Windows系统
- 直接使用 `python` 命令运行脚本
- 虚拟环境路径：`venv\Scripts\python.exe`

### Linux/Unix系统（包括Ubuntu、macOS）
- 使用 `python3` 命令运行脚本
- 虚拟环境路径：`venv/bin/python`
- 可能需要给脚本添加执行权限：
  ```bash
  chmod +x start_data_conversion.py
  chmod +x start_nl_query.py
  ```
- 然后可以直接运行：
  ```bash
  ./start_nl_query.py
  ```

### 环境准备（Linux/Unix系统）

```bash
# 安装Python3和虚拟环境支持
sudo apt update
sudo apt install python3 python3-pip python3-venv

# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 脚本兼容性

两个启动脚本都设计为跨平台兼容：
- 自动检测操作系统类型
- 使用正确的虚拟环境Python路径
- 处理不同系统的路径分隔符

如果在Linux/Unix系统上遇到权限问题，可以尝试：
```bash
# 使用python3显式运行
python3 start_nl_query.py

# 或者直接使用虚拟环境中的Python
venv/bin/python start_nl_query.py
```