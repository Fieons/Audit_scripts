# Ubuntu系统使用说明

## 环境要求

- Ubuntu 20.04 或更高版本
- Python 3.8 或更高版本
- pip (Python包管理器)

## 快速开始

### 1. 安装系统依赖

```bash
# 更新包列表
sudo apt update

# 安装Python3和pip
sudo apt install python3 python3-pip python3-venv
```

### 2. 使用启动脚本

项目提供了优化的启动脚本，位于项目根目录：

#### 自然语言查询系统（推荐）
```bash
# 给启动脚本执行权限（首次运行）
chmod +x start.sh

# 运行启动脚本
./start.sh
```

这个脚本会：
1. 检查Python3是否安装
2. 创建/激活虚拟环境
3. 检查并安装依赖
4. 检查配置文件
5. 启动Streamlit应用（访问 http://localhost:8501）

#### 旧版启动脚本（不推荐，已重命名）
```bash
# 旧版数据转换工具（功能已整合到新脚本中）
./run_data_conversion.old.sh

# 旧版自然语言查询系统
./start_nl_query.old.sh
```

### 3. 首次运行配置

#### 配置DeepSeek API密钥
自然语言查询系统需要DeepSeek API密钥：

```bash
# 复制配置文件模板（使用新的配置文件位置）
cp configs/.env.example configs/.env

# 编辑配置文件，添加你的API密钥
nano configs/.env
```

在 `configs/.env` 文件中添加：
```
DEEPSEEK_API_KEY=your_api_key_here
```

## 脚本功能说明

### start.sh（推荐）
- 启动自然语言SQL查询系统（包含数据库转换功能）
- 基于Streamlit的Web界面
- 支持自然语言查询数据库
- 自动配置环境变量
- 检查并安装依赖
- 管理虚拟环境
- 提供统一功能菜单

### 旧版脚本（不推荐，已重命名为.old后缀）
#### run_data_conversion.old.sh
- 启动数据转换工具环境
- 提供以下Python脚本：
  - `csv_to_db.py` - 将CSV序时账转换为数据库
  - `data_consistency_checker.py` - 数据一致性检查
  - `data_cleaner.py` - 数据清洗
  - `auxiliary_parser.py` - 辅助项解析
  - `test_basic.py` - 基本测试

#### start_nl_query.old.sh
- 启动自然语言SQL查询系统
- 基于Streamlit的Web界面
- 支持自然语言查询数据库
- 自动配置环境变量

## 手动操作指南

### 1. 创建虚拟环境
```bash
python3 -m venv venv
```

### 2. 激活虚拟环境
```bash
source venv/bin/activate
```

### 3. 安装依赖
```bash
pip install -r requirements.txt
```

### 4. 运行数据转换脚本
```bash
# CSV转数据库
python3 data_conversion/csv_to_db.py

# 数据一致性检查
python3 data_conversion/data_consistency_checker.py

# 数据清洗
python3 data_conversion/data_cleaner.py
```

### 5. 运行自然语言查询
```bash
# 启动Streamlit应用
streamlit run nl_query/app.py
```

## 故障排除

### 1. 权限问题
如果脚本无法执行：
```bash
# 为新脚本添加执行权限
chmod +x scripts/linux/start_nl_query.sh

# 为旧脚本添加执行权限（如果需要）
chmod +x run_data_conversion.old.sh start_nl_query.old.sh
```

### 2. Python版本问题
确保使用Python3：
```bash
# 检查Python版本
python3 --version

# 如果只有python命令，可以创建别名
alias python=python3
```

### 3. 依赖安装失败
如果pip安装失败：
```bash
# 升级pip
pip install --upgrade pip

# 使用国内镜像源
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 4. 虚拟环境问题
如果虚拟环境有问题：
```bash
# 删除现有虚拟环境
rm -rf venv

# 重新创建
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 文件结构

```
审计凭证数据处理与分析系统/
├── scripts/                    # 启动脚本目录
│   ├── linux/                 # Linux启动脚本
│   │   └── start_nl_query.sh  # 自然语言查询启动脚本（推荐）
│   ├── windows/               # Windows启动脚本
│   └── common/                # 通用脚本
├── run_data_conversion.old.sh  # 数据转换启动脚本（Ubuntu，旧版，已重命名）
├── start_nl_query.old.sh       # 自然语言查询启动脚本（Ubuntu，旧版，已重命名）
├── run_data_conversion.old.bat # 数据转换启动脚本（Windows，旧版，已重命名）
├── start_nl_query.old.bat      # 自然语言查询启动脚本（Windows，旧版，已重命名）
├── requirements.txt            # 项目依赖
├── pyproject.toml             # Python项目配置
├── venv/                       # Python虚拟环境
├── configs/                    # 配置文件目录
├── data_conversion/            # 数据转换脚本
├── nl_query/                   # 自然语言查询系统
├── data/                       # 数据文件目录
├── database/                   # 数据库文件目录
├── docs/                       # 项目文档目录
├── tests/                      # 测试文件目录
└── UBUNTU_USAGE.md            # 本使用说明
```

## 注意事项

1. 首次运行脚本时会自动创建虚拟环境并安装依赖
2. 自然语言查询需要配置DeepSeek API密钥
3. 确保有足够的磁盘空间存储数据库文件
4. 建议在运行前备份重要数据
5. 两个脚本可以同时运行，但注意端口冲突（8501端口）

## 技术支持

如有问题，请检查：
1. Python版本是否符合要求
2. 虚拟环境是否激活
3. 依赖是否安装完整
4. 配置文件是否正确
5. 网络连接是否正常