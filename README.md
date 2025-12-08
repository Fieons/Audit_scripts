# 审计凭证数据处理与分析系统

一个完整的审计凭证数据处理与分析系统，包含数据库转换和自然语言查询两大核心功能。

## 项目概述

本项目旨在将CSV格式的审计凭证数据转换为科学的关系型数据库结构，并提供自然语言查询接口，便于进行会计层面的分析和审计查询。

### 核心功能

1. **数据库转换模块** - 将CSV序时账转换为标准化的SQLite数据库
2. **自然语言查询系统** - 基于DeepSeek API的自然语言到SQL查询工具

## 项目结构

```
审计凭证数据处理与分析系统/
├── src/                        # 源代码包
│   ├── nl_query/              # 自然语言查询系统
│   │   ├── clients/           # LLM客户端模块
│   │   ├── app.py             # Streamlit主应用
│   │   ├── config.py          # 配置管理
│   │   ├── database.py        # 数据库管理
│   │   ├── generator.py       # SQL生成器
│   │   └── utils.py           # 工具函数
│   └── data_conversion/       # 数据转换模块
│       ├── converter.py       # 主转换器
│       ├── schema.py          # 数据库模式
│       ├── cleaner.py         # 数据清洗
│       ├── parser.py          # 辅助项解析
│       └── validator.py       # 数据验证
├── data/                       # 原始CSV数据文件
├── database/                   # 生成的SQLite数据库
├── docs/                       # 项目文档
├── configs/                    # 配置文件目录
├── tests/                      # 测试文件目录
├── venv/                       # Python虚拟环境
├── start.bat                   # Windows启动脚本（推荐）
├── start.sh                    # Linux启动脚本（推荐）
├── pyproject.toml             # Python项目配置
├── requirements.txt            # 统一依赖包列表
├── .pre-commit-config.yaml    # 预提交钩子配置
└── README.md                   # 本项目根目录说明
```

## 快速开始

### 环境准备

1. **创建虚拟环境**（如果尚未创建）：
```bash
# Windows
python -m venv venv

# Linux/Mac
python3 -m venv venv
```

2. **激活虚拟环境**：
```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. **安装依赖**：
```bash
pip install -r requirements.txt
```

### 使用启动脚本（推荐）

项目根目录提供了统一的启动脚本：

#### Windows:
```bash
# 在项目根目录运行
start.bat
```

#### Linux/Mac:
```bash
# 在项目根目录运行
chmod +x start.sh  # 首次运行需要给执行权限
./start.sh
```

启动脚本提供以下功能：
1. **自然语言查询系统** - Streamlit Web界面
2. **数据库转换工具** - 命令行工具
3. **运行测试** - 执行项目测试
4. **检查依赖** - 查看已安装的包

### 手动启动方式

#### 1. 自然语言查询系统
```bash
# 激活虚拟环境后
streamlit run src/nl_query/app.py
```

#### 2. 数据库转换模块
```bash
# 查看帮助
python -m src.data_conversion.converter --help

# 运行数据转换
python -m src.data_conversion.converter --reset-db

# 验证数据一致性
python -m src.data_conversion.validator
```
```

### 2. 自然语言查询系统

```bash
# 配置环境变量（使用新的配置文件位置）
cp configs/.env.example configs/.env
# 编辑 configs/.env 文件，设置API密钥

# 启动Web应用（推荐使用启动脚本）
# Windows:
start.bat

# Linux/Mac:
chmod +x start.sh
./start.sh

# 或手动启动：
streamlit run src/nl_query/app.py
```

## 详细文档

### 数据库转换模块
- [技术方案文档](docs/方案文档_v2.md) - 详细的技术方案和数据库设计
- [开发说明文档](docs/开发说明文档.md) - 开发进度和功能说明
- [数据查询指引](docs/数据查询指引.md) - SQL查询示例和使用指南
- [模块说明文档](docs/database_transformation_README.md) - 数据库转换模块详细说明

### 自然语言查询系统
- [系统说明文档](nl_query/README.md) - 自然语言查询系统使用说明
- [聊天功能说明](nl_query/CHAT_FEATURE_README.md) - 聊天功能增强版使用说明

## 数据统计

- **公司数量**: 5家
- **凭证数量**: 26,082个
- **凭证明细**: 140,854条
- **辅助项**: 293,915个
- **时间范围**: 2023-2025年

## 技术栈

### 数据库转换模块
- Python 3.8+
- pandas >= 1.3.0
- SQLite3

### 自然语言查询系统
- Streamlit >= 1.28.0
- DeepSeek API
- OpenAI Python SDK
- python-dotenv

## 主要特性

### 数据库转换模块
- ✅ 完整的数据清洗和验证
- ✅ 辅助项智能解析
- ✅ 数据库schema规范化设计
- ✅ 数据一致性严格检验
- ✅ 支持多种CSV格式兼容

### 自然语言查询系统
- ✅ 中文自然语言查询
- ✅ 智能SQL生成
- ✅ 查询结果可视化
- ✅ 查询历史记录
- ✅ 安全SQL执行验证

## 开发状态

### 已完成
- [x] 数据库转换核心功能（第一阶段）
- [x] 数据一致性检验（第二阶段）
- [x] 自然语言查询系统（独立模块）

### 进行中/规划中
- [ ] 数据质量报告生成
- [ ] 查询性能优化
- [ ] 更多可视化分析功能

## 许可证

本项目基于MIT许可证开源。

## 联系方式

如有问题或建议，请通过项目Issue反馈。