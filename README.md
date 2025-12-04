# 审计凭证数据处理与分析系统

一个完整的审计凭证数据处理与分析系统，包含数据库转换和自然语言查询两大核心功能。

## 项目概述

本项目旨在将CSV格式的审计凭证数据转换为科学的关系型数据库结构，并提供自然语言查询接口，便于进行会计层面的分析和审计查询。

### 核心功能

1. **数据库转换模块** - 将CSV序时账转换为标准化的SQLite数据库
2. **自然语言查询系统** - 基于DeepSeek API的自然语言到SQL查询工具

## 项目结构

```
Audit_scripts/
├── database_transformation/     # 数据库转换主项目
│   ├── data/                   # 原始CSV数据文件
│   ├── database/               # 生成的SQLite数据库
│   ├── docs/                   # 项目文档
│   ├── nl_query/               # 自然语言查询系统
│   ├── scripts/                # 转换脚本
│   ├── requirements.txt        # 依赖包列表
│   └── README.md              # 项目说明文档
└── README.md                  # 本项目根目录说明
```

## 快速开始

### 安装依赖

项目包含两个独立模块，各有自己的依赖：

#### 选项1：安装所有依赖（推荐）
```bash
# 安装数据库转换模块依赖
pip install -r database_transformation/requirements.txt

# 安装自然语言查询系统依赖
pip install -r database_transformation/nl_query/requirements.txt
```

#### 选项2：按需安装
- 仅使用数据库转换模块：`pip install -r database_transformation/requirements.txt`
- 仅使用自然语言查询系统：`pip install -r database_transformation/nl_query/requirements.txt`

### 1. 数据库转换模块

```bash
cd database_transformation/scripts

# 运行数据转换
python csv_to_db.py --reset-db

# 验证数据一致性
python data_consistency_checker.py
```

### 2. 自然语言查询系统

```bash
cd database_transformation/nl_query

# 配置环境变量
cp .env.example .env
# 编辑.env文件，设置API密钥

# 启动Web应用
streamlit run app.py
```

## 详细文档

### 数据库转换模块
- [技术方案文档](database_transformation/docs/方案文档_v2.md) - 详细的技术方案和数据库设计
- [开发说明文档](database_transformation/docs/开发说明文档.md) - 开发进度和功能说明
- [数据查询指引](database_transformation/docs/数据查询指引.md) - SQL查询示例和使用指南

### 自然语言查询系统
- [系统说明文档](database_transformation/nl_query/README.md) - 自然语言查询系统使用说明

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