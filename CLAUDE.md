# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是一个审计脚本项目，主要功能是将CSV格式的序时账数据完整无损地转换为SQLite数据库，并进行数据质量验证和平衡检查。项目包含数据转换、验证、分析和自然语言查询四个主要模块。

**核心功能**：
1. **数据转换**：将CSV格式的序时账数据完整无损地转换为SQLite数据库
2. **数据验证**：验证转换后的数据与原始数据的一致性
3. **数据分析**：零金额分析、借贷平衡分析等
4. **自然语言查询**：使用DeepSeek API将自然语言查询转换为SQL查询并执行

## 项目结构

```
├── analyze_zero_amounts.py              # 零金额分析脚本
├── validate_transformation.py           # 数据转换验证脚本
├── nl_query.py                          # 自然语言查询主程序
├── database_transformation/             # 主转换项目目录
│   ├── transform_journal.py            # 主转换脚本（完整无损转换）
│   ├── config.py                       # 配置文件
│   ├── requirements.txt                # Python依赖包
│   ├── accounting.db                   # 最终生成的SQLite数据库
│   ├── data/                           # 原始数据目录
│   │   └── 序时账2025.1-9.csv        # 原始序时账数据
│   ├── output/                         # 输出文件目录
│   ├── sql/                            # SQL脚本目录
│   │   └── create_tables.sql          # 数据库建表脚本
│   ├── scripts/                        # 核心脚本目录
│   └── docs/                           # 文档目录
│       └── 项目状态报告.md            # 项目状态报告
├── natural_language_query/             # 自然语言查询系统
│   ├── config.yaml                     # 配置文件
│   ├── requirements.txt                # Python依赖包
│   └── src/                            # 源代码目录
│       ├── __init__.py
│       ├── api/
│       │   ├── __init__.py
│       │   └── deepseek_client.py    # DeepSeek API客户端
│       ├── database/
│       │   ├── __init__.py
│       │   ├── connection.py         # 数据库连接管理
│       │   ├── query_executor.py     # 查询执行器
│       │   └── schema.py             # 数据库模式管理
│       ├── sql_generator/
│       │   ├── __init__.py
│       │   └── core.py               # SQL生成器核心（纯LLM）
│       ├── query_processor.py        # 查询处理器
│       └── utils/
│           ├── __init__.py
│           ├── config_loader.py      # 配置加载器
│           ├── logger.py             # 日志系统
│           ├── security.py           # 安全模块（可选）
│           └── string_utils.py       # 安全的字符串处理工具
└── venv/                               # Python虚拟环境
```

## 数据库结构

转换后的SQLite数据库包含以下表：

1. **accounts（科目表）** - 189条记录
   - `id`: 主键
   - `code`: 科目编码
   - `name`: 科目名称
   - `type`: 科目类型（资产、负债、所有者权益、成本、费用）
   - `level`: 科目层级

2. **vouchers（凭证表）** - 491条记录
   - `id`: 主键
   - `voucher_no`: 凭证号
   - `date`: 凭证日期
   - `type`: 凭证类型（银付、银收、现付、现收、转）
   - `description`: 凭证摘要
   - `is_balanced`: 是否平衡标识

3. **transactions（交易表）** - 17,211条记录
   - `id`: 主键
   - `voucher_id`: 凭证ID
   - `line_no`: 分录号
   - `account_code`: 科目编码
   - `date`: 交易日期
   - `debit_credit`: 借贷标识（D/C）
   - `amount`: 金额（保持原始符号，包括负金额）
   - `description`: 交易描述
   - `voucher_no`: 凭证号

4. **balance_adjustments（平衡调整表）** - 0条记录（当前数据完全平衡）

## 开发环境

项目使用Python 3.12.10虚拟环境：

```bash
# 激活虚拟环境
venv\Scripts\activate.bat

# 或直接使用虚拟环境中的Python
venv\Scripts\python.exe script_name.py

# 安装依赖
venv\Scripts\python.exe -m pip install -r database_transformation/requirements.txt
venv\Scripts\python.exe -m pip install -r natural_language_query/requirements.txt
```

## 核心命令

```bash
# 数据转换
cd database_transformation
venv\Scripts\python.exe transform_journal.py

# 数据验证
venv\Scripts\python.exe validate_transformation.py

# 自然语言查询
venv\Scripts\python.exe nl_query.py

# 单次查询
echo "查询科目总数" | venv\Scripts\python.exe nl_query.py
```

## 核心原则

1. **完整无损转换**：保持原始数据的完整性
2. **数据一致性**：源数据与数据库完全一致
3. **借贷平衡**：保持原始借贷平衡（差额0.00）

## 注意事项

1. **API密钥**：自然语言查询需要DeepSeek API密钥
2. **数据准确性**：所有分析基于数据库准确数据
3. **数据备份**：定期备份数据库文件