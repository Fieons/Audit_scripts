# 审计凭证数据库转换系统

将CSV格式的序时账（审计凭证数据）转换为科学的关系型数据库结构，便于进行会计层面的分析和审计查询。

## 项目状态

**当前阶段：第二阶段完成**（数据一致性检验）

## 核心功能

### ✅ 已完成功能
1. **数据库Schema设计** - 完整的8张表结构设计（符合会计规范）
2. **数据清洗模块** - CSV数据清洗、金额格式化、公司信息提取
3. **辅助项解析模块** - 智能解析`【类型：值】`格式的辅助项
4. **主转换脚本** - 完整的CSV到数据库转换流水线
5. **数据一致性检验** - 严格的源数据与数据库数据一致性验证

### 📋 规划中功能
1. **数据质量报告** - 数据完整性、准确性、一致性报告
2. **监控和告警** - 转换过程监控、异常检测
3. **性能优化** - 大数据量处理优化

## 项目结构

```
database_transformation/
├── data/                    # 原始CSV数据（13个文件，5家公司2023-2025年数据）
├── database/                # 生成的SQLite数据库
│   └── accounting.db       # 数据库文件（140,854条记录）
├── docs/                    # 项目文档
│   ├── 方案文档_v2.md      # 详细技术方案文档
│   ├── 开发说明文档.md     # 开发进度和功能说明
│   └── 数据查询指引.md     # SQL查询示例和使用指南
├── nl_query/                # 自然语言查询系统（独立模块）
├── scripts/                 # 核心转换脚本
│   ├── csv_to_db.py        # 主转换脚本（已完成）
│   ├── data_cleaner.py     # 数据清洗模块（已完成）
│   ├── db_schema.py        # 数据库schema定义（已完成）
│   ├── auxiliary_parser.py # 辅助项解析模块（已完成）
│   ├── test_basic.py       # 基本功能测试脚本（已完成）
│   └── data_consistency_checker.py  # 数据一致性检验脚本（第二阶段完成）
├── requirements.txt         # 依赖包列表（仅pandas）
└── README.md               # 本说明文档
```

## 数据库设计

### 表结构（8张表）
1. `companies` - 公司表
2. `account_books` - 账簿表
3. `account_subjects` - 会计科目表
4. `vouchers` - 凭证主表
5. `voucher_details` - 凭证明细表
6. `auxiliary_items` - 辅助项解析表
7. `projects` - 项目表（可选）
8. `suppliers_customers` - 客商表（可选）

### 数据统计
- **公司数量**: 5家
- **凭证数量**: 26,082个
- **凭证明细**: 140,854条
- **辅助项**: 293,915个
- **时间范围**: 2023-2025年

## 快速开始

### 环境要求
- Python 3.8+
- pandas >= 1.3.0

### 安装依赖
```bash
pip install -r requirements.txt
```

### 运行数据转换
```bash
cd scripts

# 创建数据库并导入所有CSV数据
python csv_to_db.py --reset-db

# 或指定参数
python csv_to_db.py --data-dir ../data --db-path ../database/accounting.db --reset-db
```

### 验证数据库
```bash
# 验证数据库完整性
python csv_to_db.py --validate-only

# 检查数据一致性
python data_consistency_checker.py
```

### 基本功能测试
```bash
python test_basic.py
```

## 命令行参数

### csv_to_db.py
```bash
# 基本用法（使用默认路径）
python csv_to_db.py

# 指定数据目录和数据库路径
python csv_to_db.py --data-dir ../data --db-path ../database/accounting.db

# 重置数据库（删除重建）
python csv_to_db.py --reset-db

# 只验证数据库完整性
python csv_to_db.py --validate-only

# 显示帮助信息
python csv_to_db.py --help
```

### data_consistency_checker.py
```bash
# 检查所有文件
python data_consistency_checker.py

# 检查单个文件
python data_consistency_checker.py --single-file "../data/凭证明细-和立-2024年.csv"

# 显示详细差异
python data_consistency_checker.py --verbose
```

## 技术特点

### 1. 数据一致性保障
- **严格验证**：源数据与数据库数据逐条对比
- **精度要求**：只允许计算机浮点数精度误差（0.00000001）
- **全面检查**：6项一致性检查确保数据完整性

### 2. 智能数据处理
- **金额清理**：自动处理千分位分隔符
- **辅助项解析**：智能解析`【类型：值】`格式
- **公司信息提取**：从核算账簿名称中分离公司信息
- **科目标准化**：科目编码、名称、层级、类型识别

### 3. 性能优化
- **批量处理**：按凭证分组批量插入
- **缓存机制**：避免数据库重复插入
- **索引优化**：为查询字段创建索引

### 4. 错误处理
- **完善验证**：多重数据验证机制
- **详细日志**：完整的操作日志记录
- **错误恢复**：部分错误后可继续处理

## 数据源说明

### 包含公司
1. 广东和立交通养护科技有限公司
2. 广东和立交通养护科技有限公司-省交院账簿类型
3. 广东和立交通养护科技有限公司佛山分公司
4. 广东盛翔交通工程检测有限公司
5. 广东盛翔交通工程检测有限公司江门分公司

### 时间范围
- 2023年全年数据
- 2024年全年数据
- 2025年1-9月数据

### 数据格式
- **编码**: UTF-8-sig
- **分隔符**: 逗号
- **金额格式**: 包含千分位分隔符（如"542,884.60"）
- **辅助项格式**: `【类型：值】`（如`【客商：中国电信股份有限公司广州分公司】`）

## 开发说明

### 代码阅读顺序
1. `docs/方案文档_v2.md` - 理解业务需求和技术方案
2. `scripts/db_schema.py` - 理解数据库结构
3. `scripts/data_cleaner.py` - 理解数据清洗逻辑
4. `scripts/auxiliary_parser.py` - 理解辅助项解析
5. `scripts/csv_to_db.py` - 理解完整转换流程
6. `scripts/data_consistency_checker.py` - 理解数据一致性验证
7. `scripts/test_basic.py` - 理解测试方法

### 兼容性说明
- 支持两种借方/贷方列名格式：`借方-本币`/`贷方-本币` 和 `借-本币`/`贷-本币`
- 假设CSV使用utf-8-sig编码
- 假设辅助项格式为`【类型：值】`
- 假设金额包含千分位分隔符

## 相关项目

### 自然语言查询系统
位于`nl_query/`目录，提供基于DeepSeek API的自然语言到SQL查询功能。

```bash
cd nl_query
pip install -r requirements.txt
streamlit run app.py
```

## 许可证

本项目基于MIT许可证开源。