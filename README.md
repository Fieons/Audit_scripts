# 审计脚本研究项目

一个用于财务审计的脚本工具集，包含数据转换、验证和自然语言查询功能。

## 项目概述

本项目提供以下核心功能：

1. **数据转换**：将CSV格式的序时账数据完整无损地转换为SQLite数据库
2. **数据验证**：验证转换后的数据与原始数据的一致性
3. **自然语言查询**：使用DeepSeek API将自然语言查询转换为SQL查询并执行

## 项目结构

```
├── README.md                          # 项目主文档（本文档）
├── CLAUDE.md                          # Claude Code项目指导文档
├── nl_query.py                        # 自然语言查询主程序
├── analyze_zero_amounts.py            # 零金额分析脚本
├── validate_transformation.py         # 数据转换验证脚本
├── database_transformation/           # 数据转换项目目录
│   ├── transform_journal.py          # 主转换脚本（完整无损转换）
│   ├── config.py                     # 配置文件
│   ├── requirements.txt              # Python依赖包
│   ├── accounting.db                 # 最终生成的SQLite数据库
│   ├── data/                         # 原始数据目录
│   │   └── 序时账2025.1-9.csv      # 原始序时账数据
│   ├── output/                       # 输出文件目录（空）
│   ├── sql/                          # SQL脚本目录
│   │   └── create_tables.sql        # 数据库建表脚本
│   ├── scripts/                      # 核心脚本目录（空）
│   └── docs/                         # 文档目录
│       └── 项目状态报告.md          # 项目状态报告
└── natural_language_query/           # 自然语言查询系统
    ├── config.yaml                   # 配置文件
    ├── requirements.txt              # Python依赖包
    └── src/                          # 源代码目录
        ├── __init__.py
        ├── api/
        │   ├── __init__.py
        │   └── deepseek_client.py    # DeepSeek API客户端
        ├── database/
        │   ├── __init__.py
        │   ├── connection.py         # 数据库连接管理
        │   ├── query_executor.py     # 查询执行器
        │   └── schema.py             # 数据库模式管理
        ├── sql_generator/
        │   ├── __init__.py
        │   └── core.py               # SQL生成器核心（纯LLM）
        ├── query_processor.py        # 查询处理器
        └── utils/
            ├── __init__.py
            ├── config_loader.py      # 配置加载器
            ├── logger.py             # 日志系统
            ├── security.py           # 安全模块（可选）
            └── string_utils.py       # 安全的字符串处理工具
```

## 数据库结构

转换后的SQLite数据库包含以下表：

### 1. accounts（科目表） - 189条记录
- `id`: 主键
- `code`: 科目编码
- `name`: 科目名称
- `type`: 科目类型（资产、负债、所有者权益、成本、费用）
- `level`: 科目层级

### 2. vouchers（凭证表） - 491条记录
- `id`: 主键
- `voucher_no`: 凭证号
- `date`: 凭证日期
- `type`: 凭证类型（银付、银收、现付、现收、转）
- `description`: 凭证摘要
- `is_balanced`: 是否平衡标识

### 3. transactions（交易表） - 17,211条记录
- `id`: 主键
- `voucher_id`: 凭证ID
- `line_no`: 分录号
- `account_code`: 科目编码
- `date`: 交易日期
- `debit_credit`: 借贷标识（D/C）
- `amount`: 金额（保持原始符号，包括负金额）
- `description`: 交易描述
- `voucher_no`: 凭证号

### 4. balance_adjustments（平衡调整表） - 0条记录（当前数据完全平衡）

## 🚀 快速开始

### 1. 环境设置

项目使用Python 3.12.10虚拟环境：

```bash
# Windows - 激活虚拟环境
venv\Scripts\activate.bat

# 或直接使用虚拟环境中的Python
venv\Scripts\python.exe script_name.py
```

### 2. 安装依赖

```bash
# 安装数据转换依赖
venv\Scripts\python.exe -m pip install -r database_transformation/requirements.txt

# 安装自然语言查询依赖
venv\Scripts\python.exe -m pip install -r natural_language_query/requirements.txt
```

### 3. 数据转换（完整无损转换）

```bash
cd database_transformation
venv\Scripts\python.exe transform_journal.py
```

**关键特性**：
- 保持原始数据的完整性，不改变负金额
- 源数据与数据库数据完全一致
- 借贷完全平衡（差额0.00）

### 4. 数据验证

```bash
venv\Scripts\python.exe validate_transformation.py
```

**验证内容**：
1. 记录数量一致性（CSV vs 数据库）
2. 金额总数一致性（借方/贷方总额）
3. 负金额一致性（负借方/负贷方记录）
4. 借贷平衡验证

### 5. 自然语言查询

```bash
# 交互模式
venv\Scripts\python.exe nl_query.py

# 单次查询
echo "查询科目总数" | venv\Scripts\python.exe nl_query.py

# 批量查询
venv\Scripts\python.exe nl_query.py --batch queries.txt
```

**查询示例**：
- `查询科目总数`
- `查询银行存款科目的交易`
- `查询2025年1月的所有银行存款交易`
- `统计银行存款科目的交易总金额`
- `查询每个银行存款科目的交易笔数和总金额`
- `查询固定资产清理`

## 自然语言查询系统配置

### 配置文件位置
`natural_language_query/config.yaml`

### 配置示例
```yaml
database:
  path: "../database_transformation/accounting.db"
  pool_size: 5
  timeout: 30

api:
  base_url: "https://api.deepseek.com"
  api_key: "your_api_key_here"  # 必需：DeepSeek API密钥
  model: "deepseek-chat"
  timeout: 30
  max_retries: 3
  request_interval: 1.0

query:
  cache_size: 100
  default_limit: 50
  enable_cache: true

security:
  enable_sql_injection_check: false  # 已禁用，确保程序顺利运行

logging:
  level: "INFO"
  file: "logs/query_system.log"
```

### API密钥设置
1. 获取DeepSeek API密钥：https://platform.deepseek.com/api_keys
2. 将密钥填入`config.yaml`中的`api.api_key`字段
3. 或设置环境变量：`DEEPSEEK_API_KEY`

## 技术特性

### 数据转换
- **完整无损转换**：保持原始数据的完整性，不改变负金额
- **金额一致**：数据库金额与源数据完全一致（311,531,191.65）
- **记录一致**：所有记录完整转换，包括负金额记录
- **借贷平衡**：保持原始借贷平衡（差额0.00）

### 自然语言查询
- **纯LLM生成**：使用DeepSeek API智能生成SQL查询
- **自动修复**：支持多轮对话自动修复不完整的SQL语句
- **编码安全**：内置安全的字符串处理，解决Unicode代理对字符问题
- **查询缓存**：LRU缓存机制提升重复查询性能
- **错误处理**：完善的错误处理和日志记录

### 验证系统
- **全面验证**：记录数量、金额总数、负金额、借贷平衡
- **详细报告**：生成详细的验证报告
- **数据一致性**：确保源数据与数据库数据完全一致

## 使用示例

### 数据验证结果
```
✅ 记录数量验证通过
  CSV记录数: 17,211
  数据库记录数: 17,211

✅ 金额总数验证通过
  借方总额: 311,531,191.65
  贷方总额: 311,531,191.65
  差额: 0.00

✅ 负金额验证通过
  负借方记录: 988条
  负贷方记录: 215条
```

### 自然语言查询示例
```
输入查询: 查询科目总数
生成的SQL: SELECT COUNT(*) FROM accounts LIMIT 50
查询结果: 189

输入查询: 查询银行存款科目的交易
生成的SQL: SELECT t.* FROM transactions t JOIN accounts a ON t.account_code = a.code WHERE a.code LIKE '1002%' LIMIT 50
查询结果: 50条记录

输入查询: 统计银行存款科目的交易总金额
生成的SQL: SELECT SUM(t.amount) as total_amount FROM transactions t JOIN accounts a ON t.account_code = a.code WHERE a.code LIKE '1002%'
查询结果: 282,695,931.76
```

## 开发说明

### 虚拟环境
项目使用Python 3.12.10虚拟环境，所有依赖应在虚拟环境中安装：

```bash
# 安装数据转换依赖
venv\Scripts\python.exe -m pip install -r database_transformation/requirements.txt

# 安装自然语言查询依赖
venv\Scripts\python.exe -m pip install -r natural_language_query/requirements.txt
```

### 主要依赖包
- `pandas>=2.0.0`
- `numpy>=1.24.0`
- `sqlalchemy>=2.0.0`
- `pyyaml>=6.0.0`
- `requests>=2.31.0`
- `tqdm>=4.65.0`

### 编码问题解决方案
项目已实现全面的编码问题解决方案：
- `string_utils.py`：安全的字符串处理工具
- 自动处理Unicode代理对字符
- 防止JSON解析失败
- 确保API调用稳定性

## 数据转换原则

1. **完整无损转换**：保持原始数据的完整性，不改变负金额
2. **金额一致**：数据库金额与源数据完全一致（311,531,191.65）
3. **记录一致**：所有记录完整转换，包括负金额记录
4. **借贷平衡**：保持原始借贷平衡（差额0.00）

## 数据验证逻辑

验证脚本比较原始CSV与转换后数据库的一致性：
- 记录数量：CSV 17,211条 = 数据库 17,211条
- 借方记录：CSV 12,951条 = 数据库 12,951条
- 贷方记录：CSV 4,260条 = 数据库 4,260条
- 借方总额：311,531,191.65
- 贷方总额：311,531,191.65
- 负金额：负借方988条，负贷方215条

## 项目状态

✅ **项目清理完成** - 删除所有错误和过渡性文件
✅ **命名规则完善** - 统一文件命名
✅ **数据转换正确** - 完整无损转换
✅ **借贷完全平衡** - 保持原始平衡
✅ **数据一致性验证通过** - 源数据与数据库完全一致
✅ **脚本逻辑正确** - 验证脚本逻辑修复
✅ **自然语言查询功能** - 纯LLM生成SQL，支持复杂查询
✅ **编码问题解决** - 全面的Unicode代理对字符处理

## 🔧 故障排除

### 常见问题

#### 1. API密钥问题
**症状**: `DeepSeek API Key未设置` 或 `API密钥无效`
**解决方案**:
1. 获取DeepSeek API密钥：访问 https://platform.deepseek.com/api_keys
2. 将密钥填入 `natural_language_query/config.yaml` 中的 `api.api_key` 字段
3. 或设置环境变量：`DEEPSEEK_API_KEY`

#### 2. 数据库连接失败
**症状**: `无法连接到数据库` 或 `数据库文件不存在`
**解决方案**:
1. 确保已运行数据转换：`venv\Scripts\python.exe database_transformation\transform_journal.py`
2. 检查数据库路径：`natural_language_query/config.yaml` 中的 `database.path`
3. 验证数据库文件存在：`database_transformation/accounting.db`

#### 3. 编码错误
**症状**: `'utf-8' codec can't encode character '\udcaf'`
**解决方案**:
1. 系统已内置编码安全处理，通常会自动解决
2. 如仍出现，检查输入字符串中的特殊字符
3. 使用 `src/utils/string_utils.py` 中的安全字符串处理函数

#### 4. 查询无结果
**症状**: 查询执行成功但返回空结果
**解决方案**:
1. 检查自然语言描述是否准确
2. 验证数据库中有匹配数据
3. 尝试更具体的查询条件

### 调试模式
启用详细日志：
```yaml
# natural_language_query/config.yaml
logging:
  level: "DEBUG"
```

## ⚠️ 注意事项

1. **API密钥**：自然语言查询功能需要有效的DeepSeek API密钥
2. **数据准确性**：所有财务分析都应基于数据库中的准确数据
3. **性能考虑**：大量数据查询时建议添加适当的限制条件
4. **数据备份**：建议定期备份数据库文件
5. **安全设置**：当前禁用SQL注入检查以确保程序顺利运行，生产环境应重新启用

## 扩展方向

1. **多账簿支持**：支持多个账簿的数据整合
2. **实时数据更新**：实现数据变更监听和增量同步
3. **报表系统**：开发标准化财务报表和自定义报表格式
4. **数据可视化**：集成图表展示和仪表板功能
5. **Web界面**：提供Web-based查询界面
6. **多模型支持**：支持OpenAI、通义千问等其他LLM

## 🤝 贡献指南

欢迎贡献代码、报告问题或提出建议！

### 报告问题
1. 在 Issues 页面创建新问题
2. 描述问题的详细步骤
3. 提供错误信息和环境信息
4. 如有可能，提供最小可复现示例

### 提交代码
1. Fork 项目仓库
2. 创建功能分支：`git checkout -b feature/your-feature`
3. 提交更改：`git commit -m "Add your feature"`
4. 推送到分支：`git push origin feature/your-feature`
5. 创建 Pull Request

### 开发规范
1. 遵循现有代码风格
2. 添加适当的注释和文档
3. 确保所有测试通过
4. 更新相关文档

## 📄 许可证

本项目采用 MIT 许可证。

## 📞 联系方式

如有问题或建议，请通过项目 Issues 页面提交。

---

**文档更新时间**: 2025-12-02
**当前版本**: v2.0.0
**项目状态**: 功能完整，稳定运行