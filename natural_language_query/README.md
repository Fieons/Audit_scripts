# 自然语言查询系统

基于DeepSeek API的自然语言到SQL查询转换系统，专为审计脚本项目设计。

## 功能特性

- **纯LLM生成**：使用DeepSeek API智能生成SQL查询，不依赖模板匹配
- **自动修复**：支持多轮对话自动修复不完整的SQL语句
- **编码安全**：内置安全的字符串处理，解决Unicode代理对字符问题
- **查询缓存**：LRU缓存机制提升重复查询性能
- **简单易用**：简洁的命令行接口，支持交互模式和批量查询

## 快速开始

### 1. 配置系统

编辑 `config.yaml` 文件，设置您的DeepSeek API密钥：

```yaml
api:
  api_key: "your_api_key_here"  # 必需：DeepSeek API密钥
  model: "deepseek-chat"
  timeout: 30
  max_retries: 3
```

### 2. 运行查询

```bash
# 使用项目根目录的nl_query.py（推荐）
cd "E:\审计\脚本研究"
venv\Scripts\python.exe nl_query.py

# 或直接运行
echo "查询科目总数" | venv\Scripts\python.exe nl_query.py
```

### 3. 查询示例

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

## 系统架构

```
natural_language_query/
├── config.yaml                   # 配置文件
├── requirements.txt              # Python依赖包
└── src/                          # 源代码目录
    ├── api/
    │   └── deepseek_client.py    # DeepSeek API客户端
    ├── database/
    │   ├── connection.py         # 数据库连接管理
    │   ├── query_executor.py     # 查询执行器
    │   └── schema.py             # 数据库模式管理
    ├── sql_generator/
    │   └── core.py               # SQL生成器核心（纯LLM）
    ├── query_processor.py        # 查询处理器
    └── utils/
        ├── config_loader.py      # 配置加载器
        ├── logger.py             # 日志系统
        ├── security.py           # 安全模块（可选）
        └── string_utils.py       # 安全的字符串处理工具
```

## 核心模块

### 1. SQL生成器 (`src/sql_generator/core.py`)
- 使用DeepSeek API生成SQL查询
- 支持自动多轮对话修复不完整的SQL
- 内置详细的数据库模式提示词
- 查询缓存和性能统计

### 2. 查询处理器 (`src/query_processor.py`)
- 集成SQL生成和查询执行
- 结果格式化和显示
- 错误处理和日志记录

### 3. DeepSeek客户端 (`src/api/deepseek_client.py`)
- 完整的DeepSeek API客户端
- 支持聊天补全和流式响应
- 错误重试和速率限制处理

### 4. 字符串工具 (`src/utils/string_utils.py`)
- 安全的字符串处理，解决Unicode代理对字符问题
- 防止JSON解析失败和API调用错误

## 配置说明

### 必需配置
```yaml
api:
  api_key: "your_api_key_here"  # DeepSeek API密钥
```

### 可选配置
```yaml
database:
  path: "../database_transformation/accounting.db"  # 数据库路径
  pool_size: 5                                      # 连接池大小

query:
  cache_size: 100                                   # 缓存大小
  default_limit: 50                                 # 默认结果限制

security:
  enable_sql_injection_check: false                 # SQL注入检查（已禁用）

logging:
  level: "INFO"                                     # 日志级别
  file: "logs/query_system.log"                     # 日志文件
```

## 使用方式

### 交互模式
```bash
venv\Scripts\python.exe nl_query.py
```

### 单次查询
```bash
echo "查询科目总数" | venv\Scripts\python.exe nl_query.py
```

### 批量查询
```bash
venv\Scripts\python.exe nl_query.py --batch queries.txt
```

### 命令行参数
```bash
venv\Scripts\python.exe nl_query.py --help

用法: nl_query.py [选项] [查询]

选项:
  --interactive, -i     进入交互模式（默认）
  --query TEXT, -q TEXT 执行单次查询
  --batch FILE, -b FILE 从文件批量执行查询
  --output FILE, -o FILE 将结果输出到文件
  --no-cache            禁用查询缓存
  --verbose, -v         详细输出模式
  --help                显示帮助信息
```

## 查询示例

### 简单查询
- `查询科目总数`
- `查询所有凭证`
- `查询所有交易`

### 条件查询
- `查询银行存款科目的交易`
- `查询2025年1月的所有银行存款交易`
- `查询金额大于10000的交易`

### 统计查询
- `统计凭证数量`
- `统计银行存款科目的交易总金额`
- `查询每个银行存款科目的交易笔数和总金额`
- `计算平均交易金额`

### 复杂查询
- `查询固定资产清理`
- `查询2025年第一季度凭证不平衡的凭证及其调整记录`
- `按科目统计交易总额并排序`

## 技术特性

### LLM提示词优化
系统提示词包含详细的数据库结构信息：
- 表结构和列定义
- 主键和外键关系
- 常见科目编码规则
- 查询示例和最佳实践

### 自动修复机制
当LLM生成的SQL不完整时，系统会自动：
1. 分析SQL问题（缺少FROM子句、括号不匹配等）
2. 向LLM提供错误描述和问题SQL
3. 请求LLM重新生成完整的SQL
4. 最多重试2次

### 编码安全
- 自动处理Unicode代理对字符
- 防止JSON解析失败
- 确保API调用稳定性
- 安全的缓存键生成

### 性能优化
- LRU查询缓存
- 数据库连接池
- 异步API调用（未来支持）
- 查询结果限制

## 故障排除

### 常见问题

1. **API密钥无效**
   ```
   错误: DeepSeek API Key未设置
   解决: 在config.yaml中设置正确的api.api_key
   ```

2. **数据库连接失败**
   ```
   错误: 无法连接到数据库
   解决: 检查database.path配置，确保数据库文件存在
   ```

3. **编码错误**
   ```
   错误: 'utf-8' codec can't encode character '\udcaf'
   解决: 系统已内置编码安全处理，如仍出现请检查输入字符串
   ```

4. **查询无结果**
   ```
   错误: 查询执行成功但返回空结果
   解决: 检查自然语言描述是否准确，验证数据库中有匹配数据
   ```

### 调试模式
启用详细日志：
```yaml
logging:
  level: "DEBUG"
```

## 开发指南

### 添加新的查询类型
系统使用纯LLM生成，无需添加模板。如需优化特定类型查询，可修改提示词中的示例部分。

### 扩展数据库支持
1. 修改 `src/database/schema.py` 中的模式发现逻辑
2. 更新提示词中的数据库结构信息
3. 测试新的查询类型

### 性能调优
1. 调整缓存大小：`query.cache_size`
2. 调整连接池：`database.pool_size`
3. 调整API超时：`api.timeout`

## 依赖包

```txt
pyyaml>=6.0.0
requests>=2.31.0
sqlalchemy>=2.0.0
```

安装依赖：
```bash
venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 许可证

MIT License

---

**版本**: v2.0.0
**最后更新**: 2025-12-02
**状态**: 稳定运行