"""
SQL生成器核心

提供自然语言到SQL的转换功能，使用LLM生成SQL。
"""

import re
import json
import hashlib
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
import time

from ..api.deepseek_client import DeepSeekClient, Message, MessageRole, get_deepseek_client
from ..database.schema import get_schema_manager, DatabaseSchema, TableInfo, ColumnInfo
from ..utils.string_utils import safe_string, safe_hash

logger = logging.getLogger(__name__)


class GenerationMethod(Enum):
    """生成方法"""
    LLM = "llm"


class QueryType(Enum):
    """查询类型"""
    SELECT = "SELECT"
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MAX = "MAX"
    MIN = "MIN"
    GROUP_BY = "GROUP_BY"
    JOIN = "JOIN"
    COMPLEX = "COMPLEX"




@dataclass
class GeneratedSQL:
    """生成的SQL"""
    sql: str
    method: GenerationMethod
    query_type: QueryType
    confidence: float  # 置信度 0.0-1.0
    tables_involved: List[str]
    columns_involved: List[str]
    generation_time: float
    error_message: Optional[str] = None

    def is_valid(self) -> bool:
        """检查SQL是否有效"""
        return self.sql and not self.error_message and self.confidence > 0.3

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "sql": self.sql,
            "method": self.method.value,
            "query_type": self.query_type.value,
            "confidence": self.confidence,
            "tables_involved": self.tables_involved,
            "columns_involved": self.columns_involved,
            "generation_time": self.generation_time,
            "error_message": self.error_message,
            "is_valid": self.is_valid()
        }


class SQLGenerator:
    """SQL生成器"""

    def __init__(self, schema: Optional[DatabaseSchema] = None, client: Optional[DeepSeekClient] = None):
        """
        初始化SQL生成器

        Args:
            schema: 数据库模式，如果为None则自动加载
            client: DeepSeek客户端，如果为None则尝试获取（可能失败）
        """
        self.schema = schema or get_schema_manager().load_schema()

        # 尝试获取客户端，但允许失败（只使用模板功能）
        try:
            self.client = client or get_deepseek_client()
            self.has_llm = True
            logger.info("SQL生成器初始化完成，LLM功能可用")
        except ValueError as e:
            # API密钥未设置，只使用模板功能
            self.client = None
            self.has_llm = False
            logger.warning(f"DeepSeek API Key未设置，LLM功能不可用: {e}")

        self.cache: Dict[str, GeneratedSQL] = {}

        logger.info(f"SQL生成器初始化完成，表数量: {len(self.schema.tables)}，LLM功能: {'可用' if self.has_llm else '不可用'}")


    def generate_sql_from_llm(self, query: str, max_retries: int = 2) -> GeneratedSQL:
        """使用LLM生成SQL，支持重试"""
        start_time = time.time()

        try:
            # 检查LLM是否可用
            if not self.has_llm:
                raise ValueError("DeepSeek API Key未设置，LLM功能不可用")

            # 准备模式信息
            schema_info = self._prepare_schema_info()

            # 构建初始提示词
            system_prompt = self._build_system_prompt(schema_info)
            user_prompt = self._build_user_prompt(query)

            messages = [
                Message(role=MessageRole.SYSTEM, content=system_prompt),
                Message(role=MessageRole.USER, content=user_prompt)
            ]

            sql = ""
            attempt = 0

            while attempt < max_retries + 1:  # 包括第一次尝试
                attempt += 1
                logger.info(f"第{attempt}次尝试生成SQL，查询: '{query}'")

                try:
                    # 调用LLM
                    response = self.client.chat_completion(
                        messages=messages,
                        temperature=0.3,
                        max_tokens=500
                    )

                    content = response.get_content()

                    # 解析响应
                    sql = self._extract_sql_from_response(content)
                    if not sql:
                        raise ValueError("无法从LLM响应中提取SQL")

                    # 验证SQL
                    if self._validate_sql(sql):
                        logger.info(f"第{attempt}次尝试成功，生成的SQL有效")
                        break
                    else:
                        logger.warning(f"第{attempt}次尝试失败，生成的SQL无效: {sql}")

                        # 如果不是最后一次尝试，添加修复提示
                        if attempt <= max_retries:
                            # 分析SQL问题
                            sql_upper = sql.upper()
                            error_desc = ""
                            if 'FROM' not in sql_upper:
                                error_desc = "缺少FROM子句"
                            elif sql.count('(') != sql.count(')'):
                                error_desc = "括号不匹配"
                            else:
                                error_desc = "SQL语法不完整"

                            # 添加修复提示
                            repair_prompt = f"""你生成的SQL有问题：{error_desc}

请修复以下SQL语句，确保它是完整的、语法正确的SQL查询：
{sql}

请重新生成完整的SQL语句："""

                            messages.append(Message(role=MessageRole.ASSISTANT, content=sql))
                            messages.append(Message(role=MessageRole.USER, content=repair_prompt))
                        else:
                            raise ValueError(f"经过{max_retries + 1}次尝试，仍然无法生成有效的SQL")

                except Exception as e:
                    logger.error(f"第{attempt}次尝试失败: {e}")
                    if attempt > max_retries:
                        raise
                    # 继续重试

            if not sql or not self._validate_sql(sql):
                raise ValueError("无法生成有效的SQL")

            # 提取涉及的表和列
            tables_involved = self._extract_tables_from_sql(sql)
            columns_involved = self._extract_columns_from_sql(sql, tables_involved)

            generation_time = time.time() - start_time

            # 根据尝试次数调整置信度
            confidence = 0.7 if attempt == 1 else 0.5

            return GeneratedSQL(
                sql=sql,
                method=GenerationMethod.LLM,
                query_type=self._infer_query_type(sql),
                confidence=confidence,
                tables_involved=tables_involved,
                columns_involved=columns_involved,
                generation_time=generation_time
            )

        except Exception as e:
            logger.error(f"LLM生成SQL失败: {e}")
            generation_time = time.time() - start_time
            return GeneratedSQL(
                sql="",
                method=GenerationMethod.LLM,
                query_type=QueryType.COMPLEX,
                confidence=0.0,
                tables_involved=[],
                columns_involved=[],
                generation_time=generation_time,
                error_message=str(e)
            )

    def _prepare_schema_info(self) -> str:
        """准备模式信息"""
        schema_info = []

        for table_name, table_info in self.schema.tables.items():
            table_desc = f"表: {table_name} ({table_info.estimated_row_count} 行)\n"
            table_desc += "列:\n"

            for col_name, column in table_info.columns.items():
                col_desc = f"  - {col_name}: {column.type}"
                if column.primary_key:
                    col_desc += " (主键)"
                if column.foreign_key:
                    col_desc += f" -> {column.referenced_table}.{column.referenced_column}"
                table_desc += col_desc + "\n"

            # 外键关系
            if table_info.foreign_keys:
                table_desc += "外键:\n"
                for fk_name, fk_info in table_info.foreign_keys.items():
                    table_desc += f"  - {fk_info['from_column']} -> {fk_info['to_table']}.{fk_info['to_column']}\n"

            schema_info.append(table_desc)

        return "\n".join(schema_info)

    def _build_system_prompt(self, schema_info: str) -> str:
        """构建系统提示词"""
        return f"""你是一个SQL专家，负责将自然语言查询转换为SQL查询。

数据库模式信息：
{schema_info}

重要数据库知识：
1. 这是一个财务审计数据库，包含科目、凭证、交易三张表
2. 科目表(accounts)：存储科目信息，主键是id，科目编码是code，科目名称是name
3. 凭证表(vouchers)：存储凭证信息，主键是id，凭证号是voucher_no
4. 交易表(transactions)：存储交易明细，主键是id，包含amount(金额)、date(日期)、account_code(科目编码)、voucher_no(凭证号)
5. 关键关系：
   - transactions.account_code 关联 accounts.code
   - transactions.voucher_no 关联 vouchers.voucher_no
6. 常见科目编码：
   - 银行存款科目：code以'1002'开头
   - 现金科目：code以'1001'开头

查询要求：
1. 只生成SQL语句，不要包含解释
2. 使用正确的表名和列名，不要使用t.*这样的占位符
3. 对于科目相关查询，通常需要JOIN accounts表和transactions表
4. 对于凭证相关查询，通常需要JOIN vouchers表和transactions表
5. 金额查询使用transactions.amount
6. 日期查询使用transactions.date或vouchers.date
7. 默认限制结果数量为50行（使用LIMIT 50）
8. 确保SQL语法完整（必须有SELECT、FROM、WHERE等必要子句）

常见查询示例：
1. 查询银行存款科目的交易：SELECT t.* FROM transactions t JOIN accounts a ON t.account_code = a.code WHERE a.code LIKE '1002%' LIMIT 50
2. 查询科目总数：SELECT COUNT(*) FROM accounts LIMIT 50
3. 查询所有交易：SELECT * FROM transactions LIMIT 50
4. 查询凭证信息：SELECT * FROM vouchers LIMIT 50
5. 查询银行存款科目的交易笔数和总金额：SELECT COUNT(*) as transaction_count, SUM(t.amount) as total_amount FROM transactions t JOIN accounts a ON t.account_code = a.code WHERE a.code LIKE '1002%' LIMIT 50
6. 查询1月份的交易笔数：SELECT COUNT(*) FROM transactions WHERE strftime('%m', date) = '01' LIMIT 50
7. 查询1月份银行存款科目的交易笔数和总金额：SELECT COUNT(*) as transaction_count, SUM(t.amount) as total_amount FROM transactions t JOIN accounts a ON t.account_code = a.code WHERE a.code LIKE '1002%' AND strftime('%m', t.date) = '01' LIMIT 50
8. 查询1月份各银行交易的笔数和各自的总金额：SELECT a.name AS 科目名称, COUNT(*) AS 交易笔数, SUM(t.amount) AS 总金额 FROM transactions t JOIN accounts a ON t.account_code = a.code WHERE a.name LIKE '%银行%' AND strftime('%m', t.date) = '01' GROUP BY a.name ORDER BY 总金额 DESC LIMIT 50
9. 查询按借贷方向统计的交易笔数和总金额：SELECT debit_credit AS 借贷方向, COUNT(*) AS 交易笔数, SUM(amount) AS 总金额 FROM transactions GROUP BY debit_credit LIMIT 50
10. 查询1月份银行存款科目按借贷方向统计的交易：SELECT a.name AS 科目名称, t.debit_credit AS 借贷方向, COUNT(*) AS 交易笔数, SUM(t.amount) AS 总金额 FROM transactions t JOIN accounts a ON t.account_code = a.code WHERE a.code LIKE '1002%' AND strftime('%m', t.date) = '01' GROUP BY a.name, t.debit_credit ORDER BY a.name, t.debit_credit LIMIT 50

返回格式：
只返回SQL语句，不要包含其他内容。"""

    def _build_user_prompt(self, query: str) -> str:
        """构建用户提示词"""
        # 使用安全的字符串处理
        safe_query = safe_string(query)
        return f"""请将以下自然语言查询转换为SQL查询：

查询："{safe_query}"

请生成对应的SQL语句："""

    def _extract_sql_from_response(self, response: str) -> str:
        """从响应中提取SQL"""
        # 去除代码块标记
        sql = response.strip()
        sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'^```\s*', '', sql)
        sql = re.sub(r'\s*```$', '', sql)

        # 去除可能的解释文本
        lines = sql.split('\n')
        sql_lines = []
        for line in lines:
            if line.strip().upper().startswith(('SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE')):
                sql_lines.append(line)

        return '\n'.join(sql_lines).strip()

    def _validate_sql(self, sql: str) -> bool:
        """验证SQL"""
        if not sql:
            return False

        # 基本语法检查
        sql_upper = sql.upper()
        if not sql_upper.startswith('SELECT'):
            return False

        # 检查SQL完整性
        # 1. 必须有FROM子句
        if 'FROM' not in sql_upper:
            return False

        # 2. 检查括号是否匹配（简单检查）
        if sql.count('(') != sql.count(')'):
            return False

        # 3. 检查是否有危险操作
        dangerous_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'CREATE', 'ALTER', 'TRUNCATE']
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                return False

        return True

    def _extract_tables_from_sql(self, sql: str) -> List[str]:
        """从SQL中提取表名"""
        tables = []
        sql_upper = sql.upper()

        # 简单的表名提取（实际项目中应该使用SQL解析器）
        for table_name in self.schema.tables.keys():
            table_name_upper = table_name.upper()
            if table_name_upper in sql_upper:
                tables.append(table_name)

        return tables

    def _extract_columns_from_sql(self, sql: str, tables: List[str]) -> List[str]:
        """从SQL中提取列名"""
        columns = []
        sql_upper = sql.upper()

        for table_name in tables:
            if table_name in self.schema.tables:
                table_info = self.schema.tables[table_name]
                for column_name in table_info.columns.keys():
                    column_name_upper = column_name.upper()
                    if column_name_upper in sql_upper:
                        columns.append(column_name)

        return columns

    def _infer_query_type(self, sql: str) -> QueryType:
        """推断查询类型"""
        sql_upper = sql.upper()

        if 'COUNT(' in sql_upper:
            return QueryType.COUNT
        elif 'SUM(' in sql_upper:
            return QueryType.SUM
        elif 'AVG(' in sql_upper:
            return QueryType.AVG
        elif 'MAX(' in sql_upper:
            return QueryType.MAX
        elif 'MIN(' in sql_upper:
            return QueryType.MIN
        elif 'GROUP BY' in sql_upper:
            return QueryType.GROUP_BY
        elif 'JOIN' in sql_upper:
            return QueryType.JOIN
        else:
            return QueryType.SELECT

    def _get_cache_key(self, query: str) -> str:
        """获取缓存键"""
        # 使用查询内容和模式版本作为缓存键
        schema_version = self.schema.version
        key_data = f"{safe_string(query)}_{schema_version}"
        # 使用安全的哈希函数
        return safe_hash(key_data)

    def generate_sql(
        self,
        query: str,
        use_cache: bool = True
    ) -> GeneratedSQL:
        """
        生成SQL

        Args:
            query: 自然语言查询
            use_cache: 是否使用缓存

        Returns:
            GeneratedSQL: 生成的SQL
        """
        logger.info(f"开始生成SQL，查询: '{query}'")

        # 检查缓存
        if use_cache:
            cache_key = self._get_cache_key(query)
            if cache_key in self.cache:
                cached_result = self.cache[cache_key]
                logger.info(f"缓存命中，使用缓存的SQL")
                return cached_result

        # 使用LLM生成SQL
        result = self.generate_sql_from_llm(query)

        # 缓存结果
        if use_cache and result.is_valid():
            cache_key = self._get_cache_key(query)
            self.cache[cache_key] = result

        logger.info(f"SQL生成完成，置信度: {result.confidence:.2f}，是否有效: {result.is_valid()}")
        if result.sql:
            logger.debug(f"生成的SQL: {result.sql}")

        return result

    def clear_cache(self):
        """清空缓存"""
        self.cache.clear()
        logger.info("SQL生成器缓存已清空")


# 全局SQL生成器实例
_sql_generator: Optional[SQLGenerator] = None


def get_sql_generator(schema: Optional[DatabaseSchema] = None, client: Optional[DeepSeekClient] = None) -> SQLGenerator:
    """获取SQL生成器实例"""
    global _sql_generator
    if _sql_generator is None:
        _sql_generator = SQLGenerator(schema, client)
    return _sql_generator


def clear_sql_generator_cache():
    """清空SQL生成器缓存"""
    global _sql_generator
    if _sql_generator is not None:
        _sql_generator.clear_cache()


# 导出
__all__ = [
    "GenerationMethod",
    "QueryType",
    "GeneratedSQL",
    "SQLGenerator",
    "get_sql_generator",
    "clear_sql_generator_cache",
]