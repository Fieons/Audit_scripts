"""
SQL生成专用客户端
继承BaseLLMClient，专门处理自然语言到SQL的转换
"""

import logging
import re
import time
from typing import Dict, Any, List

from .base import BaseLLMClient, LLMError
from ..config import DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL, DEEPSEEK_MODEL

logger = logging.getLogger(__name__)

class SQLGenerationError(LLMError):
    """SQL生成相关错误"""
    pass

class SQLGenerationClient(BaseLLMClient):
    """SQL生成专用客户端"""

    def __init__(self, api_key: str = None, base_url: str = None, model: str = None):
        """初始化SQL生成客户端"""
        super().__init__(
            api_key=api_key or DEEPSEEK_API_KEY,
            base_url=base_url or DEEPSEEK_BASE_URL,
            model=model or DEEPSEEK_MODEL
        )

    def generate_sql(self, natural_language: str, schema_info: str, examples: str,
                    temperature: float = 0.1, max_tokens: int = 1000) -> str:
        """
        调用DeepSeek API生成SQL

        Args:
            natural_language: 自然语言查询
            schema_info: 数据库schema信息
            examples: 查询示例
            temperature: 生成温度（0-1）
            max_tokens: 最大token数

        Returns:
            生成的SQL语句
        """
        start_time = time.time()

        try:
            # 构建系统提示词
            system_prompt = self._build_system_prompt(schema_info, examples)

            # 构建用户消息
            user_message = f"用户查询: {natural_language}\n\n请生成对应的SQL语句:"

            # 调用API
            response = self.call_api(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                temperature=temperature,
                max_tokens=max_tokens
            )

            # 提取SQL
            sql = self._extract_sql_from_response(response)

            # 记录日志
            elapsed_time = time.time() - start_time
            logger.info(f"SQL生成成功: 耗时{elapsed_time:.2f}秒, 请求次数: {self.request_count}")
            return sql

        except Exception as e:
            logger.error(f"SQL生成失败: {e}")
            raise SQLGenerationError(f"SQL生成失败: {e}")

    def _build_system_prompt(self, schema_info: str, examples: str) -> str:
        """构建系统提示词"""
        prompt = """你是一个专业的SQL专家，负责将自然语言查询转换为准确、高效的SQL语句。

数据库审计凭证系统包含以下表结构：
{schema_info}

以下是一些查询示例，请参考这些示例的格式和风格：
{examples}

重要规则：
1. 只生成SELECT查询语句，绝对不允许任何修改操作（INSERT、UPDATE、DELETE、DROP、ALTER、TRUNCATE、CREATE等）
2. 使用中文别名提高查询结果的可读性（如：公司名称 as 公司）
3. 添加适当的LIMIT子句限制结果集大小（默认1000行）
4. 优先使用索引字段进行查询优化
5. 确保JOIN条件正确，避免笛卡尔积
6. 对于金额字段，使用适当的聚合函数（SUM、AVG等）
7. 对于日期范围查询，使用BETWEEN或>=/<运算符
8. 生成的SQL语句要完整，以分号结尾

请根据用户查询生成对应的SQL语句，确保语法正确、逻辑清晰、性能优化。"""

        return prompt.format(schema_info=schema_info, examples=examples)

    def _extract_sql_from_response(self, response) -> str:
        """从API响应中提取SQL语句"""
        content = self._extract_content_from_response(response)

        # 提取SQL代码块
        sql = self._extract_sql_code_block(content)

        # 清理SQL
        sql = self._clean_sql(sql)

        # 验证SQL基本格式
        self._validate_sql_format(sql)

        return sql

    def _extract_sql_code_block(self, content: str) -> str:
        """提取SQL代码块"""
        # 查找```sql ... ```格式的代码块
        pattern = r"```sql\s*(.*?)\s*```"
        matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)

        if matches:
            return matches[0].strip()

        # 如果没有代码块，返回整个内容
        return content

    def _clean_sql(self, sql: str) -> str:
        """清理SQL语句"""
        # 移除多余的空格和换行
        sql = ' '.join(sql.split())

        # 确保以分号结尾
        if not sql.endswith(';'):
            sql += ';'

        return sql

    def _validate_sql_format(self, sql: str):
        """验证SQL基本格式"""
        sql_upper = sql.upper()

        # 检查是否以SELECT或WITH开头
        if not (sql_upper.strip().startswith("SELECT") or sql_upper.strip().startswith("WITH")):
            raise SQLGenerationError("生成的SQL不是SELECT查询")

        # 检查是否包含禁止的关键字
        forbidden_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER",
                            "TRUNCATE", "CREATE", "GRANT", "REVOKE"]
        for keyword in forbidden_keywords:
            if keyword in sql_upper:
                raise SQLGenerationError(f"生成的SQL包含禁止的操作: {keyword}")

# 工具函数（保持向后兼容）
def create_sql_generation_prompt(natural_language: str, schema_info: Dict[str, Any],
                                examples: List[Dict[str, str]]) -> Dict[str, str]:
    """创建SQL生成提示词"""
    from .database import format_schema_for_prompt, format_examples_for_prompt

    schema_prompt = format_schema_for_prompt(schema_info)
    examples_prompt = format_examples_for_prompt(examples)

    return {
        "schema_info": schema_prompt,
        "examples": examples_prompt,
        "user_query": natural_language
    }

if __name__ == "__main__":
    # 测试SQL生成客户端
    import logging
    logging.basicConfig(level=logging.INFO)

    from .config import DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL, DEEPSEEK_MODEL

    print("测试SQL生成客户端...")

    client = SQLGenerationClient(
        api_key=DEEPSEEK_API_KEY,
        base_url=DEEPSEEK_BASE_URL,
        model=DEEPSEEK_MODEL
    )

    # 测试连接
    if client.test_connection():
        print("✓ API连接测试成功")

        # 测试SQL生成（使用模拟数据）
        test_schema = """
        companies表: id, name, address
        vouchers表: id, company_id, voucher_date, amount
        """
        test_examples = """
        示例1: 查询所有公司信息 -> SELECT * FROM companies;
        示例2: 查询2024年凭证 -> SELECT * FROM vouchers WHERE voucher_date >= '2024-01-01';
        """
        test_query = "查询所有公司信息"

        try:
            sql = client.generate_sql(test_query, test_schema, test_examples)
            print(f"✓ SQL生成测试成功: {sql}")

            # 显示统计信息
            stats = client.get_stats()
            print(f"\n统计信息:")
            for key, value in stats.items():
                print(f"  {key}: {value}")

        except Exception as e:
            print(f"✗ SQL生成测试失败: {e}")
    else:
        print("✗ API连接测试失败")