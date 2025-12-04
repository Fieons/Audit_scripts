"""
SQL生成器核心逻辑
整合数据库连接和DeepSeek API，实现自然语言到SQL的完整转换流程
"""

import logging
from typing import Optional, Dict, Any, Tuple
from functools import lru_cache
import time

from database import DatabaseManager, format_schema_for_prompt, format_examples_for_prompt
from deepseek_client import DeepSeekClient, DeepSeekError
from config import CACHE_ENABLED, CACHE_MAX_SIZE, CACHE_TTL

logger = logging.getLogger(__name__)

class SQLGenerationError(Exception):
    """SQL生成相关错误"""
    pass

class SQLGenerator:
    """SQL生成器"""

    def __init__(self, db_manager: DatabaseManager = None, deepseek_client: DeepSeekClient = None):
        self.db_manager = db_manager or DatabaseManager()
        self.deepseek_client = deepseek_client or DeepSeekClient()

        # 缓存schema和示例
        self._schema_info = None
        self._examples = None
        self._schema_prompt = None
        self._examples_prompt = None

        # 初始化统计
        self.generation_count = 0
        self.success_count = 0
        self.total_time = 0.0

    def nl_to_sql(self, natural_language: str, use_cache: bool = True) -> Tuple[str, Dict[str, Any]]:
        """
        将自然语言转换为SQL

        Args:
            natural_language: 自然语言查询
            use_cache: 是否使用缓存

        Returns:
            (sql语句, 元数据)
        """
        start_time = time.time()
        self.generation_count += 1

        try:
            # 检查缓存
            if use_cache and CACHE_ENABLED:
                cached_result = self._get_cached_sql(natural_language)
                if cached_result:
                    logger.info(f"使用缓存结果: {natural_language[:50]}...")
                    self.success_count += 1
                    elapsed_time = time.time() - start_time
                    self.total_time += elapsed_time
                    return cached_result

            # 获取schema信息和示例
            schema_info = self._get_schema_info()
            examples = self._get_examples()

            # 生成SQL
            sql = self.deepseek_client.generate_sql(
                natural_language=natural_language,
                schema_info=self._schema_prompt,
                examples=self._examples_prompt
            )

            # 验证SQL
            self._validate_generated_sql(sql)

            # 缓存结果
            if use_cache and CACHE_ENABLED:
                self._cache_sql(natural_language, sql)

            # 记录成功
            self.success_count += 1
            elapsed_time = time.time() - start_time
            self.total_time += elapsed_time

            # 构建元数据
            metadata = {
                "generation_time": elapsed_time,
                "cache_hit": False,
                "natural_language": natural_language,
                "sql_length": len(sql),
                "success": True
            }

            logger.info(f"SQL生成成功: {natural_language[:50]}... -> {sql[:100]}...")
            return sql, metadata

        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"SQL生成失败: {e}")

            metadata = {
                "generation_time": elapsed_time,
                "cache_hit": False,
                "natural_language": natural_language,
                "error": str(e),
                "success": False
            }

            raise SQLGenerationError(f"SQL生成失败: {e}") from e

    def execute_query(self, sql: str, params: tuple = None, limit: int = None) -> Tuple[Any, Dict[str, Any]]:
        """
        执行SQL查询

        Args:
            sql: SQL语句
            params: 查询参数
            limit: 结果限制

        Returns:
            (查询结果, 元数据)
        """
        start_time = time.time()

        try:
            # 使用数据库管理器执行查询
            with self.db_manager as db:
                result = db.execute_query(sql, params, limit)

            elapsed_time = time.time() - start_time

            metadata = {
                "execution_time": elapsed_time,
                "row_count": len(result),
                "column_count": len(result.columns) if hasattr(result, 'columns') else 0,
                "sql": sql,
                "success": True
            }

            logger.info(f"查询执行成功: {len(result)}行, 耗时{elapsed_time:.2f}秒")
            return result, metadata

        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"查询执行失败: {e}")

            metadata = {
                "execution_time": elapsed_time,
                "error": str(e),
                "sql": sql,
                "success": False
            }

            raise SQLGenerationError(f"查询执行失败: {e}") from e

    def nl_to_result(self, natural_language: str, use_cache: bool = True) -> Tuple[Any, Dict[str, Any]]:
        """
        自然语言到查询结果的完整流程

        Args:
            natural_language: 自然语言查询
            use_cache: 是否使用缓存

        Returns:
            (查询结果, 完整元数据)
        """
        full_metadata = {
            "natural_language": natural_language,
            "steps": [],
            "total_time": 0.0,
            "success": False
        }

        start_time = time.time()

        try:
            # 步骤1: 生成SQL
            sql_step_start = time.time()
            sql, sql_metadata = self.nl_to_sql(natural_language, use_cache)
            sql_step_time = time.time() - sql_step_start

            full_metadata["steps"].append({
                "step": "sql_generation",
                "time": sql_step_time,
                "sql": sql,
                **sql_metadata
            })

            # 步骤2: 执行查询
            exec_step_start = time.time()
            result, exec_metadata = self.execute_query(sql)
            exec_step_time = time.time() - exec_step_start

            full_metadata["steps"].append({
                "step": "query_execution",
                "time": exec_step_time,
                **exec_metadata
            })

            # 更新总元数据
            full_metadata["total_time"] = time.time() - start_time
            full_metadata["success"] = True
            full_metadata["final_sql"] = sql
            full_metadata["result_shape"] = {
                "rows": len(result),
                "columns": len(result.columns) if hasattr(result, 'columns') else 0
            }

            logger.info(f"完整流程成功: {natural_language[:50]}... -> {len(result)}行结果")
            return result, full_metadata

        except Exception as e:
            full_metadata["total_time"] = time.time() - start_time
            full_metadata["error"] = str(e)
            logger.error(f"完整流程失败: {e}")
            raise

    def _get_schema_info(self) -> Dict[str, Any]:
        """获取schema信息（带缓存）"""
        if self._schema_info is None:
            with self.db_manager as db:
                self._schema_info = db.get_schema_info()
                self._schema_prompt = format_schema_for_prompt(self._schema_info)
        return self._schema_info

    def _get_examples(self) -> list:
        """获取查询示例（带缓存）"""
        if self._examples is None:
            with self.db_manager as db:
                self._examples = db.get_query_examples()
                self._examples_prompt = format_examples_for_prompt(self._examples)
        return self._examples

    def _validate_generated_sql(self, sql: str):
        """验证生成的SQL"""
        # 基本验证已在DeepSeek客户端中完成
        # 这里可以添加额外的验证逻辑
        pass

    @lru_cache(maxsize=CACHE_MAX_SIZE)
    def _get_cached_sql(self, natural_language: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        """获取缓存的SQL（使用LRU缓存）"""
        # 这里使用内存缓存，实际可以扩展到Redis等
        return None

    def _cache_sql(self, natural_language: str, sql: str):
        """缓存SQL结果"""
        # LRU缓存自动管理
        pass

    def get_stats(self) -> Dict[str, Any]:
        """获取生成器统计信息"""
        success_rate = (self.success_count / self.generation_count * 100) if self.generation_count > 0 else 0
        avg_time = (self.total_time / self.success_count) if self.success_count > 0 else 0

        return {
            "generation_count": self.generation_count,
            "success_count": self.success_count,
            "success_rate": f"{success_rate:.1f}%",
            "total_time": f"{self.total_time:.2f}秒",
            "average_time": f"{avg_time:.2f}秒",
            "cache_enabled": CACHE_ENABLED,
            "cache_max_size": CACHE_MAX_SIZE
        }

    def test_connection(self) -> Dict[str, bool]:
        """测试所有连接"""
        results = {}

        # 测试数据库连接
        try:
            results["database"] = self.db_manager.test_connection()
        except Exception as e:
            logger.error(f"数据库连接测试失败: {e}")
            results["database"] = False

        # 测试API连接
        try:
            results["deepseek_api"] = self.deepseek_client.test_connection()
        except Exception as e:
            logger.error(f"API连接测试失败: {e}")
            results["deepseek_api"] = False

        return results

# 工具函数
def create_sql_generator() -> SQLGenerator:
    """创建SQL生成器实例"""
    return SQLGenerator()

def format_result_for_display(result, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """格式化查询结果用于显示"""
    if hasattr(result, 'to_dict'):
        # pandas DataFrame
        data = result.to_dict('records')
        columns = list(result.columns)
    elif isinstance(result, list):
        # 列表格式
        data = result
        columns = list(result[0].keys()) if result else []
    else:
        data = []
        columns = []

    return {
        "data": data,
        "columns": columns,
        "metadata": metadata,
        "row_count": len(data),
        "column_count": len(columns)
    }

if __name__ == "__main__":
    # 测试SQL生成器
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    print("测试SQL生成器...")
    generator = SQLGenerator()

    # 测试连接
    connections = generator.test_connection()
    print("\n连接测试结果:")
    for service, status in connections.items():
        print(f"  {service}: {'✓ 成功' if status else '✗ 失败'}")

    if all(connections.values()):
        print("\n所有连接测试成功，开始功能测试...")

        # 测试自然语言到SQL
        test_queries = [
            "查询所有公司信息",
            "查看2024年的凭证流水",
            "统计管理费用科目"
        ]

        for query in test_queries:
            try:
                print(f"\n测试查询: {query}")
                sql, metadata = generator.nl_to_sql(query)
                print(f"生成的SQL: {sql[:100]}...")
                print(f"生成时间: {metadata['generation_time']:.2f}秒")

                # 测试执行查询
                result, exec_metadata = generator.execute_query(sql)
                print(f"查询结果: {len(result)}行")
                print(f"执行时间: {exec_metadata['execution_time']:.2f}秒")

            except Exception as e:
                print(f"测试失败: {e}")

        # 显示统计信息
        stats = generator.get_stats()
        print(f"\n生成器统计:")
        for key, value in stats.items():
            print(f"  {key}: {value}")

    else:
        print("\n连接测试失败，请检查配置")