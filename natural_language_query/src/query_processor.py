"""
查询处理器

安全SQL执行器包装器，集成SQL生成器和查询执行器。
提供从自然语言查询到查询结果的完整流程。
"""

import logging
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import time

from .sql_generator.core import (
    get_sql_generator, GenerationMethod, GeneratedSQL, QueryType
)
from .database.query_executor import (
    get_query_executor, execute_query, QueryResult, QueryStats
)
from .database.schema import get_schema_manager, DatabaseSchema
from .utils.config_loader import load_config

logger = logging.getLogger(__name__)


class ProcessingMethod(Enum):
    """处理方法"""
    LLM = "llm"   # 仅使用LLM


@dataclass
class QueryProcessingResult:
    """查询处理结果"""
    # 原始查询信息
    original_query: str
    processing_method: ProcessingMethod

    # SQL生成信息
    sql_generation: Optional[GeneratedSQL] = None
    sql_generation_time: float = 0.0

    # 查询执行信息
    query_execution: Optional[QueryResult] = None
    query_execution_time: float = 0.0

    # 总体信息
    total_time: float = 0.0
    success: bool = False
    error_message: Optional[str] = None

    # 统计信息
    cache_hit: bool = False
    confidence: float = 0.0  # 总体置信度

    def is_valid(self) -> bool:
        """检查结果是否有效"""
        return self.success and self.sql_generation and self.sql_generation.is_valid()

    def get_data(self) -> List[Dict[str, Any]]:
        """获取查询数据"""
        if self.query_execution and self.query_execution.success:
            return self.query_execution.data
        return []

    def get_row_count(self) -> int:
        """获取行数"""
        if self.query_execution and self.query_execution.success:
            return self.query_execution.row_count
        return 0

    def get_columns(self) -> List[str]:
        """获取列名"""
        if self.query_execution and self.query_execution.success:
            return self.query_execution.columns
        return []

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            "original_query": self.original_query,
            "processing_method": self.processing_method.value,
            "success": self.success,
            "total_time": self.total_time,
            "confidence": self.confidence,
            "cache_hit": self.cache_hit,
            "row_count": self.get_row_count(),
        }

        if self.sql_generation:
            result.update({
                "generated_sql": self.sql_generation.sql,
                "sql_generation_method": self.sql_generation.method.value,
                "sql_generation_time": self.sql_generation_time,
                "sql_confidence": self.sql_generation.confidence,
                "tables_involved": self.sql_generation.tables_involved,
                "columns_involved": self.sql_generation.columns_involved,
            })

        if self.query_execution:
            result.update({
                "query_execution_time": self.query_execution_time,
                "execution_success": self.query_execution.success,
                "cached": self.query_execution.cached,
            })

            if self.query_execution.error_message:
                result["execution_error"] = self.query_execution.error_message

        if self.error_message:
            result["error_message"] = self.error_message

        return result

    def print_summary(self):
        """打印结果摘要"""
        print("=" * 60)
        print("查询处理结果摘要")
        print("=" * 60)
        print(f"原始查询: {self.original_query}")
        print(f"处理方法: {self.processing_method.value}")
        print(f"成功: {'✅' if self.success else '❌'}")
        print(f"总耗时: {self.total_time:.3f}秒")
        print(f"置信度: {self.confidence:.2f}")

        if self.sql_generation:
            print(f"生成的SQL: {self.sql_generation.sql}")
            print(f"SQL生成方法: {self.sql_generation.method.value}")
            print(f"SQL生成耗时: {self.sql_generation_time:.3f}秒")
            print(f"SQL置信度: {self.sql_generation.confidence:.2f}")
            print(f"涉及的表: {', '.join(self.sql_generation.tables_involved)}")

        if self.query_execution:
            print(f"查询执行耗时: {self.query_execution_time:.3f}秒")
            print(f"缓存命中: {'✅' if self.query_execution.cached else '❌'}")
            print(f"返回行数: {self.get_row_count()}")

            if self.query_execution.error_message:
                print(f"执行错误: {self.query_execution.error_message}")

        if self.error_message:
            print(f"处理错误: {self.error_message}")

        print("=" * 60)


class QueryProcessor:
    """查询处理器"""

    def __init__(self,
                 enable_cache: bool = True,
                 enable_security_check: bool = False):  # 默认禁用安全检查
        """
        初始化查询处理器

        Args:
            enable_cache: 是否启用缓存
            enable_security_check: 是否启用安全检查
        """
        self.enable_cache = enable_cache
        self.enable_security_check = enable_security_check

        # 初始化组件
        self.sql_generator = get_sql_generator()
        self.query_executor = get_query_executor(
            enable_cache=enable_cache,
            enable_security_check=enable_security_check
        )
        self.schema_manager = get_schema_manager()
        self.config = load_config()

        # 统计信息
        self.total_queries = 0
        self.successful_queries = 0
        self.failed_queries = 0
        self.total_processing_time = 0.0

        logger.info("查询处理器初始化完成，使用LLM生成SQL")

    def _determine_generation_method(self, query: str) -> GenerationMethod:
        """确定SQL生成方法"""
        # 现在只有LLM一种方法
        return GenerationMethod.LLM

    def _validate_generated_sql(self, generated_sql: GeneratedSQL) -> bool:
        """验证生成的SQL"""
        if not generated_sql.is_valid():
            logger.warning(f"生成的SQL无效: {generated_sql.error_message}")
            return False

        # 检查SQL是否为空或太短
        if len(generated_sql.sql.strip()) < 10:
            logger.warning(f"生成的SQL太短: {generated_sql.sql}")
            return False

        # 检查置信度
        if generated_sql.confidence < 0.3:
            logger.warning(f"SQL置信度过低: {generated_sql.confidence}")
            return False

        return True

    def _extract_query_params(self, generated_sql: GeneratedSQL) -> Optional[Dict[str, Any]]:
        """从生成的SQL中提取查询参数"""
        # 这里可以添加参数提取逻辑
        # 例如：从查询中提取日期范围、数值范围等
        return None

    def process_query(self, query: str, use_cache: bool = True) -> QueryProcessingResult:
        """
        处理自然语言查询

        Args:
            query: 自然语言查询
            use_cache: 是否使用缓存

        Returns:
            QueryProcessingResult: 查询处理结果
        """
        start_time = time.time()
        result = QueryProcessingResult(
            original_query=query,
            processing_method=ProcessingMethod.LLM,
            success=False
        )

        try:
            self.total_queries += 1
            logger.info(f"开始处理查询: {query}")

            # 1. 确定生成方法
            generation_method = self._determine_generation_method(query)
            logger.debug(f"使用生成方法: {generation_method.value}")

            # 2. 生成SQL
            sql_gen_start = time.time()
            generated_sql = self.sql_generator.generate_sql(
                query=query,
                use_cache=use_cache
            )
            result.sql_generation_time = time.time() - sql_gen_start
            result.sql_generation = generated_sql

            # 3. 验证生成的SQL
            if not self._validate_generated_sql(generated_sql):
                result.error_message = f"SQL生成失败: {generated_sql.error_message}"
                logger.error(result.error_message)
                self.failed_queries += 1
                return result

            logger.info(f"生成的SQL: {generated_sql.sql}")
            logger.info(f"SQL置信度: {generated_sql.confidence:.2f}")

            # 4. 提取查询参数（如果有）
            query_params = self._extract_query_params(generated_sql)

            # 5. 执行查询
            query_exec_start = time.time()
            query_result = self.query_executor.execute(
                sql=generated_sql.sql,
                params=query_params,
                use_cache=use_cache
            )
            result.query_execution_time = time.time() - query_exec_start
            result.query_execution = query_result

            # 6. 更新结果
            result.total_time = time.time() - start_time
            result.success = query_result.success
            result.confidence = generated_sql.confidence * (0.7 if query_result.success else 0.3)
            result.cache_hit = query_result.cached if query_result else False

            if query_result.success:
                self.successful_queries += 1
                logger.info(f"查询处理成功: {result.get_row_count()} 行, 总耗时: {result.total_time:.3f}秒")
            else:
                self.failed_queries += 1
                result.error_message = query_result.error_message
                logger.error(f"查询执行失败: {query_result.error_message}")

        except Exception as e:
            result.total_time = time.time() - start_time
            result.error_message = str(e)
            self.failed_queries += 1
            logger.error(f"查询处理异常: {e}", exc_info=True)

        self.total_processing_time += result.total_time
        return result

    def batch_process(self, queries: List[str], use_cache: bool = True) -> List[QueryProcessingResult]:
        """批量处理查询"""
        results = []
        for i, query in enumerate(queries):
            logger.info(f"处理查询 {i+1}/{len(queries)}: {query}")
            result = self.process_query(query, use_cache)
            results.append(result)
        return results

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        query_stats = self.query_executor.get_stats()

        return {
            "total_queries": self.total_queries,
            "successful_queries": self.successful_queries,
            "failed_queries": self.failed_queries,
            "success_rate": self.successful_queries / max(self.total_queries, 1),
            "total_processing_time": self.total_processing_time,
            "avg_processing_time": self.total_processing_time / max(self.total_queries, 1),
            "query_execution_stats": {
                "total_queries": query_stats.total_queries,
                "successful_queries": query_stats.successful_queries,
                "failed_queries": query_stats.failed_queries,
                "cache_hits": query_stats.cache_hits,
                "cache_misses": query_stats.cache_misses,
            }
        }

    def print_stats(self):
        """打印统计信息"""
        stats = self.get_stats()

        print("=" * 60)
        print("查询处理器统计")
        print("=" * 60)
        print(f"总查询次数: {stats['total_queries']}")
        print(f"成功查询: {stats['successful_queries']}")
        print(f"失败查询: {stats['failed_queries']}")
        print(f"成功率: {stats['success_rate'] * 100:.1f}%")
        print(f"总处理时间: {stats['total_processing_time']:.3f}秒")
        print(f"平均处理时间: {stats['avg_processing_time']:.3f}秒")

        query_stats = stats['query_execution_stats']
        print("-" * 60)
        print("查询执行统计:")
        print(f"  总查询次数: {query_stats['total_queries']}")
        print(f"  成功查询: {query_stats['successful_queries']}")
        print(f"  失败查询: {query_stats['failed_queries']}")
        print(f"  缓存命中: {query_stats['cache_hits']}")
        print(f"  缓存未命中: {query_stats['cache_misses']}")
        print("=" * 60)

    def clear_cache(self):
        """清空缓存"""
        self.query_executor.clear_cache()
        logger.info("查询处理器缓存已清空")


# 全局查询处理器实例
_query_processor: Optional[QueryProcessor] = None


def get_query_processor(
    enable_cache: bool = True,
    enable_security_check: bool = True
) -> QueryProcessor:
    """获取查询处理器实例"""
    global _query_processor
    if _query_processor is None:
        _query_processor = QueryProcessor(
            enable_cache=enable_cache,
            enable_security_check=enable_security_check
        )
    return _query_processor


def process_query(
    query: str,
    use_cache: bool = True
) -> QueryProcessingResult:
    """处理查询"""
    processor = get_query_processor()
    return processor.process_query(query, use_cache)


def batch_process(
    queries: List[str],
    use_cache: bool = True
) -> List[QueryProcessingResult]:
    """批量处理查询"""
    processor = get_query_processor()
    return processor.batch_process(queries, use_cache)


def get_processor_stats() -> Dict[str, Any]:
    """获取处理器统计"""
    processor = get_query_processor()
    return processor.get_stats()


def print_processor_stats():
    """打印处理器统计"""
    processor = get_query_processor()
    processor.print_stats()


def clear_processor_cache():
    """清空处理器缓存"""
    processor = get_query_processor()
    processor.clear_cache()


# 导出
__all__ = [
    "ProcessingMethod",
    "QueryProcessingResult",
    "QueryProcessor",
    "get_query_processor",
    "process_query",
    "batch_process",
    "get_processor_stats",
    "print_processor_stats",
    "clear_processor_cache",
]