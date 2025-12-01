"""
查询执行器

安全执行SQL查询，提供参数化查询、结果缓存、性能监控等功能。
"""

import re
import time
import hashlib
import json
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime
import logging
from functools import lru_cache

from .connection import DatabaseConnection, get_database_connection
from .schema import SchemaManager, get_schema_manager
from ..utils.config_loader import load_config

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """查询结果"""
    success: bool
    data: List[Dict[str, Any]] = field(default_factory=list)
    columns: List[str] = field(default_factory=list)
    row_count: int = 0
    execution_time: float = 0.0
    error_message: Optional[str] = None
    query_hash: Optional[str] = None
    cached: bool = False


@dataclass
class QueryStats:
    """查询统计"""
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    total_execution_time: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0
    avg_execution_time: float = 0.0


class QueryExecutor:
    """查询执行器"""

    # SQL注入检测模式
    SQL_INJECTION_PATTERNS = [
        r"(?i)\b(union\s+select|union\s+all\s+select)\b",
        r"(?i)\b(insert\s+into|update\s+.*set|delete\s+from)\b",
        r"(?i)\b(drop\s+table|truncate\s+table|alter\s+table)\b",
        r"(?i)\b(create\s+table|create\s+index|create\s+view)\b",
        r"(?i)\b(grant\s+|revoke\s+|deny\s+)\b",
        r"(?i)\b(exec\s+|execute\s+|sp_executesql)\b",
        r"(?i)\b(xp_cmdshell|sp_oacreate|sp_oamethod)\b",
        r"(?i)\b(select\s+.*from\s+.*where\s+.*=\s*['\"]?\s*['\"]?\s*or\s+.*=)",
        r"(?i)\b(select\s+.*from\s+.*where\s+1\s*=\s*1)\b",
        r"(?i)\b(--|\/\*|\*\/|;)\s*$",
    ]

    def __init__(self, db_connection: Optional[DatabaseConnection] = None,
                 enable_cache: bool = True, enable_security_check: bool = True):
        """
        初始化查询执行器

        Args:
            db_connection: 数据库连接
            enable_cache: 是否启用查询缓存
            enable_security_check: 是否启用安全检查
        """
        self.db_connection = db_connection or get_database_connection()
        self.schema_manager = get_schema_manager(self.db_connection)
        self.enable_cache = enable_cache
        self.enable_security_check = enable_security_check
        self.query_cache: Dict[str, Tuple[QueryResult, float]] = {}
        self.stats = QueryStats()
        self.config = load_config()

        # 初始化缓存
        if self.enable_cache:
            self._init_cache()

    def _init_cache(self):
        """初始化缓存"""
        maxsize = self.config.query.cache_size
        self._cached_execute = lru_cache(maxsize=maxsize)(self._execute_uncached)

    def _get_query_hash(self, sql: str, params: Optional[Dict[str, Any]] = None) -> str:
        """获取查询哈希值"""
        query_str = sql
        if params:
            # 对参数进行排序以确保一致性
            sorted_params = json.dumps(params, sort_keys=True)
            query_str += sorted_params

        return hashlib.md5(query_str.encode('utf-8')).hexdigest()

    def _check_sql_injection(self, sql: str, params: Optional[Dict[str, Any]] = None) -> bool:
        """检查SQL注入"""
        if not self.enable_security_check:
            return True

        # 检查SQL语句
        for pattern in self.SQL_INJECTION_PATTERNS:
            if re.search(pattern, sql, re.IGNORECASE):
                logger.warning(f"检测到可能的SQL注入: {pattern}")
                return False

        # 检查参数值
        if params:
            for key, value in params.items():
                if isinstance(value, str):
                    value_str = str(value).lower()
                    suspicious_keywords = ['union', 'select', 'insert', 'update', 'delete',
                                           'drop', 'truncate', 'alter', 'create', 'exec',
                                           'execute', 'xp_', 'sp_', '--', '/*', '*/', ';']
                    for keyword in suspicious_keywords:
                        if keyword in value_str:
                            logger.warning(f"参数 {key} 包含可疑内容: {keyword}")
                            return False

        return True

    def _validate_query_complexity(self, sql: str) -> bool:
        """验证查询复杂度"""
        if not self.enable_security_check:
            return True

        # 简单的复杂度检查：子查询数量、JOIN数量等
        complexity_score = 0

        # 计算子查询数量
        subquery_count = len(re.findall(r'\(\s*select\b', sql, re.IGNORECASE))
        complexity_score += subquery_count * 2

        # 计算JOIN数量
        join_count = len(re.findall(r'\bjoin\b', sql, re.IGNORECASE))
        complexity_score += join_count

        # 计算UNION数量
        union_count = len(re.findall(r'\bunion\b', sql, re.IGNORECASE))
        complexity_score += union_count * 2

        max_complexity = self.config.security.max_query_complexity
        if complexity_score > max_complexity:
            logger.warning(f"查询复杂度过高: {complexity_score} > {max_complexity}")
            return False

        return True

    def _extract_table_names(self, sql: str) -> List[str]:
        """从SQL中提取表名"""
        # 简单的表名提取（实际应用中可能需要更复杂的解析）
        table_names = []

        # 查找FROM和JOIN后面的表名
        from_matches = re.findall(r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        join_matches = re.findall(r'\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)

        table_names.extend(from_matches)
        table_names.extend(join_matches)

        # 去重
        return list(set(table_names))

    def _validate_table_access(self, sql: str) -> bool:
        """验证表访问权限"""
        table_names = self._extract_table_names(sql)
        if not table_names:
            return True

        # 检查表是否存在
        schema = self.schema_manager.load_schema()
        for table_name in table_names:
            if table_name not in schema.tables:
                logger.warning(f"表不存在: {table_name}")
                return False

        return True

    def _execute_uncached(self, sql: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """执行查询（无缓存）"""
        start_time = time.time()
        result = QueryResult(success=False)

        try:
            # 安全检查
            if not self._check_sql_injection(sql, params):
                result.error_message = "SQL注入检查失败"
                return result

            if not self._validate_query_complexity(sql):
                result.error_message = "查询复杂度超过限制"
                return result

            if not self._validate_table_access(sql):
                result.error_message = "表访问验证失败"
                return result

            # 执行查询
            data = self.db_connection.execute_query(sql, params)

            # 构建结果
            result.success = True
            result.data = data
            if data:
                result.columns = list(data[0].keys())
                result.row_count = len(data)
            else:
                result.columns = []
                result.row_count = 0

            execution_time = time.time() - start_time
            result.execution_time = execution_time

            # 更新统计
            self.stats.total_queries += 1
            self.stats.successful_queries += 1
            self.stats.total_execution_time += execution_time
            self.stats.avg_execution_time = self.stats.total_execution_time / self.stats.successful_queries

            logger.info(f"查询执行成功: {result.row_count} 行, 耗时: {execution_time:.3f}秒")

        except Exception as e:
            execution_time = time.time() - start_time
            result.execution_time = execution_time
            result.error_message = str(e)

            # 更新统计
            self.stats.total_queries += 1
            self.stats.failed_queries += 1

            logger.error(f"查询执行失败: {e}\nSQL: {sql}\n参数: {params}")

        return result

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None,
                use_cache: bool = True) -> QueryResult:
        """
        执行查询

        Args:
            sql: SQL查询语句
            params: 查询参数
            use_cache: 是否使用缓存

        Returns:
            QueryResult: 查询结果
        """
        query_hash = self._get_query_hash(sql, params)

        # 检查缓存
        if self.enable_cache and use_cache:
            cache_key = query_hash
            if cache_key in self.query_cache:
                cached_result, timestamp = self.query_cache[cache_key]
                cache_ttl = self.config.query.cache_ttl

                # 检查缓存是否过期
                if time.time() - timestamp < cache_ttl:
                    cached_result.cached = True
                    cached_result.query_hash = query_hash
                    self.stats.cache_hits += 1
                    logger.debug(f"缓存命中: {query_hash}")
                    return cached_result
                else:
                    # 缓存过期，删除
                    del self.query_cache[cache_key]

            self.stats.cache_misses += 1

        # 执行查询
        if self.enable_cache and use_cache:
            result = self._cached_execute(sql, params)
        else:
            result = self._execute_uncached(sql, params)

        result.query_hash = query_hash

        # 更新缓存
        if self.enable_cache and use_cache and result.success:
            self.query_cache[query_hash] = (result, time.time())
            logger.debug(f"查询已缓存: {query_hash}")

        return result

    def execute_update(self, sql: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        执行更新操作（INSERT, UPDATE, DELETE）

        Args:
            sql: SQL语句
            params: 参数

        Returns:
            int: 受影响的行数，失败返回-1
        """
        start_time = time.time()

        try:
            # 安全检查（对于更新操作更严格）
            if not self._check_sql_injection(sql, params):
                logger.error("更新操作SQL注入检查失败")
                return -1

            # 禁止DROP、TRUNCATE等危险操作
            dangerous_patterns = [
                r"(?i)\b(drop\s+table)\b",
                r"(?i)\b(truncate\s+table)\b",
                r"(?i)\b(alter\s+table)\b",
                r"(?i)\b(create\s+table)\b",
                r"(?i)\b(grant\s+|revoke\s+)\b",
            ]

            for pattern in dangerous_patterns:
                if re.search(pattern, sql, re.IGNORECASE):
                    logger.error(f"检测到危险操作: {pattern}")
                    return -1

            # 执行更新
            affected_rows = self.db_connection.execute_update(sql, params)

            execution_time = time.time() - start_time
            logger.info(f"更新执行成功: 影响 {affected_rows} 行, 耗时: {execution_time:.3f}秒")

            return affected_rows

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"更新执行失败: {e}\nSQL: {sql}\n参数: {params}")
            return -1

    def explain_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        解释查询执行计划

        Args:
            sql: SQL查询语句
            params: 查询参数

        Returns:
            Dict[str, Any]: 执行计划信息
        """
        explain_sql = f"EXPLAIN QUERY PLAN {sql}"

        try:
            result = self.execute(explain_sql, params, use_cache=False)
            if result.success:
                plan_info = {
                    "sql": sql,
                    "plan": result.data,
                    "summary": self._parse_explain_plan(result.data)
                }
                return plan_info
            else:
                return {"error": result.error_message}
        except Exception as e:
            return {"error": str(e)}

    def _parse_explain_plan(self, plan_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """解析EXPLAIN QUERY PLAN结果"""
        summary = {
            "scan_type": "UNKNOWN",
            "tables_used": [],
            "indexes_used": [],
            "detail": ""
        }

        for row in plan_data:
            detail = row.get("detail", "")
            if "SCAN TABLE" in detail:
                summary["scan_type"] = "TABLE SCAN"
                # 提取表名
                table_match = re.search(r'SCAN TABLE (\w+)', detail)
                if table_match:
                    summary["tables_used"].append(table_match.group(1))
            elif "SEARCH TABLE" in detail:
                summary["scan_type"] = "INDEX SEARCH"
                # 提取表名和索引
                table_match = re.search(r'SEARCH TABLE (\w+)', detail)
                index_match = re.search(r'USING INDEX (\w+)', detail)
                if table_match:
                    summary["tables_used"].append(table_match.group(1))
                if index_match:
                    summary["indexes_used"].append(index_match.group(1))
            elif "USING TEMP B-TREE" in detail:
                summary["scan_type"] = "TEMPORARY B-TREE"

            summary["detail"] += detail + "\n"

        # 去重
        summary["tables_used"] = list(set(summary["tables_used"]))
        summary["indexes_used"] = list(set(summary["indexes_used"]))

        return summary

    def clear_cache(self):
        """清空查询缓存"""
        self.query_cache.clear()
        if hasattr(self, '_cached_execute'):
            self._cached_execute.cache_clear()
        logger.info("查询缓存已清空")

    def get_cache_info(self) -> Dict[str, Any]:
        """获取缓存信息"""
        return {
            "enabled": self.enable_cache,
            "size": len(self.query_cache),
            "max_size": self.config.query.cache_size,
            "ttl": self.config.query.cache_ttl,
            "hits": self.stats.cache_hits,
            "misses": self.stats.cache_misses,
            "hit_rate": self.stats.cache_hits / max(self.stats.cache_hits + self.stats.cache_misses, 1)
        }

    def get_stats(self) -> QueryStats:
        """获取查询统计"""
        return self.stats

    def print_stats(self):
        """打印统计信息"""
        stats = self.stats
        cache_info = self.get_cache_info()

        print("=" * 60)
        print("查询执行统计")
        print("=" * 60)
        print(f"总查询次数: {stats.total_queries}")
        print(f"成功查询: {stats.successful_queries}")
        print(f"失败查询: {stats.failed_queries}")
        print(f"成功率: {stats.successful_queries / max(stats.total_queries, 1) * 100:.1f}%")
        print(f"总执行时间: {stats.total_execution_time:.3f}秒")
        print(f"平均执行时间: {stats.avg_execution_time:.3f}秒")
        print("-" * 60)
        print("缓存统计:")
        print(f"  启用: {cache_info['enabled']}")
        print(f"  当前大小: {cache_info['size']}")
        print(f"  最大大小: {cache_info['max_size']}")
        print(f"  命中次数: {cache_info['hits']}")
        print(f"  未命中次数: {cache_info['misses']}")
        print(f"  命中率: {cache_info['hit_rate'] * 100:.1f}%")
        print("=" * 60)


# 全局查询执行器实例
_query_executor: Optional[QueryExecutor] = None


def get_query_executor(db_connection: Optional[DatabaseConnection] = None,
                       enable_cache: bool = True,
                       enable_security_check: bool = True) -> QueryExecutor:
    """获取查询执行器实例"""
    global _query_executor
    if _query_executor is None:
        _query_executor = QueryExecutor(db_connection, enable_cache, enable_security_check)
    return _query_executor


def execute_query(sql: str, params: Optional[Dict[str, Any]] = None,
                  use_cache: bool = True) -> QueryResult:
    """执行查询"""
    executor = get_query_executor()
    return executor.execute(sql, params, use_cache)


def execute_update(sql: str, params: Optional[Dict[str, Any]] = None) -> int:
    """执行更新"""
    executor = get_query_executor()
    return executor.execute_update(sql, params)


def explain_query(sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """解释查询"""
    executor = get_query_executor()
    return executor.explain_query(sql, params)


def clear_query_cache():
    """清空查询缓存"""
    executor = get_query_executor()
    executor.clear_cache()


def get_query_stats() -> QueryStats:
    """获取查询统计"""
    executor = get_query_executor()
    return executor.get_stats()


def print_query_stats():
    """打印查询统计"""
    executor = get_query_executor()
    executor.print_stats()


# 导出
__all__ = [
    "QueryResult",
    "QueryStats",
    "QueryExecutor",
    "get_query_executor",
    "execute_query",
    "execute_update",
    "explain_query",
    "clear_query_cache",
    "get_query_stats",
    "print_query_stats",
]