"""
数据库连接管理

提供数据库连接池、连接管理和连接健康检查功能。
"""

import sqlite3
import re
from pathlib import Path
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
import logging
import threading
import time

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """数据库连接管理器"""

    def __init__(self, db_path: str, pool_size: int = 5, timeout: int = 30):
        """
        初始化数据库连接管理器

        Args:
            db_path: 数据库文件路径
            pool_size: 连接池大小
            timeout: 连接超时时间（秒）
        """
        self.db_path = Path(db_path)
        self.pool_size = pool_size
        self.timeout = timeout
        self._connection_pool: List[sqlite3.Connection] = []
        self._lock = threading.Lock()
        self._initialized = False

        # 验证数据库文件
        self._validate_database()

    def _validate_database(self):
        """验证数据库文件"""
        if not self.db_path.exists():
            raise FileNotFoundError(f"数据库文件不存在: {self.db_path}")

        if not self.db_path.is_file():
            raise ValueError(f"数据库路径不是文件: {self.db_path}")

        # 测试连接
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=self.timeout)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            conn.close()

            if not tables:
                logger.warning(f"数据库中没有表: {self.db_path}")
            else:
                logger.info(f"数据库验证通过，发现 {len(tables)} 个表")

        except sqlite3.Error as e:
            raise ConnectionError(f"数据库连接失败: {e}")

    def initialize(self):
        """初始化连接池"""
        if self._initialized:
            return

        with self._lock:
            try:
                for i in range(self.pool_size):
                    conn = self._create_connection()
                    self._connection_pool.append(conn)

                self._initialized = True
                logger.info(f"数据库连接池初始化完成，大小: {self.pool_size}")

            except Exception as e:
                logger.error(f"连接池初始化失败: {e}")
                self._close_all_connections()
                raise

    def _create_connection(self) -> sqlite3.Connection:
        """创建新的数据库连接"""
        try:
            conn = sqlite3.connect(
                str(self.db_path),
                timeout=self.timeout,
                check_same_thread=False  # 允许多线程使用
            )
            # 启用外键约束
            conn.execute("PRAGMA foreign_keys = ON")
            # 设置行工厂为字典
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.Error as e:
            logger.error(f"创建数据库连接失败: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """
        获取数据库连接上下文管理器

        Yields:
            sqlite3.Connection: 数据库连接

        Raises:
            ConnectionError: 获取连接失败
        """
        if not self._initialized:
            self.initialize()

        conn = None
        start_time = time.time()

        try:
            # 等待获取连接
            while time.time() - start_time < self.timeout:
                with self._lock:
                    if self._connection_pool:
                        conn = self._connection_pool.pop()
                        break

                if conn is None:
                    time.sleep(0.1)  # 短暂等待后重试

            if conn is None:
                raise ConnectionError("获取数据库连接超时")

            # 检查连接是否有效
            if not self._check_connection(conn):
                logger.warning("连接无效，创建新连接")
                conn = self._create_connection()

            yield conn

        except Exception as e:
            logger.error(f"获取数据库连接失败: {e}")
            raise ConnectionError(f"获取数据库连接失败: {e}")

        finally:
            # 归还连接到池中
            if conn is not None:
                with self._lock:
                    if self._check_connection(conn):
                        self._connection_pool.append(conn)
                    else:
                        logger.warning("连接无效，关闭并创建新连接")
                        try:
                            conn.close()
                        except:
                            pass
                        new_conn = self._create_connection()
                        self._connection_pool.append(new_conn)

    def _check_connection(self, conn: sqlite3.Connection) -> bool:
        """检查连接是否有效"""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except sqlite3.Error:
            return False

    def _is_valid_table_name(self, table_name: str) -> bool:
        """验证表名是否安全"""
        # 只允许字母、数字、下划线
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
            return False

        # 防止SQL注入
        forbidden_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'CREATE', 'ALTER']
        table_name_upper = table_name.upper()
        for keyword in forbidden_keywords:
            if keyword in table_name_upper:
                return False

        return True

    def execute_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        执行查询并返回结果

        Args:
            sql: SQL查询语句
            params: 查询参数

        Returns:
            List[Dict[str, Any]]: 查询结果列表

        Raises:
            sqlite3.Error: SQL执行错误
        """
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                # 获取列名
                column_names = [description[0] for description in cursor.description]

                # 转换为字典列表
                results = []
                for row in cursor.fetchall():
                    result_dict = {}
                    for i, column_name in enumerate(column_names):
                        result_dict[column_name] = row[i]
                    results.append(result_dict)

                logger.debug(f"查询执行成功，返回 {len(results)} 条记录")
                return results

            except sqlite3.Error as e:
                logger.error(f"SQL执行失败: {e}\nSQL: {sql}\n参数: {params}")
                raise

    def execute_update(self, sql: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        执行更新操作（INSERT, UPDATE, DELETE）

        Args:
            sql: SQL语句
            params: 参数

        Returns:
            int: 受影响的行数

        Raises:
            sqlite3.Error: SQL执行错误
        """
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                conn.commit()
                affected_rows = cursor.rowcount
                logger.debug(f"更新执行成功，影响 {affected_rows} 行")
                return affected_rows

            except sqlite3.Error as e:
                conn.rollback()
                logger.error(f"更新执行失败: {e}\nSQL: {sql}\n参数: {params}")
                raise

    def execute_many(self, sql: str, params_list: List[Dict[str, Any]]) -> int:
        """
        批量执行SQL语句

        Args:
            sql: SQL语句
            params_list: 参数列表

        Returns:
            int: 总受影响行数

        Raises:
            sqlite3.Error: SQL执行错误
        """
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                total_affected = 0

                for params in params_list:
                    cursor.execute(sql, params)
                    total_affected += cursor.rowcount

                conn.commit()
                logger.debug(f"批量执行成功，总影响 {total_affected} 行")
                return total_affected

            except sqlite3.Error as e:
                conn.rollback()
                logger.error(f"批量执行失败: {e}\nSQL: {sql}")
                raise

    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """
        获取表结构信息

        Args:
            table_name: 表名

        Returns:
            List[Dict[str, Any]]: 表结构信息
        """
        # PRAGMA语句不支持参数化查询，需要直接拼接表名
        # 但需要确保表名是安全的
        if not self._is_valid_table_name(table_name):
            raise ValueError(f"无效的表名: {table_name}")

        sql = f"PRAGMA table_info({table_name})"
        return self.execute_query(sql)

    def get_all_tables(self) -> List[str]:
        """
        获取所有表名

        Returns:
            List[str]: 表名列表
        """
        sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        results = self.execute_query(sql)
        return [row["name"] for row in results]

    def get_database_stats(self) -> Dict[str, Any]:
        """
        获取数据库统计信息

        Returns:
            Dict[str, Any]: 统计信息
        """
        stats = {}

        # 获取所有表
        tables = self.get_all_tables()
        stats["table_count"] = len(tables)

        # 获取每个表的记录数
        table_stats = {}
        for table in tables:
            try:
                sql = f"SELECT COUNT(*) as count FROM {table}"
                result = self.execute_query(sql)
                if result:
                    table_stats[table] = result[0]["count"]
            except:
                table_stats[table] = "error"

        stats["table_record_counts"] = table_stats

        # 获取数据库大小
        db_size = self.db_path.stat().st_size if self.db_path.exists() else 0
        stats["database_size_bytes"] = db_size
        stats["database_size_mb"] = db_size / (1024 * 1024)

        return stats

    def _close_all_connections(self):
        """关闭所有连接"""
        with self._lock:
            for conn in self._connection_pool:
                try:
                    conn.close()
                except:
                    pass
            self._connection_pool.clear()
            self._initialized = False

    def close(self):
        """关闭连接池"""
        self._close_all_connections()
        logger.info("数据库连接池已关闭")

    def __enter__(self):
        """上下文管理器入口"""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()

    def __del__(self):
        """析构函数"""
        try:
            self.close()
        except:
            pass


# 全局数据库连接实例
_db_connection: Optional[DatabaseConnection] = None


def get_database_connection(db_path: Optional[str] = None, **kwargs) -> DatabaseConnection:
    """
    获取数据库连接实例

    Args:
        db_path: 数据库文件路径，如果为None则使用默认配置
        **kwargs: 其他连接参数

    Returns:
        DatabaseConnection: 数据库连接实例
    """
    global _db_connection

    if _db_connection is None:
        if db_path is None:
            # 从配置加载
            from ..utils.config_loader import load_config
            config = load_config()
            db_path = config.database.path

        _db_connection = DatabaseConnection(db_path, **kwargs)
        _db_connection.initialize()

    return _db_connection


def close_database_connection():
    """关闭数据库连接"""
    global _db_connection
    if _db_connection is not None:
        _db_connection.close()
        _db_connection = None


# 导出
__all__ = [
    "DatabaseConnection",
    "get_database_connection",
    "close_database_connection",
]