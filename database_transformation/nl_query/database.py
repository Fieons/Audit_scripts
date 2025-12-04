"""
数据库连接和管理模块
处理SQLite数据库连接、查询执行和结果格式化
"""

import sqlite3
import pandas as pd
from typing import Optional, List, Dict, Any, Tuple
import logging
from pathlib import Path
import time

from config import DATABASE_PATH, MAX_RESULTS, QUERY_TIMEOUT

logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """数据库相关错误"""
    pass

class DatabaseManager:
    """数据库管理器"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or DATABASE_PATH
        self._connection = None
        self._cursor = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """建立数据库连接"""
        try:
            if not Path(self.db_path).exists():
                raise DatabaseError(f"数据库文件不存在: {self.db_path}")

            self._connection = sqlite3.connect(self.db_path)
            self._connection.row_factory = sqlite3.Row  # 返回字典格式的结果
            self._cursor = self._connection.cursor()
            logger.info(f"成功连接到数据库: {self.db_path}")

        except sqlite3.Error as e:
            logger.error(f"数据库连接失败: {e}")
            raise DatabaseError(f"数据库连接失败: {e}")

    def close(self):
        """关闭数据库连接"""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
            logger.info("数据库连接已关闭")

    def execute_query(self, sql: str, params: tuple = None, limit: int = None) -> pd.DataFrame:
        """
        执行SQL查询并返回结果

        Args:
            sql: SQL查询语句
            params: 查询参数
            limit: 结果限制（覆盖配置中的MAX_RESULTS）

        Returns:
            pandas DataFrame包含查询结果
        """
        start_time = time.time()

        try:
            # 验证SQL安全性
            self._validate_sql_security(sql)

            # 添加LIMIT子句（如果不存在）
            sql_with_limit = self._add_limit_clause(sql, limit or MAX_RESULTS)

            # 执行查询
            self._cursor.execute(sql_with_limit, params or ())

            # 获取结果
            rows = self._cursor.fetchall()
            columns = [description[0] for description in self._cursor.description] if self._cursor.description else []

            # 转换为DataFrame
            df = pd.DataFrame(rows, columns=columns)

            # 记录查询统计
            elapsed_time = time.time() - start_time
            logger.info(f"查询执行成功: {len(df)}行, 耗时: {elapsed_time:.2f}秒")

            return df

        except sqlite3.Error as e:
            logger.error(f"SQL执行错误: {e}")
            raise DatabaseError(f"SQL执行错误: {e}")

        except Exception as e:
            logger.error(f"查询执行失败: {e}")
            raise DatabaseError(f"查询执行失败: {e}")

    def get_schema_info(self) -> Dict[str, Any]:
        """
        获取数据库schema信息

        Returns:
            包含表结构信息的字典
        """
        try:
            schema_info = {
                "tables": [],
                "total_tables": 0,
                "database_info": {}
            }

            # 获取所有表
            self._cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in self._cursor.fetchall()]

            for table_name in tables:
                # 获取表结构
                self._cursor.execute(f"PRAGMA table_info({table_name})")
                columns = self._cursor.fetchall()

                # 获取索引信息
                self._cursor.execute(f"PRAGMA index_list({table_name})")
                indexes = self._cursor.fetchall()

                table_info = {
                    "name": table_name,
                    "columns": [],
                    "indexes": [],
                    "row_count": self._get_table_row_count(table_name)
                }

                # 处理列信息
                for col in columns:
                    column_info = {
                        "cid": col[0],
                        "name": col[1],
                        "type": col[2],
                        "notnull": bool(col[3]),
                        "default": col[4],
                        "pk": bool(col[5])
                    }
                    table_info["columns"].append(column_info)

                # 处理索引信息
                for idx in indexes:
                    index_info = {
                        "seq": idx[0],
                        "name": idx[1],
                        "unique": bool(idx[2]),
                        "origin": idx[3],
                        "partial": bool(idx[4])
                    }
                    table_info["indexes"].append(index_info)

                schema_info["tables"].append(table_info)

            schema_info["total_tables"] = len(tables)

            # 获取数据库信息
            self._cursor.execute("SELECT sqlite_version()")
            schema_info["database_info"] = {
                "sqlite_version": self._cursor.fetchone()[0],
                "database_path": self.db_path
            }

            logger.info(f"成功获取schema信息: {len(tables)}张表")
            return schema_info

        except sqlite3.Error as e:
            logger.error(f"获取schema信息失败: {e}")
            raise DatabaseError(f"获取schema信息失败: {e}")

    def get_query_examples(self) -> List[Dict[str, str]]:
        """
        获取查询示例（基于数据查询指引.md）

        Returns:
            查询示例列表
        """
        examples = [
            {
                "title": "查询所有公司",
                "description": "获取数据库中的所有公司信息",
                "sql": "SELECT id, name, code FROM companies ORDER BY id;",
                "natural_language": "查询所有公司信息"
            },
            {
                "title": "查询凭证流水",
                "description": "按日期倒序查询凭证流水，可指定公司",
                "sql": """SELECT c.name as 公司, v.voucher_number as 凭证号, v.voucher_date as 日期,
       v.voucher_type as 类型, v.total_debit as 借方合计, v.total_credit as 贷方合计
FROM vouchers v
JOIN account_books ab ON v.book_id = ab.id
JOIN companies c ON ab.company_id = c.id
WHERE c.name LIKE '%和立%'
ORDER BY v.voucher_date DESC
LIMIT 100;""",
                "natural_language": "查询和立公司的凭证流水，按日期倒序排列"
            },
            {
                "title": "查询科目余额表",
                "description": "统计科目发生额和涉及凭证数",
                "sql": """SELECT s.code as 科目编码, s.name as 科目名称, s.subject_type as 科目类型,
       SUM(vd.debit_amount) as 借方发生额, SUM(vd.credit_amount) as 贷方发生额,
       COUNT(DISTINCT v.id) as 涉及凭证数
FROM voucher_details vd
JOIN account_subjects s ON vd.subject_id = s.id
JOIN vouchers v ON vd.voucher_id = v.id
WHERE s.code LIKE '6602%'
GROUP BY s.code, s.name, s.subject_type
ORDER BY 借方发生额 DESC;""",
                "natural_language": "查询管理费用科目的余额表"
            },
            {
                "title": "大额交易检测",
                "description": "检测100万以上的大额交易",
                "sql": """SELECT v.voucher_number as 凭证号, v.voucher_date as 日期, c.name as 公司,
       v.voucher_type as 类型, vd.summary as 摘要,
       vd.debit_amount as 借方金额, vd.credit_amount as 贷方金额,
       s.name as 科目名称
FROM voucher_details vd
JOIN vouchers v ON vd.voucher_id = v.id
JOIN account_subjects s ON vd.subject_id = s.id
JOIN account_books ab ON v.book_id = ab.id
JOIN companies c ON ab.company_id = c.id
WHERE (vd.debit_amount > 1000000 OR vd.credit_amount > 1000000)
    AND v.voucher_date >= '2024-01-01'
ORDER BY CASE
    WHEN vd.debit_amount > vd.credit_amount THEN vd.debit_amount
    ELSE vd.credit_amount
END DESC;""",
                "natural_language": "查找2024年100万以上的大额交易"
            }
        ]

        return examples

    def _validate_sql_security(self, sql: str):
        """验证SQL安全性"""
        sql_upper = sql.upper()

        # 检查是否包含禁止的关键字
        from config import FORBIDDEN_SQL_KEYWORDS
        for keyword in FORBIDDEN_SQL_KEYWORDS:
            if keyword in sql_upper:
                raise DatabaseError(f"SQL包含禁止的操作: {keyword}")

        # 检查是否以SELECT开头（允许WITH开头的CTE）
        if not (sql_upper.strip().startswith("SELECT") or sql_upper.strip().startswith("WITH")):
            raise DatabaseError("只允许SELECT查询")

    def _add_limit_clause(self, sql: str, limit: int) -> str:
        """为SQL添加LIMIT子句（如果不存在）"""
        sql_upper = sql.upper()

        # 如果已经有LIMIT，不添加
        if "LIMIT" in sql_upper:
            return sql

        # 添加LIMIT子句
        # 处理分号
        sql = sql.rstrip(";")
        return f"{sql} LIMIT {limit};"

    def _get_table_row_count(self, table_name: str) -> int:
        """获取表的行数"""
        try:
            self._cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            return self._cursor.fetchone()[0]
        except:
            return 0

    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            self.connect()
            self._cursor.execute("SELECT 1")
            result = self._cursor.fetchone()[0]
            return result == 1
        except:
            return False
        finally:
            self.close()

# 工具函数
def format_schema_for_prompt(schema_info: Dict[str, Any]) -> str:
    """将schema信息格式化为提示词"""
    prompt = "数据库结构信息：\n\n"

    for table in schema_info["tables"]:
        prompt += f"表名: {table['name']} (行数: {table['row_count']})\n"
        prompt += "字段:\n"

        for col in table["columns"]:
            pk_marker = " [主键]" if col["pk"] else ""
            notnull_marker = " [非空]" if col["notnull"] else ""
            default_marker = f" [默认值: {col['default']}]" if col["default"] else ""

            prompt += f"  - {col['name']}: {col['type']}{pk_marker}{notnull_marker}{default_marker}\n"

        if table["indexes"]:
            prompt += "索引:\n"
            for idx in table["indexes"]:
                unique_marker = " [唯一]" if idx["unique"] else ""
                prompt += f"  - {idx['name']}{unique_marker}\n"

        prompt += "\n"

    prompt += f"数据库版本: {schema_info['database_info']['sqlite_version']}\n"
    prompt += f"总表数: {schema_info['total_tables']}\n"

    return prompt

def format_examples_for_prompt(examples: List[Dict[str, str]]) -> str:
    """将查询示例格式化为提示词"""
    prompt = "查询示例：\n\n"

    for i, example in enumerate(examples, 1):
        prompt += f"示例 {i}: {example['title']}\n"
        prompt += f"描述: {example['description']}\n"
        prompt += f"自然语言查询: {example['natural_language']}\n"
        prompt += f"对应SQL:\n```sql\n{example['sql']}\n```\n\n"

    return prompt

if __name__ == "__main__":
    # 测试数据库连接
    db = DatabaseManager()

    try:
        if db.test_connection():
            print("数据库连接测试成功")

            # 测试schema信息获取
            schema = db.get_schema_info()
            print(f"数据库包含 {schema['total_tables']} 张表")

            # 测试查询执行
            df = db.execute_query("SELECT name FROM companies LIMIT 5")
            print(f"查询结果: {len(df)} 行")
            print(df)

        else:
            print("数据库连接测试失败")

    except Exception as e:
        print(f"测试失败: {e}")