"""
数据库模式信息

提供数据库表结构、列信息、关系等模式信息的查询和管理。
"""

import json
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
import logging

from .connection import get_database_connection

logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    """列信息"""
    name: str
    type: str
    not_null: bool
    default_value: Optional[str]
    primary_key: bool
    foreign_key: Optional[str] = None
    referenced_table: Optional[str] = None
    referenced_column: Optional[str] = None


@dataclass
class TableInfo:
    """表信息"""
    name: str
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: Dict[str, Dict[str, str]] = field(default_factory=dict)
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    estimated_row_count: int = 0


@dataclass
class DatabaseSchema:
    """数据库模式"""
    tables: Dict[str, TableInfo] = field(default_factory=dict)
    version: str = "1.0"
    last_updated: str = ""


class SchemaManager:
    """模式管理器"""

    def __init__(self, db_connection=None):
        """
        初始化模式管理器

        Args:
            db_connection: 数据库连接，如果为None则创建新连接
        """
        self.db_connection = db_connection or get_database_connection()
        self.schema: Optional[DatabaseSchema] = None
        self._cached_schema_json: Optional[str] = None

    def load_schema(self, force_reload: bool = False) -> DatabaseSchema:
        """
        加载数据库模式

        Args:
            force_reload: 是否强制重新加载

        Returns:
            DatabaseSchema: 数据库模式
        """
        if self.schema is not None and not force_reload:
            return self.schema

        logger.info("开始加载数据库模式...")
        schema = DatabaseSchema()

        try:
            # 获取所有表
            tables = self.db_connection.get_all_tables()
            logger.info(f"发现 {len(tables)} 个表")

            for table_name in tables:
                table_info = self._load_table_info(table_name)
                schema.tables[table_name] = table_info

            # 更新外键关系
            self._update_foreign_key_relations(schema)

            # 估计行数
            self._estimate_row_counts(schema)

            schema.last_updated = self._get_current_timestamp()
            self.schema = schema
            self._cached_schema_json = None  # 清除缓存

            logger.info("数据库模式加载完成")
            return schema

        except Exception as e:
            logger.error(f"加载数据库模式失败: {e}")
            raise

    def _load_table_info(self, table_name: str) -> TableInfo:
        """加载表信息"""
        table_info = TableInfo(name=table_name)

        # 获取列信息
        columns_info = self.db_connection.get_table_info(table_name)
        for col_info in columns_info:
            column = ColumnInfo(
                name=col_info["name"],
                type=col_info["type"],
                not_null=bool(col_info["notnull"]),
                default_value=col_info["dflt_value"],
                primary_key=bool(col_info["pk"])
            )
            table_info.columns[column.name] = column

            if column.primary_key:
                table_info.primary_keys.append(column.name)

        # 获取外键信息
        foreign_keys = self._get_foreign_keys(table_name)
        table_info.foreign_keys = foreign_keys

        # 获取索引信息
        indexes = self._get_indexes(table_name)
        table_info.indexes = indexes

        return table_info

    def _get_foreign_keys(self, table_name: str) -> Dict[str, Dict[str, str]]:
        """获取外键信息"""
        foreign_keys = {}
        try:
            # SQLite的PRAGMA foreign_key_list命令
            # PRAGMA不支持参数化查询，需要直接拼接表名
            sql = f"PRAGMA foreign_key_list({table_name})"
            results = self.db_connection.execute_query(sql)

            for row in results:
                fk_name = f"fk_{row['from']}"
                foreign_keys[fk_name] = {
                    "from_column": row["from"],
                    "to_table": row["table"],
                    "to_column": row["to"],
                    "on_update": row.get("on_update", ""),
                    "on_delete": row.get("on_delete", "")
                }

                # 更新列信息中的外键引用
                if row["from"] in self.schema.tables.get(table_name, TableInfo("")).columns:
                    column = self.schema.tables[table_name].columns[row["from"]]
                    column.foreign_key = fk_name
                    column.referenced_table = row["table"]
                    column.referenced_column = row["to"]

        except Exception as e:
            logger.warning(f"获取表 {table_name} 的外键信息失败: {e}")

        return foreign_keys

    def _get_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """获取索引信息"""
        indexes = []
        try:
            sql = f"PRAGMA index_list({table_name})"
            results = self.db_connection.execute_query(sql)

            for idx_info in results:
                if idx_info["origin"] == "c":  # 只关心创建的索引，不是自动创建的
                    index_name = idx_info["name"]
                    index_details = self._get_index_details(index_name)
                    indexes.append({
                        "name": index_name,
                        "unique": bool(idx_info["unique"]),
                        "columns": index_details
                    })

        except Exception as e:
            logger.warning(f"获取表 {table_name} 的索引信息失败: {e}")

        return indexes

    def _get_index_details(self, index_name: str) -> List[str]:
        """获取索引详细信息"""
        columns = []
        try:
            sql = f"PRAGMA index_info({index_name})"
            results = self.db_connection.execute_query(sql)
            columns = [row["name"] for row in results]
        except Exception as e:
            logger.warning(f"获取索引 {index_name} 的详细信息失败: {e}")

        return columns

    def _update_foreign_key_relations(self, schema: DatabaseSchema):
        """更新外键关系"""
        for table_name, table_info in schema.tables.items():
            for fk_name, fk_info in table_info.foreign_keys.items():
                from_column = fk_info["from_column"]
                to_table = fk_info["to_table"]
                to_column = fk_info["to_column"]

                # 更新源列的外键信息
                if from_column in table_info.columns:
                    column = table_info.columns[from_column]
                    column.foreign_key = fk_name
                    column.referenced_table = to_table
                    column.referenced_column = to_column

    def _estimate_row_counts(self, schema: DatabaseSchema):
        """估计每个表的行数"""
        for table_name, table_info in schema.tables.items():
            try:
                sql = f"SELECT COUNT(*) as count FROM {table_name}"
                result = self.db_connection.execute_query(sql)
                if result:
                    table_info.estimated_row_count = result[0]["count"]
            except Exception as e:
                logger.warning(f"估计表 {table_name} 的行数失败: {e}")
                table_info.estimated_row_count = -1

    def _get_current_timestamp(self) -> str:
        """获取当前时间戳"""
        from datetime import datetime
        return datetime.now().isoformat()

    def get_table(self, table_name: str) -> Optional[TableInfo]:
        """获取表信息"""
        if self.schema is None:
            self.load_schema()

        return self.schema.tables.get(table_name)

    def get_column(self, table_name: str, column_name: str) -> Optional[ColumnInfo]:
        """获取列信息"""
        table_info = self.get_table(table_name)
        if table_info:
            return table_info.columns.get(column_name)
        return None

    def get_related_tables(self, table_name: str) -> Dict[str, List[str]]:
        """
        获取相关表（外键关系）

        Returns:
            Dict[str, List[str]]: {
                "referenced_by": [被哪些表引用],
                "references_to": [引用哪些表]
            }
        """
        if self.schema is None:
            self.load_schema()

        referenced_by = []  # 被哪些表引用
        references_to = []  # 引用哪些表

        # 查找引用此表的表
        for other_table_name, other_table_info in self.schema.tables.items():
            if other_table_name == table_name:
                continue

            for fk_info in other_table_info.foreign_keys.values():
                if fk_info["to_table"] == table_name:
                    referenced_by.append(other_table_name)

        # 查找此表引用的表
        table_info = self.get_table(table_name)
        if table_info:
            for fk_info in table_info.foreign_keys.values():
                references_to.append(fk_info["to_table"])

        return {
            "referenced_by": list(set(referenced_by)),
            "references_to": list(set(references_to))
        }

    def validate_query_tables(self, table_names: List[str]) -> bool:
        """验证查询中涉及的表是否存在"""
        if self.schema is None:
            self.load_schema()

        for table_name in table_names:
            if table_name not in self.schema.tables:
                return False
        return True

    def validate_query_columns(self, table_name: str, column_names: List[str]) -> bool:
        """验证查询中涉及的列是否存在"""
        table_info = self.get_table(table_name)
        if not table_info:
            return False

        for column_name in column_names:
            if column_name not in table_info.columns:
                return False
        return True

    def get_schema_summary(self) -> Dict[str, Any]:
        """获取模式摘要"""
        if self.schema is None:
            self.load_schema()

        summary = {
            "table_count": len(self.schema.tables),
            "total_columns": 0,
            "total_indexes": 0,
            "total_foreign_keys": 0,
            "tables": {}
        }

        for table_name, table_info in self.schema.tables.items():
            summary["tables"][table_name] = {
                "column_count": len(table_info.columns),
                "primary_keys": table_info.primary_keys,
                "foreign_key_count": len(table_info.foreign_keys),
                "index_count": len(table_info.indexes),
                "estimated_rows": table_info.estimated_row_count
            }
            summary["total_columns"] += len(table_info.columns)
            summary["total_indexes"] += len(table_info.indexes)
            summary["total_foreign_keys"] += len(table_info.foreign_keys)

        return summary

    def to_json(self, pretty: bool = True) -> str:
        """将模式转换为JSON"""
        if self.schema is None:
            self.load_schema()

        # 使用缓存
        if self._cached_schema_json is not None:
            return self._cached_schema_json

        # 转换为可序列化的字典
        schema_dict = {
            "version": self.schema.version,
            "last_updated": self.schema.last_updated,
            "tables": {}
        }

        for table_name, table_info in self.schema.tables.items():
            table_dict = {
                "name": table_info.name,
                "columns": {},
                "primary_keys": table_info.primary_keys,
                "foreign_keys": table_info.foreign_keys,
                "indexes": table_info.indexes,
                "estimated_row_count": table_info.estimated_row_count
            }

            for col_name, column in table_info.columns.items():
                table_dict["columns"][col_name] = {
                    "name": column.name,
                    "type": column.type,
                    "not_null": column.not_null,
                    "default_value": column.default_value,
                    "primary_key": column.primary_key,
                    "foreign_key": column.foreign_key,
                    "referenced_table": column.referenced_table,
                    "referenced_column": column.referenced_column
                }

            schema_dict["tables"][table_name] = table_dict

        if pretty:
            json_str = json.dumps(schema_dict, indent=2, ensure_ascii=False)
        else:
            json_str = json.dumps(schema_dict, ensure_ascii=False)

        self._cached_schema_json = json_str
        return json_str

    def save_to_file(self, file_path: str):
        """保存模式到文件"""
        json_str = self.to_json()
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(json_str)
        logger.info(f"数据库模式已保存到: {file_path}")

    def print_summary(self):
        """打印模式摘要"""
        summary = self.get_schema_summary()

        print("=" * 60)
        print("数据库模式摘要")
        print("=" * 60)
        print(f"表数量: {summary['table_count']}")
        print(f"总列数: {summary['total_columns']}")
        print(f"总索引数: {summary['total_indexes']}")
        print(f"总外键数: {summary['total_foreign_keys']}")
        print("-" * 60)

        for table_name, table_info in summary["tables"].items():
            print(f"{table_name}:")
            print(f"  列数: {table_info['column_count']}")
            print(f"  主键: {', '.join(table_info['primary_keys'])}")
            print(f"  外键数: {table_info['foreign_key_count']}")
            print(f"  索引数: {table_info['index_count']}")
            print(f"  估计行数: {table_info['estimated_rows']}")
            print()

        print("=" * 60)


# 全局模式管理器实例
_schema_manager: Optional[SchemaManager] = None


def get_schema_manager(db_connection=None) -> SchemaManager:
    """获取模式管理器实例"""
    global _schema_manager
    if _schema_manager is None:
        _schema_manager = SchemaManager(db_connection)
    return _schema_manager


def load_schema(force_reload: bool = False) -> DatabaseSchema:
    """加载数据库模式"""
    manager = get_schema_manager()
    return manager.load_schema(force_reload)


def get_schema_summary() -> Dict[str, Any]:
    """获取模式摘要"""
    manager = get_schema_manager()
    return manager.get_schema_summary()


def print_schema_summary():
    """打印模式摘要"""
    manager = get_schema_manager()
    manager.print_summary()


# 导出
__all__ = [
    "ColumnInfo",
    "TableInfo",
    "DatabaseSchema",
    "SchemaManager",
    "get_schema_manager",
    "load_schema",
    "get_schema_summary",
    "print_schema_summary",
]