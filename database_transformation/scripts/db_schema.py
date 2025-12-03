#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库schema定义模块
定义会计凭证数据库的表结构和索引
"""

import sqlite3
from typing import List, Tuple


class DatabaseSchema:
    """数据库schema管理类"""

    def __init__(self, db_path: str = "../database/accounting.db"):
        """
        初始化数据库schema

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.connection = None

    def connect(self) -> sqlite3.Connection:
        """连接到数据库"""
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        return self.connection

    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def create_tables(self):
        """创建所有表"""
        conn = self.connect()
        cursor = conn.cursor()

        try:
            # 1. 公司表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name VARCHAR(100) NOT NULL,
                    code VARCHAR(50) UNIQUE
                )
            """)

            # 2. 账簿表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS account_books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_id INTEGER NOT NULL,
                    name VARCHAR(200) NOT NULL,
                    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
                )
            """)

            # 3. 会计科目表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS account_subjects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code VARCHAR(20) NOT NULL UNIQUE,
                    name VARCHAR(200) NOT NULL,
                    full_name VARCHAR(500),
                    level INTEGER,
                    subject_type VARCHAR(20),
                    normal_balance VARCHAR(10)
                )
            """)

            # 4. 凭证主表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vouchers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    book_id INTEGER NOT NULL,
                    voucher_number VARCHAR(50) NOT NULL,
                    voucher_type VARCHAR(10),
                    voucher_date DATE NOT NULL,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    day INTEGER NOT NULL,
                    total_debit DECIMAL(15,2) DEFAULT 0,
                    total_credit DECIMAL(15,2) DEFAULT 0,
                    FOREIGN KEY (book_id) REFERENCES account_books(id) ON DELETE CASCADE
                )
            """)

            # 5. 凭证明细表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS voucher_details (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    voucher_id INTEGER NOT NULL,
                    entry_number INTEGER NOT NULL,
                    summary TEXT,
                    subject_id INTEGER NOT NULL,
                    currency VARCHAR(20),
                    debit_amount DECIMAL(15,2) DEFAULT 0,
                    credit_amount DECIMAL(15,2) DEFAULT 0,
                    auxiliary_info TEXT,
                    write_off_info TEXT,
                    settlement_info TEXT,
                    FOREIGN KEY (voucher_id) REFERENCES vouchers(id) ON DELETE CASCADE,
                    FOREIGN KEY (subject_id) REFERENCES account_subjects(id)
                )
            """)

            # 6. 辅助项解析表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS auxiliary_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    detail_id INTEGER NOT NULL,
                    item_type VARCHAR(50) NOT NULL,
                    item_name VARCHAR(100),
                    item_value VARCHAR(500) NOT NULL,
                    FOREIGN KEY (detail_id) REFERENCES voucher_details(id) ON DELETE CASCADE
                )
            """)

            # 7. 项目表（可选）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_code VARCHAR(50) UNIQUE,
                    project_name VARCHAR(200) NOT NULL,
                    company_id INTEGER,
                    FOREIGN KEY (company_id) REFERENCES companies(id)
                )
            """)

            # 8. 客商表（可选）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS suppliers_customers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name VARCHAR(200) NOT NULL,
                    type VARCHAR(20)
                )
            """)

            conn.commit()
            print("[OK] 所有表创建成功")

        except sqlite3.Error as e:
            conn.rollback()
            print(f"[ERROR] 创建表时出错: {e}")
            raise
        finally:
            self.close()

    def create_indexes(self):
        """创建索引"""
        conn = self.connect()
        cursor = conn.cursor()

        try:
            # 账簿表索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_books_company ON account_books(company_id)")

            # 凭证表索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_vouchers_book_date ON vouchers(book_id, voucher_date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_vouchers_number ON vouchers(voucher_number)")

            # 凭证明细表索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_voucher_details_voucher ON voucher_details(voucher_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_voucher_details_subject ON voucher_details(subject_id)")

            # 辅助项表索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_auxiliary_items_detail ON auxiliary_items(detail_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_auxiliary_items_type_value ON auxiliary_items(item_type, item_value)")

            conn.commit()
            print("[OK] 所有索引创建成功")

        except sqlite3.Error as e:
            conn.rollback()
            print(f"[ERROR] 创建索引时出错: {e}")
            raise
        finally:
            self.close()

    def drop_all_tables(self):
        """删除所有表（用于测试）"""
        conn = self.connect()
        cursor = conn.cursor()

        try:
            # 注意：需要按依赖关系的逆序删除
            tables = [
                'auxiliary_items',
                'voucher_details',
                'vouchers',
                'projects',
                'suppliers_customers',
                'account_subjects',
                'account_books',
                'companies'
            ]

            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")

            conn.commit()
            print("[OK] 所有表已删除")

        except sqlite3.Error as e:
            conn.rollback()
            print(f"[ERROR] 删除表时出错: {e}")
            raise
        finally:
            self.close()

    def get_table_info(self) -> List[Tuple[str, int]]:
        """获取所有表的信息"""
        conn = self.connect()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT name, COUNT(*) as row_count
                FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                GROUP BY name
                ORDER BY name
            """)

            tables = cursor.fetchall()
            return [(table['name'], table['row_count']) for table in tables]

        finally:
            self.close()

    def validate_schema(self) -> bool:
        """验证schema是否完整"""
        required_tables = [
            'companies',
            'account_books',
            'account_subjects',
            'vouchers',
            'voucher_details',
            'auxiliary_items'
        ]

        conn = self.connect()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """)

            existing_tables = [row['name'] for row in cursor.fetchall()]

            missing_tables = []
            for table in required_tables:
                if table not in existing_tables:
                    missing_tables.append(table)

            if missing_tables:
                print(f"[ERROR] 缺少以下表: {', '.join(missing_tables)}")
                return False
            else:
                print("[OK] Schema验证通过")
                return True

        finally:
            self.close()


def main():
    """主函数：创建数据库schema"""
    import os

    # 确保database目录存在
    os.makedirs("../database", exist_ok=True)

    schema = DatabaseSchema()

    print("=" * 50)
    print("开始创建会计凭证数据库schema")
    print("=" * 50)

    try:
        # 创建表
        schema.create_tables()

        # 创建索引
        schema.create_indexes()

        # 验证schema
        if schema.validate_schema():
            print("[OK] 数据库schema创建完成")

            # 显示表信息
            print("\n数据库表信息:")
            print("-" * 30)
            tables = schema.get_table_info()
            for table_name, row_count in tables:
                print(f"{table_name:20s} | 行数: {row_count}")

        else:
            print("[ERROR] Schema验证失败")

    except Exception as e:
        print(f"[ERROR] 创建schema时发生错误: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())