"""
数据库schema定义模块
根据方案文档设计的关系型数据库结构
"""

import sqlite3
from typing import Optional


class DatabaseSchema:
    """数据库schema管理类"""

    def __init__(self, db_path: str = "../database/accounting.db"):
        """
        初始化数据库连接

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """连接到数据库"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        return self.conn

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def create_tables(self):
        """
        创建所有表结构
        根据方案文档附录9.1的DDL脚本
        """
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
            print("[成功] 所有表创建成功")

        except sqlite3.Error as e:
            conn.rollback()
            print(f"[失败] 创建表时出错: {e}")
            raise

    def create_indexes(self):
        """
        创建索引以提高查询性能
        根据方案文档7.3节的索引设计
        """
        conn = self.connect()
        cursor = conn.cursor()

        try:
            # 账簿表索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_books_company ON account_books(company_id)")

            # 凭证主表索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_vouchers_book_date ON vouchers(book_id, voucher_date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_vouchers_number ON vouchers(voucher_number)")

            # 凭证明细表索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_voucher_details_voucher ON voucher_details(voucher_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_voucher_details_subject ON voucher_details(subject_id)")

            # 辅助项表索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_auxiliary_items_detail ON auxiliary_items(detail_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_auxiliary_items_type_value ON auxiliary_items(item_type, item_value)")

            conn.commit()
            print("[成功] 所有索引创建成功")

        except sqlite3.Error as e:
            conn.rollback()
            print(f"[失败] 创建索引时出错: {e}")
            raise

    def drop_all_tables(self):
        """
        删除所有表（用于测试和重置）
        注意：此操作会删除所有数据！
        """
        conn = self.connect()
        cursor = conn.cursor()

        try:
            # 注意删除顺序，因为有外键约束
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
            print("[成功] 所有表已删除")

        except sqlite3.Error as e:
            conn.rollback()
            print(f"[失败] 删除表时出错: {e}")
            raise

    def get_table_info(self):
        """
        获取数据库表结构信息
        """
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()

        print("[信息] 数据库表结构信息:")
        print("-" * 50)

        for table in tables:
            table_name = table[0]
            print(f"\n表名: {table_name}")
            print("字段列表:")

            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()

            for col in columns:
                col_id, col_name, col_type, not_null, default_val, pk = col
                pk_str = "PRIMARY KEY" if pk else ""
                not_null_str = "NOT NULL" if not_null else ""
                print(f"  {col_name:20} {col_type:15} {pk_str:12} {not_null_str}")

        print("-" * 50)

    def validate_schema(self):
        """
        验证数据库schema是否符合预期
        """
        conn = self.connect()
        cursor = conn.cursor()

        expected_tables = {
            'companies': ['id', 'name', 'code'],
            'account_books': ['id', 'company_id', 'name'],
            'account_subjects': ['id', 'code', 'name', 'full_name', 'level', 'subject_type', 'normal_balance'],
            'vouchers': ['id', 'book_id', 'voucher_number', 'voucher_type', 'voucher_date', 'year', 'month', 'day', 'total_debit', 'total_credit'],
            'voucher_details': ['id', 'voucher_id', 'entry_number', 'summary', 'subject_id', 'currency', 'debit_amount', 'credit_amount', 'auxiliary_info', 'write_off_info', 'settlement_info'],
            'auxiliary_items': ['id', 'detail_id', 'item_type', 'item_name', 'item_value'],
            'projects': ['id', 'project_code', 'project_name', 'company_id'],
            'suppliers_customers': ['id', 'name', 'type']
        }

        all_valid = True

        for table_name, expected_columns in expected_tables.items():
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if not cursor.fetchone():
                print(f"[失败] 表 '{table_name}' 不存在")
                all_valid = False
                continue

            cursor.execute(f"PRAGMA table_info({table_name})")
            actual_columns = [col[1] for col in cursor.fetchall()]

            missing_columns = set(expected_columns) - set(actual_columns)
            extra_columns = set(actual_columns) - set(expected_columns)

            if missing_columns:
                print(f"[失败] 表 '{table_name}' 缺少字段: {missing_columns}")
                all_valid = False

            if extra_columns:
                print(f"[警告]  表 '{table_name}' 有多余字段: {extra_columns}")

        if all_valid:
            print("[成功] 数据库schema验证通过")
        else:
            print("[失败] 数据库schema验证失败")

        return all_valid


def main():
    """主函数：创建数据库schema"""
    import os

    # 确保database目录存在
    os.makedirs("../database", exist_ok=True)

    db = DatabaseSchema("../database/accounting.db")

    try:
        # 创建表
        db.create_tables()

        # 创建索引
        db.create_indexes()

        # 显示表结构
        db.get_table_info()

        # 验证schema
        db.validate_schema()

    finally:
        db.close()


if __name__ == "__main__":
    main()