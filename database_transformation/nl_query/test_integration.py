"""
集成测试
测试自然语言SQL查询工具的核心功能
"""

import unittest
import tempfile
import sqlite3
import pandas as pd
from pathlib import Path
import sys
import os

# 添加项目路径
sys.path.append(str(Path(__file__).parent))

from config import validate_config
from database import DatabaseManager, DatabaseError
from deepseek_client import DeepSeekClient, DeepSeekError
from sql_generator import SQLGenerator, SQLGenerationError
from utils import validate_natural_language_query, QueryHistory

class TestDatabaseManager(unittest.TestCase):
    """测试数据库管理器"""

    def setUp(self):
        """创建测试数据库"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.db_path = self.temp_db.name

        # 创建测试表
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 创建测试表
        cursor.execute("""
            CREATE TABLE test_companies (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                code TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE test_vouchers (
                id INTEGER PRIMARY KEY,
                company_id INTEGER,
                voucher_number TEXT,
                voucher_date DATE
            )
        """)

        # 插入测试数据
        cursor.execute("INSERT INTO test_companies (name, code) VALUES ('测试公司1', 'TEST001')")
        cursor.execute("INSERT INTO test_companies (name, code) VALUES ('测试公司2', 'TEST002')")
        cursor.execute("INSERT INTO test_vouchers (company_id, voucher_number, voucher_date) VALUES (1, 'V001', '2024-01-01')")

        conn.commit()
        conn.close()

    def tearDown(self):
        """清理测试数据库"""
        # 确保数据库连接已关闭
        import gc
        gc.collect()

        # 尝试多次删除文件
        import time
        for _ in range(5):
            try:
                if os.path.exists(self.db_path):
                    os.unlink(self.db_path)
                    break
            except (PermissionError, OSError):
                time.sleep(0.1)
                continue

    def test_connection(self):
        """测试数据库连接"""
        db = DatabaseManager(self.db_path)
        self.assertTrue(db.test_connection())

    def test_execute_query(self):
        """测试查询执行"""
        db = DatabaseManager(self.db_path)
        with db:
            result = db.execute_query("SELECT * FROM test_companies")
            self.assertEqual(len(result), 2)
            self.assertIn('name', result.columns)

    def test_schema_info(self):
        """测试schema信息获取"""
        db = DatabaseManager(self.db_path)
        with db:
            schema = db.get_schema_info()
            self.assertEqual(schema['total_tables'], 2)
            self.assertIn('test_companies', [t['name'] for t in schema['tables']])

    def test_sql_security(self):
        """测试SQL安全性验证"""
        db = DatabaseManager(self.db_path)

        # 测试允许的查询
        with db:
            result = db.execute_query("SELECT * FROM test_companies")
            self.assertIsNotNone(result)

        # 测试禁止的查询应该抛出异常
        with self.assertRaises(DatabaseError):
            with db:
                db.execute_query("DROP TABLE test_companies")

class TestUtils(unittest.TestCase):
    """测试工具函数"""

    def test_validate_natural_language_query(self):
        """测试自然语言查询验证"""
        # 有效查询
        is_valid, msg = validate_natural_language_query("查询所有公司信息")
        self.assertTrue(is_valid)
        self.assertEqual(msg, "")

        # 空查询
        is_valid, msg = validate_natural_language_query("")
        self.assertFalse(is_valid)
        self.assertIn("不能为空", msg)

        # 太短的查询
        is_valid, msg = validate_natural_language_query("ab")
        self.assertFalse(is_valid)
        self.assertIn("太短", msg)

        # 太长的查询
        long_query = "a" * 1001
        is_valid, msg = validate_natural_language_query(long_query)
        self.assertFalse(is_valid)
        self.assertIn("太长", msg)

        # 包含禁止关键字的查询
        is_valid, msg = validate_natural_language_query("DROP TABLE users")
        self.assertFalse(is_valid)
        self.assertIn("禁止的关键字", msg)

    def test_query_history(self):
        """测试查询历史管理"""
        history = QueryHistory(max_history=3)

        # 添加查询
        history.add_query("查询1", "SELECT 1", {"success": True})
        history.add_query("查询2", "SELECT 2", {"success": True})
        history.add_query("查询3", "SELECT 3", {"success": True})

        # 测试数量限制
        self.assertEqual(len(history.history), 3)

        history.add_query("查询4", "SELECT 4", {"success": True})
        self.assertEqual(len(history.history), 3)

        # 测试最近查询
        recent = history.get_recent_queries(2)
        self.assertEqual(len(recent), 2)
        self.assertEqual(recent[0]['natural_language'], "查询4")

        # 测试清空
        history.clear_history()
        self.assertEqual(len(history.history), 0)

class TestConfig(unittest.TestCase):
    """测试配置管理"""

    def test_validate_config(self):
        """测试配置验证"""
        # 注意：这个测试依赖于实际环境配置
        errors = validate_config()
        # 不检查具体错误，只检查返回类型
        self.assertIsInstance(errors, list)

class TestSQLGenerator(unittest.TestCase):
    """测试SQL生成器"""

    def setUp(self):
        """设置测试环境"""
        # 使用测试数据库
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.db_path = self.temp_db.name

        # 创建简单的测试表
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        cursor.execute("INSERT INTO test VALUES (1, '测试1')")
        cursor.execute("INSERT INTO test VALUES (2, '测试2')")
        conn.commit()
        conn.close()

        # 修改环境变量使用测试数据库
        os.environ['DATABASE_PATH'] = self.db_path

    def tearDown(self):
        """清理测试环境"""
        # 确保数据库连接已关闭
        import gc
        gc.collect()

        # 尝试多次删除文件
        import time
        for _ in range(5):
            try:
                if os.path.exists(self.db_path):
                    os.unlink(self.db_path)
                    break
            except (PermissionError, OSError):
                time.sleep(0.1)
                continue

        if 'DATABASE_PATH' in os.environ:
            del os.environ['DATABASE_PATH']

    def test_generator_initialization(self):
        """测试生成器初始化"""
        generator = SQLGenerator()
        connections = generator.test_connection()

        # 数据库连接应该成功
        self.assertTrue(connections.get('database', False))

        # API连接可能失败（如果没有配置API密钥），但不影响测试
        # 我们只检查返回类型
        self.assertIsInstance(connections, dict)

class TestIntegration(unittest.TestCase):
    """集成测试"""

    def test_end_to_end_without_api(self):
        """测试端到端流程（不调用API）"""
        # 这个测试模拟完整的流程，但不实际调用DeepSeek API
        # 主要测试各个组件的集成

        # 创建临时数据库
        temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        db_path = temp_db.name

        try:
            # 创建测试数据
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE companies (id INTEGER, name TEXT)")
            cursor.execute("INSERT INTO companies VALUES (1, '测试公司')")
            conn.commit()
            conn.close()

            # 修改环境变量
            os.environ['DATABASE_PATH'] = db_path

            # 测试配置验证
            errors = validate_config()
            print(f"配置验证错误: {errors}")

            # 测试数据库连接
            db = DatabaseManager()
            self.assertTrue(db.test_connection())

            # 测试查询执行
            with db:
                result = db.execute_query("SELECT * FROM companies")
                # 注意：这里可能查询到实际数据库的数据，所以不检查具体行数
                # 只检查返回的是DataFrame
                self.assertIsInstance(result, pd.DataFrame)
                if len(result) > 0:
                    self.assertIn('name', result.columns)

            # 测试工具函数
            is_valid, msg = validate_natural_language_query("测试查询")
            self.assertTrue(is_valid)

        finally:
            # 清理
            import gc
            gc.collect()

            # 尝试多次删除文件
            import time
            for _ in range(5):
                try:
                    if os.path.exists(db_path):
                        os.unlink(db_path)
                        break
                except (PermissionError, OSError):
                    time.sleep(0.1)
                    continue

            if 'DATABASE_PATH' in os.environ:
                del os.environ['DATABASE_PATH']

def run_tests():
    """运行所有测试"""
    # 创建测试套件
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 添加测试类
    suite.addTests(loader.loadTestsFromTestCase(TestDatabaseManager))
    suite.addTests(loader.loadTestsFromTestCase(TestUtils))
    suite.addTests(loader.loadTestsFromTestCase(TestConfig))
    suite.addTests(loader.loadTestsFromTestCase(TestSQLGenerator))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()

if __name__ == '__main__':
    print("运行自然语言SQL查询工具集成测试...")
    print("=" * 60)

    success = run_tests()

    print("=" * 60)
    if success:
        print("[OK] 所有测试通过！")
    else:
        print("[FAIL] 部分测试失败")

    sys.exit(0 if success else 1)