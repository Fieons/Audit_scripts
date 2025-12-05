"""
主转换脚本：将CSV序时账转换为数据库
集成数据清洗、辅助项解析和数据库导入功能
"""

import os
import sys
import sqlite3
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import traceback

# 导入自定义模块
from db_schema import DatabaseSchema
from data_cleaner import DataCleaner
from auxiliary_parser import AuxiliaryParser


class CSVToDBConverter:
    """CSV到数据库转换器"""

    def __init__(self, db_path: str = "database/accounting.db"):
        """
        初始化转换器

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.db_schema = DatabaseSchema(db_path)
        self.data_cleaner = DataCleaner()
        self.auxiliary_parser = AuxiliaryParser()

        # 缓存字典，避免重复插入
        self.company_cache = {}  # 公司名称 -> ID
        self.book_cache = {}     # (公司ID, 账簿名称) -> ID
        self.subject_cache = {}  # 科目编码 -> ID
        self.project_cache = {}  # 项目名称 -> ID
        self.supplier_cache = {} # 客商名称 -> ID

    def process_csv_file(self, csv_path: str, year: int) -> Dict[str, Any]:
        """
        处理单个CSV文件

        Args:
            csv_path: CSV文件路径
            year: 数据年份

        Returns:
            处理结果统计
        """
        print(f"\n[文件] 处理文件: {csv_path}")
        print(f"[日期] 数据年份: {year}")

        try:
            # 1. 读取和清洗数据
            df_original = self.data_cleaner.read_csv(csv_path)
            df_cleaned = self.data_cleaner.clean_dataframe(df_original, year)

            # 2. 生成清洗报告
            cleaning_report = self.data_cleaner.get_cleaning_report(df_original, df_cleaned)

            # 3. 导入到数据库
            import_stats = self._import_to_database(df_cleaned)

            # 4. 合并统计信息
            result = {
                'file_path': csv_path,
                'year': year,
                'original_rows': cleaning_report['original_rows'],
                'cleaned_rows': cleaning_report['cleaned_rows'],
                'import_stats': import_stats,
                'cleaning_report': cleaning_report,
                'status': 'success'
            }

            print(f"[成功] 文件处理完成: {csv_path}")
            return result

        except Exception as e:
            print(f"[失败] 处理文件失败: {csv_path}")
            print(f"错误: {e}")
            traceback.print_exc()

            return {
                'file_path': csv_path,
                'year': year,
                'status': 'failed',
                'error': str(e)
            }

    def _import_to_database(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        将清洗后的数据导入数据库

        Args:
            df: 清洗后的DataFrame

        Returns:
            导入统计信息
        """
        conn = self.db_schema.connect()
        cursor = conn.cursor()

        stats = {
            'companies_inserted': 0,
            'books_inserted': 0,
            'subjects_inserted': 0,
            'vouchers_inserted': 0,
            'voucher_details_inserted': 0,
            'auxiliary_items_inserted': 0,
            'projects_inserted': 0,
            'suppliers_inserted': 0
        }

        try:
            # 按凭证分组处理
            voucher_groups = df.groupby(['公司名称', '账簿类型', '凭证号', '凭证日期'])

            for (company_name, book_type, voucher_number, voucher_date), group in voucher_groups:
                # 1. 获取或创建公司
                company_id = self._get_or_create_company(cursor, company_name)
                if company_id:
                    stats['companies_inserted'] += 1

                # 2. 获取或创建账簿
                book_id = self._get_or_create_book(cursor, company_id, f"{company_name}-{book_type}")
                if book_id:
                    stats['books_inserted'] += 1

                # 3. 创建凭证主记录
                voucher_id = self._create_voucher(
                    cursor, book_id, voucher_number, voucher_date, group
                )
                if voucher_id:
                    stats['vouchers_inserted'] += 1

                # 4. 处理凭证明细
                for _, row in group.iterrows():
                    # 获取或创建科目
                    subject_id = self._get_or_create_subject(cursor, row)
                    if subject_id:
                        stats['subjects_inserted'] += 1

                    # 创建凭证明细
                    detail_id = self._create_voucher_detail(
                        cursor, voucher_id, subject_id, row
                    )
                    if detail_id:
                        stats['voucher_details_inserted'] += 1

                        # 5. 处理辅助项
                        auxiliary_items = self._process_auxiliary_items(
                            cursor, detail_id, row
                        )
                        stats['auxiliary_items_inserted'] += auxiliary_items

                        # 6. 处理项目和客商（从辅助项中提取）
                        self._process_projects_and_suppliers(cursor, row, company_id)

            conn.commit()
            print(f"[成功] 数据导入完成，共导入 {len(voucher_groups)} 个凭证")

        except Exception as e:
            conn.rollback()
            print(f"[失败] 数据导入失败: {e}")
            raise

        finally:
            conn.close()

        return stats

    def _get_or_create_company(self, cursor, company_name: str) -> Optional[int]:
        """获取或创建公司记录"""
        if company_name in self.company_cache:
            return self.company_cache[company_name]

        # 检查是否已存在
        cursor.execute("SELECT id FROM companies WHERE name = ?", (company_name,))
        result = cursor.fetchone()

        if result:
            company_id = result[0]
        else:
            # 创建新公司
            company_code = self._generate_company_code(company_name)
            cursor.execute(
                "INSERT INTO companies (name, code) VALUES (?, ?)",
                (company_name, company_code)
            )
            company_id = cursor.lastrowid

        self.company_cache[company_name] = company_id
        return company_id

    def _get_or_create_book(self, cursor, company_id: int, book_name: str) -> Optional[int]:
        """获取或创建账簿记录"""
        cache_key = (company_id, book_name)
        if cache_key in self.book_cache:
            return self.book_cache[cache_key]

        # 检查是否已存在
        cursor.execute(
            "SELECT id FROM account_books WHERE company_id = ? AND name = ?",
            (company_id, book_name)
        )
        result = cursor.fetchone()

        if result:
            book_id = result[0]
        else:
            # 创建新账簿
            cursor.execute(
                "INSERT INTO account_books (company_id, name) VALUES (?, ?)",
                (company_id, book_name)
            )
            book_id = cursor.lastrowid

        self.book_cache[cache_key] = book_id
        return book_id

    def _get_or_create_subject(self, cursor, row) -> Optional[int]:
        """获取或创建科目记录"""
        subject_code = row.get('科目编码', '')
        if not subject_code:
            return None

        if subject_code in self.subject_cache:
            return self.subject_cache[subject_code]

        # 检查是否已存在
        cursor.execute("SELECT id FROM account_subjects WHERE code = ?", (subject_code,))
        result = cursor.fetchone()

        if result:
            subject_id = result[0]
        else:
            # 创建新科目
            cursor.execute(
                """INSERT INTO account_subjects
                   (code, name, full_name, level, subject_type, normal_balance)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    subject_code,
                    row.get('科目简称', ''),
                    row.get('科目全称', ''),
                    row.get('科目层级', 1),
                    row.get('科目类型', '未知'),
                    row.get('正常余额方向', '未知')
                )
            )
            subject_id = cursor.lastrowid

        self.subject_cache[subject_code] = subject_id
        return subject_id

    def _create_voucher(self, cursor, book_id: int, voucher_number: str,
                       voucher_date: pd.Timestamp, group: pd.DataFrame) -> Optional[int]:
        """创建凭证主记录"""
        # 提取凭证类型
        voucher_type = group.iloc[0].get('凭证类型', '未知')

        # 计算凭证合计金额
        total_debit = group['借方-本币'].sum()
        total_credit = group['贷方-本币'].sum()

        # 提取年月日
        if pd.isna(voucher_date):
            year = month = day = 0
        else:
            year = voucher_date.year
            month = voucher_date.month
            day = voucher_date.day

        cursor.execute(
            """INSERT INTO vouchers
               (book_id, voucher_number, voucher_type, voucher_date,
                year, month, day, total_debit, total_credit)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                book_id,
                voucher_number,
                voucher_type,
                voucher_date.strftime('%Y-%m-%d') if not pd.isna(voucher_date) else None,
                year,
                month,
                day,
                total_debit,
                total_credit
            )
        )

        return cursor.lastrowid

    def _create_voucher_detail(self, cursor, voucher_id: int, subject_id: int,
                              row: pd.Series) -> Optional[int]:
        """创建凭证明细记录"""
        cursor.execute(
            """INSERT INTO voucher_details
               (voucher_id, entry_number, summary, subject_id, currency,
                debit_amount, credit_amount, auxiliary_info,
                write_off_info, settlement_info)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                voucher_id,
                row.get('分录号', 1),
                row.get('摘要', ''),
                subject_id,
                row.get('币种', '人民币'),
                row.get('借方-本币', 0),
                row.get('贷方-本币', 0),
                row.get('辅助项', ''),
                row.get('核销信息', ''),
                row.get('结算信息', '')
            )
        )

        return cursor.lastrowid

    def _process_auxiliary_items(self, cursor, detail_id: int, row: pd.Series) -> int:
        """处理辅助项"""
        auxiliary_text = row.get('辅助项', '')
        if not auxiliary_text or pd.isna(auxiliary_text):
            return 0

        items = self.auxiliary_parser.parse_auxiliary_info(auxiliary_text)
        inserted_count = 0

        for item in items:
            cursor.execute(
                """INSERT INTO auxiliary_items
                   (detail_id, item_type, item_name, item_value)
                   VALUES (?, ?, ?, ?)""",
                (
                    detail_id,
                    item['item_type'],
                    item.get('display_type', ''),
                    item['item_value']
                )
            )
            inserted_count += 1

        return inserted_count

    def _process_projects_and_suppliers(self, cursor, row: pd.Series, company_id: int):
        """处理项目和客商（从辅助项中提取）"""
        auxiliary_text = row.get('辅助项', '')
        if not auxiliary_text or pd.isna(auxiliary_text):
            return

        items = self.auxiliary_parser.parse_auxiliary_info(auxiliary_text)

        for item in items:
            item_type = item['item_type']
            item_value = item['item_value']

            if item_type == 'project' and item_value:
                self._get_or_create_project(cursor, item_value, company_id)
            elif item_type in ['supplier_customer', 'supplier', 'customer'] and item_value:
                self._get_or_create_supplier(cursor, item_value, item_type)

    def _get_or_create_project(self, cursor, project_name: str, company_id: int) -> Optional[int]:
        """获取或创建项目记录"""
        if project_name in self.project_cache:
            return self.project_cache[project_name]

        # 检查是否已存在
        cursor.execute(
            "SELECT id FROM projects WHERE project_name = ? AND company_id = ?",
            (project_name, company_id)
        )
        result = cursor.fetchone()

        if result:
            project_id = result[0]
        else:
            # 生成项目编码：基于公司ID和项目数量
            cursor.execute(
                "SELECT COUNT(*) FROM projects WHERE company_id = ?",
                (company_id,)
            )
            project_count = cursor.fetchone()[0]
            project_code = f"PROJ{company_id:03d}-{project_count + 1:04d}"

            cursor.execute(
                """INSERT INTO projects (project_code, project_name, company_id)
                   VALUES (?, ?, ?)""",
                (project_code, project_name, company_id)
            )
            project_id = cursor.lastrowid
            self.project_cache[project_name] = project_id

        return project_id

    def _get_or_create_supplier(self, cursor, supplier_name: str, supplier_type: str) -> Optional[int]:
        """获取或创建客商记录"""
        if supplier_name in self.supplier_cache:
            return self.supplier_cache[supplier_name]

        # 检查是否已存在
        cursor.execute(
            "SELECT id FROM suppliers_customers WHERE name = ?",
            (supplier_name,)
        )
        result = cursor.fetchone()

        if result:
            supplier_id = result[0]
        else:
            # 标准化类型
            if supplier_type == 'supplier_customer':
                final_type = '供应商/客户'
            elif supplier_type == 'supplier':
                final_type = '供应商'
            elif supplier_type == 'customer':
                final_type = '客户'
            else:
                final_type = '未知'

            cursor.execute(
                """INSERT INTO suppliers_customers (name, type)
                   VALUES (?, ?)""",
                (supplier_name, final_type)
            )
            supplier_id = cursor.lastrowid
            self.supplier_cache[supplier_name] = supplier_id

        return supplier_id

    def _generate_company_code(self, company_name: str) -> str:
        """生成公司代码"""
        # 简单实现：取前两个汉字拼音首字母
        if len(company_name) >= 2:
            # 这里简化处理，实际应该使用拼音转换
            code = company_name[:2]
        else:
            code = company_name

        # 添加数字后缀避免重复
        existing_codes = [c for c in self.company_cache.values()]
        suffix = len(existing_codes) + 1

        return f"{code}{suffix:03d}"

    def process_all_files(self, data_dir: str = "./data"):
        """
        处理所有CSV文件

        Args:
            data_dir: 数据目录路径
        """
        print("[开始] 开始批量处理所有CSV文件")
        print("=" * 60)

        # 查找所有CSV文件
        csv_files = []
        for filename in os.listdir(data_dir):
            if filename.endswith('.csv'):
                file_path = os.path.join(data_dir, filename)
                csv_files.append(file_path)

        if not csv_files:
            print(f"[失败] 在目录 {data_dir} 中未找到CSV文件")
            return []

        print(f"找到 {len(csv_files)} 个CSV文件:")
        for file_path in csv_files:
            print(f"  - {os.path.basename(file_path)}")

        # 处理每个文件
        results = []
        for file_path in csv_files:
            # 从文件名中提取年份
            year = self._extract_year_from_filename(file_path)
            result = self.process_csv_file(file_path, year)
            results.append(result)

        # 生成汇总报告
        self._generate_summary_report(results)

        return results

    def _extract_year_from_filename(self, filename: str) -> int:
        """从文件名中提取年份"""
        import re
        # 查找4位数字年份
        match = re.search(r'(\d{4})年', filename)
        if match:
            return int(match.group(1))

        # 如果没有找到，尝试其他模式
        match = re.search(r'20(\d{2})', filename)
        if match:
            return int(f"20{match.group(1)}")

        # 默认返回当前年份
        return datetime.now().year

    def _generate_summary_report(self, results: List[Dict[str, Any]]):
        """生成汇总报告"""
        print("\n" + "=" * 60)
        print("[信息] 批量处理汇总报告")
        print("=" * 60)

        successful = [r for r in results if r.get('status') == 'success']
        failed = [r for r in results if r.get('status') == 'failed']

        print(f"[成功] 成功处理: {len(successful)} 个文件")
        print(f"[失败] 处理失败: {len(failed)} 个文件")

        if successful:
            total_original = sum(r.get('original_rows', 0) for r in successful)
            total_cleaned = sum(r.get('cleaned_rows', 0) for r in successful)

            print(f"\n[统计] 数据统计:")
            print(f"  原始数据总行数: {total_original:,}")
            print(f"  清洗后数据总行数: {total_cleaned:,}")

            # 汇总导入统计
            total_stats = {
                'companies_inserted': 0,
                'books_inserted': 0,
                'subjects_inserted': 0,
                'vouchers_inserted': 0,
                'voucher_details_inserted': 0,
                'auxiliary_items_inserted': 0,
                'projects_inserted': 0,
                'suppliers_inserted': 0
            }

            for result in successful:
                import_stats = result.get('import_stats', {})
                for key in total_stats:
                    total_stats[key] += import_stats.get(key, 0)

            print(f"\n[导入] 数据库导入统计:")
            for key, value in total_stats.items():
                if value > 0:
                    print(f"  {key}: {value:,}")

        if failed:
            print(f"\n[失败] 失败文件详情:")
            for result in failed:
                print(f"  文件: {os.path.basename(result.get('file_path', '未知'))}")
                print(f"  错误: {result.get('error', '未知错误')}")
                print()

        print("=" * 60)
        print("[完成] 批量处理完成!")

    def validate_database_integrity(self):
        """验证数据库完整性"""
        print("\n[验证] 验证数据库完整性")
        print("=" * 60)

        conn = self.db_schema.connect()
        cursor = conn.cursor()

        checks = [
            ("检查凭证借贷平衡", self._check_voucher_balance),
            ("检查科目引用完整性", self._check_subject_references),
            ("检查辅助项引用完整性", self._check_auxiliary_references),
            ("统计各表记录数", self._count_table_records)
        ]

        all_passed = True
        for check_name, check_func in checks:
            print(f"\n{check_name}:")
            try:
                passed = check_func(cursor)
                if passed:
                    print("  [成功] 通过")
                else:
                    print("  [失败] 失败")
                    all_passed = False
            except Exception as e:
                print(f"  [失败] 检查出错: {e}")
                all_passed = False

        conn.close()

        if all_passed:
            print("\n[成功] 所有完整性检查通过")
        else:
            print("\n[失败] 完整性检查失败")

        return all_passed

    def _check_voucher_balance(self, cursor) -> bool:
        """检查凭证借贷平衡"""
        cursor.execute("""
            SELECT v.id, v.voucher_number, v.voucher_date,
                   SUM(vd.debit_amount) as total_debit,
                   SUM(vd.credit_amount) as total_credit,
                   ABS(SUM(vd.debit_amount) - SUM(vd.credit_amount)) as difference
            FROM vouchers v
            JOIN voucher_details vd ON v.id = vd.voucher_id
            GROUP BY v.id
            HAVING ABS(SUM(vd.debit_amount) - SUM(vd.credit_amount)) > 0.01
        """)

        unbalanced = cursor.fetchall()
        if unbalanced:
            print(f"  发现 {len(unbalanced)} 个不平衡的凭证:")
            for row in unbalanced[:5]:  # 只显示前5个
                print(f"    凭证 {row['voucher_number']} ({row['voucher_date']}): "
                      f"借方={row['total_debit']:.2f}, "
                      f"贷方={row['total_credit']:.2f}, "
                      f"差额={row['difference']:.2f}")
            if len(unbalanced) > 5:
                print(f"    ... 还有 {len(unbalanced) - 5} 个")
            return False
        return True

    def _check_subject_references(self, cursor) -> bool:
        """检查科目引用完整性"""
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM voucher_details vd
            LEFT JOIN account_subjects s ON vd.subject_id = s.id
            WHERE s.id IS NULL
        """)

        missing = cursor.fetchone()['count']
        if missing > 0:
            print(f"  发现 {missing} 个无效的科目引用")
            return False
        return True

    def _check_auxiliary_references(self, cursor) -> bool:
        """检查辅助项引用完整性"""
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM auxiliary_items ai
            LEFT JOIN voucher_details vd ON ai.detail_id = vd.id
            WHERE vd.id IS NULL
        """)

        missing = cursor.fetchone()['count']
        if missing > 0:
            print(f"  发现 {missing} 个无效的辅助项引用")
            return False
        return True

    def _count_table_records(self, cursor) -> bool:
        """统计各表记录数"""
        tables = [
            'companies', 'account_books', 'account_subjects',
            'vouchers', 'voucher_details', 'auxiliary_items',
            'projects', 'suppliers_customers'
        ]

        print("  各表记录数:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()['count']
            print(f"    {table}: {count:,}")

        return True


def main():
    """主函数：执行CSV到数据库的转换"""
    import argparse

    parser = argparse.ArgumentParser(description='将CSV序时账转换为数据库')
    parser.add_argument('--data-dir', default='./data',
                       help='CSV数据目录路径 (默认: ./data)')
    parser.add_argument('--db-path', default='database/accounting.db',
                       help='数据库文件路径 (默认: database/accounting.db)')
    parser.add_argument('--reset-db', action='store_true',
                       help='重置数据库（删除所有表）')
    parser.add_argument('--validate-only', action='store_true',
                       help='只验证数据库完整性，不导入数据')

    args = parser.parse_args()

    # 确保数据库目录存在
    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)

    # 创建转换器
    converter = CSVToDBConverter(args.db_path)

    # 如果需要重置数据库
    if args.reset_db:
        print("[警告]  重置数据库...")
        converter.db_schema.drop_all_tables()

    # 创建数据库schema
    print("[创建] 创建数据库schema...")
    converter.db_schema.create_tables()
    converter.db_schema.create_indexes()

    if args.validate_only:
        # 只验证数据库
        converter.validate_database_integrity()
        return

    # 处理所有CSV文件
    results = converter.process_all_files(args.data_dir)

    # 验证数据库完整性
    converter.validate_database_integrity()

    # 显示数据库schema信息
    print("\n[结构] 数据库schema信息:")
    converter.db_schema.get_table_info()


if __name__ == "__main__":
    main()