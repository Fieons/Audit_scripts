#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV到数据库转换主脚本
将CSV格式的序时账数据转换为关系型数据库
"""

import os
import sys
import sqlite3
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import re

# 添加当前目录到路径，以便导入模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_schema import DatabaseSchema
from data_cleaner import DataCleaner


class CSVToDBConverter:
    """CSV到数据库转换器"""

    def __init__(self, db_path: str = "../database/accounting.db"):
        """
        初始化转换器

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.schema = DatabaseSchema(db_path)
        self.cleaner = DataCleaner()
        self.connection = None

        # 缓存字典，避免重复查询
        self.company_cache = {}  # 公司名称 -> id
        self.book_cache = {}     # (公司id, 账簿名称) -> id
        self.subject_cache = {}  # 科目编码 -> id

    def connect(self) -> sqlite3.Connection:
        """连接到数据库"""
        if self.connection is None:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row
        return self.connection

    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def get_or_create_company(self, company_name: str) -> int:
        """
        获取或创建公司记录

        Args:
            company_name: 公司名称

        Returns:
            int: 公司ID
        """
        if company_name in self.company_cache:
            return self.company_cache[company_name]

        conn = self.connect()
        cursor = conn.cursor()

        # 查找现有公司
        cursor.execute("SELECT id FROM companies WHERE name = ?", (company_name,))
        row = cursor.fetchone()

        if row:
            company_id = row['id']
        else:
            # 创建新公司
            # 生成公司代码（使用名称拼音首字母或编号）
            company_code = self.generate_company_code(company_name)
            cursor.execute(
                "INSERT INTO companies (name, code) VALUES (?, ?)",
                (company_name, company_code)
            )
            company_id = cursor.lastrowid
            conn.commit()

        self.company_cache[company_name] = company_id
        return company_id

    def generate_company_code(self, company_name: str) -> str:
        """
        生成公司代码

        Args:
            company_name: 公司名称

        Returns:
            str: 公司代码
        """
        # 简单实现：使用名称的前几个字符
        # 实际应用中可能需要更复杂的逻辑
        if '和立' in company_name:
            return 'HL'
        elif '盛翔' in company_name:
            return 'SX'
        else:
            # 提取中文字符的首字母（简化版）
            import re
            chinese_chars = re.findall(r'[\u4e00-\u9fff]', company_name)
            if chinese_chars:
                return ''.join(chinese_chars[:2])
            else:
                return company_name[:2].upper()

    def get_or_create_account_book(self, company_id: int, book_name: str) -> int:
        """
        获取或创建账簿记录

        Args:
            company_id: 公司ID
            book_name: 账簿名称

        Returns:
            int: 账簿ID
        """
        cache_key = (company_id, book_name)
        if cache_key in self.book_cache:
            return self.book_cache[cache_key]

        conn = self.connect()
        cursor = conn.cursor()

        # 查找现有账簿
        cursor.execute(
            "SELECT id FROM account_books WHERE company_id = ? AND name = ?",
            (company_id, book_name)
        )
        row = cursor.fetchone()

        if row:
            book_id = row['id']
        else:
            # 创建新账簿
            cursor.execute(
                "INSERT INTO account_books (company_id, name) VALUES (?, ?)",
                (company_id, book_name)
            )
            book_id = cursor.lastrowid
            conn.commit()

        self.book_cache[cache_key] = book_id
        return book_id

    def get_or_create_account_subject(self, subject_info: Dict) -> int:
        """
        获取或创建会计科目记录

        Args:
            subject_info: 科目信息字典

        Returns:
            int: 科目ID
        """
        subject_code = subject_info['subject_code']
        if not subject_code:
            raise ValueError("科目编码不能为空")

        if subject_code in self.subject_cache:
            return self.subject_cache[subject_code]

        conn = self.connect()
        cursor = conn.cursor()

        # 查找现有科目
        cursor.execute("SELECT id FROM account_subjects WHERE code = ?", (subject_code,))
        row = cursor.fetchone()

        if row:
            subject_id = row['id']
        else:
            # 创建新科目
            cursor.execute("""
                INSERT INTO account_subjects
                (code, name, full_name, level, subject_type, normal_balance)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                subject_code,
                subject_info['subject_name'],
                subject_info['full_name'],
                subject_info['level'],
                subject_info['subject_type'],
                subject_info['normal_balance']
            ))
            subject_id = cursor.lastrowid
            conn.commit()

        self.subject_cache[subject_code] = subject_id
        return subject_id

    def process_voucher(self, book_id: int, voucher_data: pd.DataFrame) -> int:
        """
        处理一个凭证（包含多条分录）

        Args:
            book_id: 账簿ID
            voucher_data: 凭证数据DataFrame

        Returns:
            int: 凭证ID
        """
        if voucher_data.empty:
            raise ValueError("凭证数据为空")

        # 获取凭证基本信息（从第一行）
        first_row = voucher_data.iloc[0]
        voucher_number = str(first_row['凭证号'])
        voucher_type = first_row['凭证类型']
        voucher_date = first_row['凭证日期']
        year = first_row['年份']
        month = first_row['月']
        day = first_row['日']

        # 计算凭证合计金额
        total_debit = voucher_data['借方-本币'].sum()
        total_credit = voucher_data['贷方-本币'].sum()

        conn = self.connect()
        cursor = conn.cursor()

        # 检查是否已存在相同凭证
        cursor.execute("""
            SELECT id FROM vouchers
            WHERE book_id = ? AND voucher_number = ? AND voucher_date = ?
        """, (book_id, voucher_number, voucher_date))

        existing = cursor.fetchone()
        if existing:
            print(f"  警告：凭证已存在，跳过: {voucher_number} ({voucher_date})")
            return existing['id']

        # 插入凭证主记录
        cursor.execute("""
            INSERT INTO vouchers
            (book_id, voucher_number, voucher_type, voucher_date, year, month, day, total_debit, total_credit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            book_id, voucher_number, voucher_type, voucher_date,
            year, month, day, total_debit, total_credit
        ))

        voucher_id = cursor.lastrowid
        conn.commit()

        # 处理凭证明细
        for _, row in voucher_data.iterrows():
            self.process_voucher_detail(voucher_id, row)

        return voucher_id

    def process_voucher_detail(self, voucher_id: int, row: pd.Series):
        """
        处理凭证明细

        Args:
            voucher_id: 凭证ID
            row: 明细数据行
        """
        conn = self.connect()
        cursor = conn.cursor()

        # 获取科目ID
        subject_info = {
            'subject_code': row['科目编码_清洗'],
            'subject_name': row['科目名称_清洗'],
            'full_name': row['科目全名'],
            'level': row['科目层级'],
            'subject_type': row['科目类型'],
            'normal_balance': row['正常余额方向']
        }

        try:
            subject_id = self.get_or_create_account_subject(subject_info)
        except ValueError as e:
            print(f"  警告：无法处理科目，跳过明细: {e}")
            return

        # 插入明细记录
        cursor.execute("""
            INSERT INTO voucher_details
            (voucher_id, entry_number, summary, subject_id, currency,
             debit_amount, credit_amount, auxiliary_info, write_off_info, settlement_info)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            voucher_id,
            row['分录号'],
            row['摘要'],
            subject_id,
            row['币种'],
            row['借方-本币'],
            row['贷方-本币'],
            row['辅助项'],
            row['核销信息'],
            row['结算信息']
        ))

        detail_id = cursor.lastrowid
        conn.commit()

        # 处理辅助项
        if pd.notna(row['辅助项']):
            self.process_auxiliary_items(detail_id, str(row['辅助项']))

    def process_auxiliary_items(self, detail_id: int, auxiliary_text: str):
        """
        处理辅助项

        Args:
            detail_id: 明细ID
            auxiliary_text: 辅助项文本
        """
        items = self.cleaner.parse_auxiliary_info(auxiliary_text)
        if not items:
            return

        conn = self.connect()
        cursor = conn.cursor()

        for item in items:
            cursor.execute("""
                INSERT INTO auxiliary_items (detail_id, item_type, item_value)
                VALUES (?, ?, ?)
            """, (detail_id, item['item_type'], item['item_value']))

        conn.commit()

    def process_csv_file(self, file_path: str, year: Optional[int] = None):
        """
        处理单个CSV文件

        Args:
            file_path: CSV文件路径
            year: 数据年份，如果为None则从文件名中提取
        """
        print(f"\n处理文件: {os.path.basename(file_path)}")
        print("-" * 50)

        # 加载和清洗数据
        df = self.cleaner.load_csv_file(file_path, year)

        if df.empty:
            print("  警告：文件为空或加载失败")
            return

        # 按凭证分组处理
        voucher_groups = df.groupby(['凭证号', '凭证日期'])

        total_vouchers = len(voucher_groups)
        processed_vouchers = 0
        processed_details = 0

        for (voucher_number, voucher_date), group in voucher_groups:
            # 获取公司信息（假设一个凭证的所有分录属于同一公司）
            first_row = group.iloc[0]
            company_name = first_row['公司名称']
            book_name = first_row['账簿类型']

            try:
                # 获取或创建公司
                company_id = self.get_or_create_company(company_name)

                # 获取或创建账簿
                book_id = self.get_or_create_account_book(company_id, book_name)

                # 处理凭证
                voucher_id = self.process_voucher(book_id, group)

                processed_vouchers += 1
                processed_details += len(group)

                if processed_vouchers % 100 == 0:
                    print(f"  已处理 {processed_vouchers}/{total_vouchers} 个凭证...")

            except Exception as e:
                print(f"  错误：处理凭证 {voucher_number} 时出错: {e}")
                continue

        print(f"\n[OK] 文件处理完成")
        print(f"  凭证数: {processed_vouchers}/{total_vouchers}")
        print(f"  分录数: {processed_details}")

    def process_all_files(self, data_dir: str = "../data"):
        """
        处理所有CSV文件

        Args:
            data_dir: 数据目录路径
        """
        print("=" * 60)
        print("开始批量处理CSV文件")
        print("=" * 60)

        # 查找所有CSV文件
        csv_files = []
        for root, dirs, files in os.walk(data_dir):
            for file in files:
                if file.lower().endswith('.csv'):
                    csv_files.append(os.path.join(root, file))

        if not csv_files:
            print(f"在目录 {data_dir} 中未找到CSV文件")
            return

        print(f"找到 {len(csv_files)} 个CSV文件")

        # 初始化数据库
        print("\n初始化数据库...")
        self.schema.create_tables()
        self.schema.create_indexes()

        # 处理每个文件
        for i, csv_file in enumerate(csv_files, 1):
            print(f"\n[{i}/{len(csv_files)}] ", end="")
            try:
                self.process_csv_file(csv_file)
            except Exception as e:
                print(f"  错误：处理文件 {csv_file} 时出错: {e}")

        print("\n" + "=" * 60)
        print("批量处理完成")
        print("=" * 60)

        # 显示统计信息
        self.show_statistics()

    def show_statistics(self):
        """显示数据统计信息"""
        conn = self.connect()
        cursor = conn.cursor()

        try:
            print("\n数据统计:")
            print("-" * 40)

            # 公司统计
            cursor.execute("SELECT COUNT(*) as count FROM companies")
            company_count = cursor.fetchone()['count']
            print(f"公司数: {company_count}")

            # 账簿统计
            cursor.execute("SELECT COUNT(*) as count FROM account_books")
            book_count = cursor.fetchone()['count']
            print(f"账簿数: {book_count}")

            # 科目统计
            cursor.execute("SELECT COUNT(*) as count FROM account_subjects")
            subject_count = cursor.fetchone()['count']
            print(f"科目数: {subject_count}")

            # 凭证统计
            cursor.execute("SELECT COUNT(*) as count FROM vouchers")
            voucher_count = cursor.fetchone()['count']
            print(f"凭证数: {voucher_count}")

            # 明细统计
            cursor.execute("SELECT COUNT(*) as count FROM voucher_details")
            detail_count = cursor.fetchone()['count']
            print(f"分录数: {detail_count}")

            # 辅助项统计
            cursor.execute("SELECT COUNT(*) as count FROM auxiliary_items")
            auxiliary_count = cursor.fetchone()['count']
            print(f"辅助项数: {auxiliary_count}")

            # 金额统计
            cursor.execute("""
                SELECT
                    SUM(total_debit) as total_debit,
                    SUM(total_credit) as total_credit
                FROM vouchers
            """)
            amount_row = cursor.fetchone()
            total_debit = amount_row['total_debit'] or 0
            total_credit = amount_row['total_credit'] or 0
            print(f"借方总额: {total_debit:,.2f}")
            print(f"贷方总额: {total_credit:,.2f}")
            print(f"借贷差额: {total_debit - total_credit:,.2f}")

            # 按公司统计
            print("\n按公司统计:")
            cursor.execute("""
                SELECT
                    c.name as company_name,
                    COUNT(DISTINCT v.id) as voucher_count,
                    COUNT(vd.id) as detail_count,
                    SUM(v.total_debit) as total_debit,
                    SUM(v.total_credit) as total_credit
                FROM companies c
                LEFT JOIN account_books ab ON c.id = ab.company_id
                LEFT JOIN vouchers v ON ab.id = v.book_id
                LEFT JOIN voucher_details vd ON v.id = vd.voucher_id
                GROUP BY c.id
                ORDER BY c.name
            """)

            for row in cursor.fetchall():
                print(f"  {row['company_name']}:")
                print(f"    凭证数: {row['voucher_count']}")
                print(f"    分录数: {row['detail_count']}")
                print(f"    借方总额: {row['total_debit'] or 0:,.2f}")
                print(f"    贷方总额: {row['total_credit'] or 0:,.2f}")

        finally:
            self.close()

    def validate_data_consistency(self) -> List[str]:
        """
        验证数据一致性

        Returns:
            List[str]: 错误信息列表
        """
        errors = []

        conn = self.connect()
        cursor = conn.cursor()

        try:
            # 1. 检查凭证借贷平衡
            print("\n检查凭证借贷平衡...")
            cursor.execute("""
                SELECT
                    v.voucher_number,
                    v.voucher_date,
                    SUM(vd.debit_amount) as total_debit,
                    SUM(vd.credit_amount) as total_credit,
                    ABS(SUM(vd.debit_amount) - SUM(vd.credit_amount)) as difference
                FROM voucher_details vd
                JOIN vouchers v ON vd.voucher_id = v.id
                GROUP BY v.id
                HAVING ABS(SUM(vd.debit_amount) - SUM(vd.credit_amount)) > 0.01
            """)

            unbalanced = cursor.fetchall()
            if unbalanced:
                errors.append(f"发现 {len(unbalanced)} 个不平衡的凭证")
                for row in unbalanced[:5]:  # 只显示前5个
                    errors.append(f"  凭证 {row['voucher_number']} ({row['voucher_date']}): "
                                 f"借方={row['total_debit']:.2f}, 贷方={row['total_credit']:.2f}, "
                                 f"差额={row['difference']:.2f}")

            # 2. 检查孤立记录
            print("检查孤立记录...")

            # 检查没有对应凭证的明细
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM voucher_details vd
                LEFT JOIN vouchers v ON vd.voucher_id = v.id
                WHERE v.id IS NULL
            """)
            orphaned_details = cursor.fetchone()['count']
            if orphaned_details > 0:
                errors.append(f"发现 {orphaned_details} 条孤立的明细记录")

            # 检查没有对应明细的凭证
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM vouchers v
                LEFT JOIN voucher_details vd ON v.id = vd.voucher_id
                WHERE vd.id IS NULL
            """)
            empty_vouchers = cursor.fetchone()['count']
            if empty_vouchers > 0:
                errors.append(f"发现 {empty_vouchers} 个空的凭证")

            # 3. 检查数据完整性
            print("检查数据完整性...")

            # 检查必填字段
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM voucher_details
                WHERE subject_id IS NULL OR subject_id = 0
            """)
            missing_subject = cursor.fetchone()['count']
            if missing_subject > 0:
                errors.append(f"发现 {missing_subject} 条缺少科目的明细")

            if errors:
                print("✗ 数据一致性检查发现错误")
            else:
                print("✓ 数据一致性检查通过")

        finally:
            self.close()

        return errors


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='CSV序时账转数据库工具')
    parser.add_argument('--data-dir', default='../data', help='数据目录路径')
    parser.add_argument('--db-path', default='../database/accounting.db', help='数据库文件路径')
    parser.add_argument('--single-file', help='处理单个CSV文件')
    parser.add_argument('--year', type=int, help='数据年份（用于单个文件）')
    parser.add_argument('--validate', action='store_true', help='验证数据一致性')
    parser.add_argument('--stats', action='store_true', help='显示统计信息')
    parser.add_argument('--reset', action='store_true', help='重置数据库（删除所有表）')

    args = parser.parse_args()

    converter = CSVToDBConverter(args.db_path)

    try:
        if args.reset:
            print("重置数据库...")
            converter.schema.drop_all_tables()
            print("数据库已重置")
            return

        if args.validate:
            print("验证数据一致性...")
            errors = converter.validate_data_consistency()
            if errors:
                print("\n发现以下错误:")
                for error in errors:
                    print(f"  - {error}")
            return

        if args.stats:
            converter.show_statistics()
            return

        if args.single_file:
            # 处理单个文件
            if not os.path.exists(args.single_file):
                print(f"错误：文件不存在: {args.single_file}")
                return 1

            # 确保数据库已初始化
            converter.schema.create_tables()
            converter.schema.create_indexes()

            converter.process_csv_file(args.single_file, args.year)
        else:
            # 处理所有文件
            converter.process_all_files(args.data_dir)

    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        converter.close()

    return 0


if __name__ == "__main__":
    exit(main())