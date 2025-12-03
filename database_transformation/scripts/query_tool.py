#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询工具模块
提供常用的会计凭证查询功能
"""

import sqlite3
import pandas as pd
from typing import List, Dict, Optional, Any
from datetime import datetime, date
import sys
import os


class QueryTool:
    """查询工具类"""

    def __init__(self, db_path: str = "../database/accounting.db"):
        """
        初始化查询工具

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.connection = None

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

    def execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        """
        执行查询并返回结果

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            List[Dict]: 查询结果列表
        """
        conn = self.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            pass  # 保持连接打开供后续使用

    def query_vouchers_by_company(self, company_name: str,
                                 start_date: Optional[str] = None,
                                 end_date: Optional[str] = None) -> pd.DataFrame:
        """
        查询某公司所有凭证

        Args:
            company_name: 公司名称
            start_date: 开始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）

        Returns:
            pd.DataFrame: 凭证数据
        """
        query = """
            SELECT
                c.name as company_name,
                ab.name as book_name,
                v.voucher_date,
                v.voucher_number,
                v.voucher_type,
                v.total_debit,
                v.total_credit,
                COUNT(vd.id) as entry_count
            FROM vouchers v
            JOIN account_books ab ON v.book_id = ab.id
            JOIN companies c ON ab.company_id = c.id
            LEFT JOIN voucher_details vd ON v.id = vd.voucher_id
            WHERE c.name = ?
        """

        params = [company_name]

        if start_date:
            query += " AND v.voucher_date >= ?"
            params.append(start_date)

        if end_date:
            query += " AND v.voucher_date <= ?"
            params.append(end_date)

        query += " GROUP BY v.id ORDER BY v.voucher_date, v.voucher_number"

        results = self.execute_query(query, tuple(params))
        return pd.DataFrame(results)

    def query_voucher_details(self, voucher_number: str,
                            voucher_date: Optional[str] = None) -> pd.DataFrame:
        """
        查询凭证明细

        Args:
            voucher_number: 凭证号
            voucher_date: 凭证日期（YYYY-MM-DD），可选

        Returns:
            pd.DataFrame: 明细数据
        """
        query = """
            SELECT
                v.voucher_date,
                v.voucher_number,
                vd.entry_number,
                vd.summary,
                s.code as subject_code,
                s.name as subject_name,
                vd.debit_amount,
                vd.credit_amount,
                vd.currency,
                vd.auxiliary_info
            FROM voucher_details vd
            JOIN vouchers v ON vd.voucher_id = v.id
            JOIN account_subjects s ON vd.subject_id = s.id
            WHERE v.voucher_number = ?
        """

        params = [voucher_number]

        if voucher_date:
            query += " AND v.voucher_date = ?"
            params.append(voucher_date)

        query += " ORDER BY vd.entry_number"

        results = self.execute_query(query, tuple(params))
        return pd.DataFrame(results)

    def query_subject_details(self, subject_code: str,
                            start_date: Optional[str] = None,
                            end_date: Optional[str] = None) -> pd.DataFrame:
        """
        查询某科目所有明细

        Args:
            subject_code: 科目编码
            start_date: 开始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）

        Returns:
            pd.DataFrame: 明细数据
        """
        query = """
            SELECT
                v.voucher_date,
                v.voucher_number,
                vd.entry_number,
                vd.summary,
                vd.debit_amount,
                vd.credit_amount,
                vd.currency,
                c.name as company_name
            FROM voucher_details vd
            JOIN vouchers v ON vd.voucher_id = v.id
            JOIN account_subjects s ON vd.subject_id = s.id
            JOIN account_books ab ON v.book_id = ab.id
            JOIN companies c ON ab.company_id = c.id
            WHERE s.code = ?
        """

        params = [subject_code]

        if start_date:
            query += " AND v.voucher_date >= ?"
            params.append(start_date)

        if end_date:
            query += " AND v.voucher_date <= ?"
            params.append(end_date)

        query += " ORDER BY v.voucher_date, v.voucher_number, vd.entry_number"

        results = self.execute_query(query, tuple(params))
        return pd.DataFrame(results)

    def query_project_summary(self, project_name: Optional[str] = None,
                            company_name: Optional[str] = None) -> pd.DataFrame:
        """
        按项目统计金额

        Args:
            project_name: 项目名称，可选
            company_name: 公司名称，可选

        Returns:
            pd.DataFrame: 项目统计
        """
        query = """
            SELECT
                ai.item_value as project_name,
                c.name as company_name,
                COUNT(DISTINCT v.id) as voucher_count,
                COUNT(vd.id) as entry_count,
                SUM(vd.debit_amount) as total_debit,
                SUM(vd.credit_amount) as total_credit,
                SUM(vd.debit_amount - vd.credit_amount) as net_amount
            FROM voucher_details vd
            JOIN auxiliary_items ai ON vd.id = ai.detail_id
            JOIN vouchers v ON vd.voucher_id = v.id
            JOIN account_books ab ON v.book_id = ab.id
            JOIN companies c ON ab.company_id = c.id
            WHERE ai.item_type = '项目'
        """

        params = []

        if project_name:
            query += " AND ai.item_value LIKE ?"
            params.append(f"%{project_name}%")

        if company_name:
            query += " AND c.name = ?"
            params.append(company_name)

        query += """
            GROUP BY ai.item_value, c.name
            ORDER BY ABS(SUM(vd.debit_amount - vd.credit_amount)) DESC
        """

        results = self.execute_query(query, tuple(params))
        return pd.DataFrame(results)

    def query_cash_flow(self, company_name: Optional[str] = None,
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> pd.DataFrame:
        """
        现金流量分析

        Args:
            company_name: 公司名称，可选
            start_date: 开始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）

        Returns:
            pd.DataFrame: 现金流量数据
        """
        query = """
            SELECT
                strftime('%Y-%m', v.voucher_date) as month,
                c.name as company_name,
                SUM(CASE WHEN vd.debit_amount > 0 THEN vd.debit_amount ELSE 0 END) as cash_in,
                SUM(CASE WHEN vd.credit_amount > 0 THEN vd.credit_amount ELSE 0 END) as cash_out,
                SUM(vd.debit_amount - vd.credit_amount) as net_cash_flow
            FROM voucher_details vd
            JOIN vouchers v ON vd.voucher_id = v.id
            JOIN account_subjects s ON vd.subject_id = s.id
            JOIN account_books ab ON v.book_id = ab.id
            JOIN companies c ON ab.company_id = c.id
            WHERE s.code LIKE '1002%'  -- 银行存款类科目
        """

        params = []

        if company_name:
            query += " AND c.name = ?"
            params.append(company_name)

        if start_date:
            query += " AND v.voucher_date >= ?"
            params.append(start_date)

        if end_date:
            query += " AND v.voucher_date <= ?"
            params.append(end_date)

        query += """
            GROUP BY strftime('%Y-%m', v.voucher_date), c.name
            ORDER BY month, c.name
        """

        results = self.execute_query(query, tuple(params))
        return pd.DataFrame(results)

    def query_balance_check(self) -> pd.DataFrame:
        """
        借贷平衡检查

        Returns:
            pd.DataFrame: 平衡检查结果
        """
        query = """
            SELECT
                v.voucher_number,
                v.voucher_date,
                c.name as company_name,
                SUM(vd.debit_amount) as total_debit,
                SUM(vd.credit_amount) as total_credit,
                ABS(SUM(vd.debit_amount) - SUM(vd.credit_amount)) as difference,
                CASE
                    WHEN ABS(SUM(vd.debit_amount) - SUM(vd.credit_amount)) <= 0.01 THEN '平衡'
                    ELSE '不平衡'
                END as balance_status
            FROM voucher_details vd
            JOIN vouchers v ON vd.voucher_id = v.id
            JOIN account_books ab ON v.book_id = ab.id
            JOIN companies c ON ab.company_id = c.id
            GROUP BY v.id
            HAVING ABS(SUM(vd.debit_amount) - SUM(vd.credit_amount)) > 0.01
            ORDER BY ABS(SUM(vd.debit_amount) - SUM(vd.credit_amount)) DESC
        """

        results = self.execute_query(query)
        return pd.DataFrame(results)

    def query_large_transactions(self, threshold: float = 100000.0) -> pd.DataFrame:
        """
        查询大额交易

        Args:
            threshold: 金额阈值

        Returns:
            pd.DataFrame: 大额交易数据
        """
        query = """
            SELECT
                v.voucher_date,
                v.voucher_number,
                vd.entry_number,
                vd.summary,
                vd.debit_amount,
                vd.credit_amount,
                s.name as subject_name,
                c.name as company_name
            FROM voucher_details vd
            JOIN vouchers v ON vd.voucher_id = v.id
            JOIN account_subjects s ON vd.subject_id = s.id
            JOIN account_books ab ON v.book_id = ab.id
            JOIN companies c ON ab.company_id = c.id
            WHERE vd.debit_amount > ? OR vd.credit_amount > ?
            ORDER BY GREATEST(vd.debit_amount, vd.credit_amount) DESC
        """

        results = self.execute_query(query, (threshold, threshold))
        return pd.DataFrame(results)

    def query_subject_balance(self, subject_code: str,
                            as_of_date: Optional[str] = None) -> Dict[str, Any]:
        """
        查询科目余额

        Args:
            subject_code: 科目编码
            as_of_date: 截止日期（YYYY-MM-DD），如果为None则查询所有

        Returns:
            Dict: 科目余额信息
        """
        query = """
            SELECT
                s.code,
                s.name,
                s.subject_type,
                s.normal_balance,
                COUNT(vd.id) as transaction_count,
                SUM(vd.debit_amount) as total_debit,
                SUM(vd.credit_amount) as total_credit,
                SUM(vd.debit_amount - vd.credit_amount) as balance
            FROM voucher_details vd
            JOIN account_subjects s ON vd.subject_id = s.id
            JOIN vouchers v ON vd.voucher_id = v.id
            WHERE s.code = ?
        """

        params = [subject_code]

        if as_of_date:
            query += " AND v.voucher_date <= ?"
            params.append(as_of_date)

        query += " GROUP BY s.id"

        results = self.execute_query(query, tuple(params))

        if results:
            return results[0]
        else:
            return {}

    def query_monthly_summary(self, year: int, month: Optional[int] = None) -> pd.DataFrame:
        """
        月度汇总查询

        Args:
            year: 年份
            month: 月份，如果为None则查询全年

        Returns:
            pd.DataFrame: 月度汇总数据
        """
        query = """
            SELECT
                strftime('%Y-%m', v.voucher_date) as period,
                c.name as company_name,
                COUNT(DISTINCT v.id) as voucher_count,
                COUNT(vd.id) as entry_count,
                SUM(vd.debit_amount) as total_debit,
                SUM(vd.credit_amount) as total_credit,
                SUM(vd.debit_amount - vd.credit_amount) as net_amount
            FROM voucher_details vd
            JOIN vouchers v ON vd.voucher_id = v.id
            JOIN account_books ab ON v.book_id = ab.id
            JOIN companies c ON ab.company_id = c.id
            WHERE strftime('%Y', v.voucher_date) = ?
        """

        params = [str(year)]

        if month:
            query += " AND strftime('%m', v.voucher_date) = ?"
            params.append(f"{month:02d}")

        query += """
            GROUP BY strftime('%Y-%m', v.voucher_date), c.name
            ORDER BY period, c.name
        """

        results = self.execute_query(query, tuple(params))
        return pd.DataFrame(results)

    def export_to_csv(self, df: pd.DataFrame, output_path: str):
        """
        导出查询结果到CSV

        Args:
            df: 要导出的DataFrame
            output_path: 输出文件路径
        """
        if df.empty:
            print("警告：没有数据可导出")
            return

        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"数据已导出到: {output_path}")
        print(f"记录数: {len(df)}")

    def interactive_query(self):
        """交互式查询界面"""
        print("=" * 60)
        print("会计凭证查询工具")
        print("=" * 60)

        while True:
            print("\n请选择查询类型:")
            print("1. 查询公司凭证")
            print("2. 查询凭证明细")
            print("3. 查询科目明细")
            print("4. 项目统计")
            print("5. 现金流量分析")
            print("6. 借贷平衡检查")
            print("7. 大额交易查询")
            print("8. 科目余额查询")
            print("9. 月度汇总")
            print("0. 退出")

            choice = input("\n请输入选项 (0-9): ").strip()

            if choice == '0':
                print("再见！")
                break

            try:
                if choice == '1':
                    company = input("公司名称: ").strip()
                    start_date = input("开始日期 (YYYY-MM-DD, 可选): ").strip() or None
                    end_date = input("结束日期 (YYYY-MM-DD, 可选): ").strip() or None

                    df = self.query_vouchers_by_company(company, start_date, end_date)
                    print(f"\n找到 {len(df)} 条记录")
                    if not df.empty:
                        print(df.to_string())

                        export = input("\n导出到CSV? (y/n): ").strip().lower()
                        if export == 'y':
                            filename = f"vouchers_{company}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                            self.export_to_csv(df, filename)

                elif choice == '2':
                    voucher_no = input("凭证号: ").strip()
                    voucher_date = input("凭证日期 (YYYY-MM-DD, 可选): ").strip() or None

                    df = self.query_voucher_details(voucher_no, voucher_date)
                    print(f"\n找到 {len(df)} 条记录")
                    if not df.empty:
                        print(df.to_string())

                elif choice == '3':
                    subject_code = input("科目编码: ").strip()
                    start_date = input("开始日期 (YYYY-MM-DD, 可选): ").strip() or None
                    end_date = input("结束日期 (YYYY-MM-DD, 可选): ").strip() or None

                    df = self.query_subject_details(subject_code, start_date, end_date)
                    print(f"\n找到 {len(df)} 条记录")
                    if not df.empty:
                        print(df.to_string())

                elif choice == '4':
                    project_name = input("项目名称 (可选): ").strip() or None
                    company_name = input("公司名称 (可选): ").strip() or None

                    df = self.query_project_summary(project_name, company_name)
                    print(f"\n找到 {len(df)} 条记录")
                    if not df.empty:
                        print(df.to_string())

                elif choice == '5':
                    company_name = input("公司名称 (可选): ").strip() or None
                    start_date = input("开始日期 (YYYY-MM-DD, 可选): ").strip() or None
                    end_date = input("结束日期 (YYYY-MM-DD, 可选): ").strip() or None

                    df = self.query_cash_flow(company_name, start_date, end_date)
                    print(f"\n找到 {len(df)} 条记录")
                    if not df.empty:
                        print(df.to_string())

                elif choice == '6':
                    df = self.query_balance_check()
                    print(f"\n找到 {len(df)} 个不平衡的凭证")
                    if not df.empty:
                        print(df.to_string())

                elif choice == '7':
                    try:
                        threshold = float(input("金额阈值 (默认100000): ").strip() or 100000)
                    except ValueError:
                        threshold = 100000

                    df = self.query_large_transactions(threshold)
                    print(f"\n找到 {len(df)} 条大额交易")
                    if not df.empty:
                        print(df.to_string())

                elif choice == '8':
                    subject_code = input("科目编码: ").strip()
                    as_of_date = input("截止日期 (YYYY-MM-DD, 可选): ").strip() or None

                    result = self.query_subject_balance(subject_code, as_of_date)
                    if result:
                        print("\n科目余额信息:")
                        for key, value in result.items():
                            print(f"  {key}: {value}")
                    else:
                        print("未找到该科目")

                elif choice == '9':
                    try:
                        year = int(input("年份: ").strip())
                        month_input = input("月份 (1-12, 可选): ").strip()
                        month = int(month_input) if month_input else None
                    except ValueError:
                        print("输入错误，请重新选择")
                        continue

                    df = self.query_monthly_summary(year, month)
                    print(f"\n找到 {len(df)} 条记录")
                    if not df.empty:
                        print(df.to_string())

                else:
                    print("无效选项，请重新选择")

            except Exception as e:
                print(f"查询出错: {e}")
                import traceback
                traceback.print_exc()

        self.close()


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='会计凭证查询工具')
    parser.add_argument('--db-path', default='../database/accounting.db', help='数据库文件路径')
    parser.add_argument('--interactive', action='store_true', help='启动交互式查询')
    parser.add_argument('--query', help='执行特定查询')
    parser.add_argument('--export', help='导出查询结果到CSV文件')

    args = parser.parse_args()

    tool = QueryTool(args.db_path)

    try:
        if args.interactive:
            tool.interactive_query()
        else:
            print("请使用 --interactive 参数启动交互式查询")
            print("或使用 --query 参数执行特定查询")

    finally:
        tool.close()


if __name__ == "__main__":
    main()