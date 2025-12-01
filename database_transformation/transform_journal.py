#!/usr/bin/env python3
"""
主转换脚本 - 将CSV序时账转换为SQLite数据库
保持原始数据的完整性，不改变负金额
"""

import pandas as pd
import sqlite3
import logging
from datetime import datetime
import os
import re

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class JournalTransformer:
    """序时账转换器"""

    def __init__(self, csv_path, db_path):
        self.csv_path = csv_path
        self.db_path = db_path
        self.conn = None

    def clean_amount(self, amount_str):
        """清洗金额，正确处理负号，但不改变符号"""
        if pd.isna(amount_str):
            return 0.0

        # 转换为字符串
        amount_str = str(amount_str)

        # 移除逗号、引号
        amount_str = amount_str.replace(',', '').replace('"', '').replace("'", "")

        # 处理特殊值
        if amount_str.lower() in ['nan', '', 'null', 'none']:
            return 0.0

        # 检查是否有负号
        is_negative = False
        if amount_str.startswith('-'):
            is_negative = True
            amount_str = amount_str[1:]  # 移除负号
        elif amount_str.endswith('-'):
            is_negative = True
            amount_str = amount_str[:-1]  # 移除末尾的负号

        # 转换为数值
        try:
            amount = float(amount_str)
            if is_negative:
                amount = -amount
            return amount
        except ValueError:
            logger.warning(f"无法转换金额: '{amount_str}'")
            return 0.0

    def load_and_clean_data(self):
        """加载并清洗数据（保持原始符号）"""
        logger.info(f"加载CSV数据: {self.csv_path}")
        df = pd.read_csv(self.csv_path, encoding='utf-8-sig')

        logger.info(f"原始数据行数: {len(df)}")

        # 清洗金额（保持原始符号）
        df['借方金额'] = df['借方'].apply(self.clean_amount)
        df['贷方金额'] = df['贷方'].apply(self.clean_amount)

        # 统计清洗结果
        negative_debit = (df['借方金额'] < 0).sum()
        negative_credit = (df['贷方金额'] < 0).sum()
        positive_debit = (df['借方金额'] > 0).sum()
        positive_credit = (df['贷方金额'] > 0).sum()
        zero_debit = (df['借方金额'] == 0).sum()
        zero_credit = (df['贷方金额'] == 0).sum()

        logger.info(f"清洗结果统计（保持原始符号）:")
        logger.info(f"  正借方: {positive_debit}条")
        logger.info(f"  负借方: {negative_debit}条")
        logger.info(f"  零借方: {zero_debit}条")
        logger.info(f"  正贷方: {positive_credit}条")
        logger.info(f"  负贷方: {negative_credit}条")
        logger.info(f"  零贷方: {zero_credit}条")

        # 金额统计
        total_debit = df['借方金额'].sum()
        total_credit = df['贷方金额'].sum()

        logger.info(f"金额统计:")
        logger.info(f"  借方总额: {total_debit:,.2f}")
        logger.info(f"  贷方总额: {total_credit:,.2f}")
        logger.info(f"  借贷差额: {total_debit - total_credit:,.2f}")

        # 负金额详细统计
        if negative_debit > 0:
            neg_debit_total = df[df['借方金额'] < 0]['借方金额'].sum()
            logger.info(f"  负借方总额: {neg_debit_total:,.2f} ({negative_debit}条)")

        if negative_credit > 0:
            neg_credit_total = df[df['贷方金额'] < 0]['贷方金额'].sum()
            logger.info(f"  负贷方总额: {neg_credit_total:,.2f} ({negative_credit}条)")

        return df

    def create_database(self, df):
        """创建数据库（保持原始符号）"""
        logger.info(f"创建数据库: {self.db_path}")

        # 连接数据库
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")

        # 清理现有表
        self.conn.executescript("""
            DROP TABLE IF EXISTS accounts;
            DROP TABLE IF EXISTS vouchers;
            DROP TABLE IF EXISTS transactions;
            DROP TABLE IF EXISTS balance_adjustments;
        """)

        # 创建表结构（允许负金额）
        self.conn.executescript("""
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                type TEXT,
                level INTEGER
            );

            CREATE TABLE vouchers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                voucher_no TEXT UNIQUE NOT NULL,
                date TEXT NOT NULL,
                type TEXT,
                description TEXT,
                is_balanced INTEGER DEFAULT 1
            );

            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                voucher_id TEXT NOT NULL,
                line_no INTEGER NOT NULL,
                account_code TEXT NOT NULL,
                date TEXT NOT NULL,
                debit_credit TEXT NOT NULL CHECK (debit_credit IN ('D', 'C')),
                amount REAL NOT NULL,  -- 允许负值
                description TEXT,
                voucher_no TEXT NOT NULL
            );

            CREATE TABLE balance_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                voucher_no TEXT NOT NULL,
                adjustment_type TEXT NOT NULL,
                adjustment_amount REAL NOT NULL,
                reason TEXT,
                created_date TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX idx_transactions_voucher ON transactions(voucher_id);
            CREATE INDEX idx_transactions_account ON transactions(account_code);
            CREATE INDEX idx_transactions_date ON transactions(date);
        """)

        # 构建科目表
        unique_accounts = df[['科目编码', '科目名称']].drop_duplicates().dropna()
        for _, account in unique_accounts.iterrows():
            self.conn.execute("""
                INSERT INTO accounts (code, name)
                VALUES (?, ?)
            """, (
                str(account['科目编码']),
                str(account['科目名称'])
            ))

        # 构建凭证表和处理交易记录
        voucher_groups = df.groupby('凭证号')

        for voucher_no, voucher_data in voucher_groups:
            # 凭证基本信息
            first_row = voucher_data.iloc[0]
            date = f"2025-{int(first_row['月']):02d}-{int(first_row['日']):02d}"

            # 提取凭证类型
            voucher_type = '其他'
            if isinstance(first_row['凭证号'], str):
                match = re.match(r'([^\d]+)', first_row['凭证号'])
                if match:
                    voucher_type = match.group(1)

            # 检查凭证平衡
            debit_sum = voucher_data['借方金额'].sum()
            credit_sum = voucher_data['贷方金额'].sum()
            is_balanced = abs(debit_sum - credit_sum) <= 0.01

            # 插入凭证记录
            self.conn.execute("""
                INSERT INTO vouchers (voucher_no, date, type, description, is_balanced)
                VALUES (?, ?, ?, ?, ?)
            """, (
                str(voucher_no),
                date,
                voucher_type,
                str(first_row['摘要']) if not pd.isna(first_row['摘要']) else '',
                1 if is_balanced else 0
            ))

            # 处理交易记录（保持原始符号）
            for _, row in voucher_data.iterrows():
                line_no = int(row['分录号']) if not pd.isna(row['分录号']) else 0
                account_code = str(row['科目编码']) if not pd.isna(row['科目编码']) else ''
                description = str(row['摘要']) if not pd.isna(row['摘要']) else ''

                # 借方记录（包括负借方）
                if row['借方金额'] != 0:
                    self.conn.execute("""
                        INSERT INTO transactions (voucher_id, line_no, account_code, date,
                                                debit_credit, amount, description, voucher_no)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        f"{voucher_no}_{line_no}_D",
                        line_no,
                        account_code,
                        date,
                        'D',
                        float(row['借方金额']),  # 保持原始符号
                        description,
                        str(voucher_no)
                    ))

                # 贷方记录（包括负贷方）
                if row['贷方金额'] != 0:
                    self.conn.execute("""
                        INSERT INTO transactions (voucher_id, line_no, account_code, date,
                                                debit_credit, amount, description, voucher_no)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        f"{voucher_no}_{line_no}_C",
                        line_no,
                        account_code,
                        date,
                        'C',
                        float(row['贷方金额']),  # 保持原始符号
                        description,
                        str(voucher_no)
                    ))

        self.conn.commit()

        # 统计结果
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM accounts")
        account_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM vouchers")
        voucher_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM transactions")
        transaction_count = cursor.fetchone()[0]

        # 验证最终平衡
        cursor.execute("""
            SELECT
                SUM(CASE WHEN debit_credit = 'D' THEN amount ELSE 0 END) as total_debit,
                SUM(CASE WHEN debit_credit = 'C' THEN amount ELSE 0 END) as total_credit
            FROM transactions
        """)
        balance = cursor.fetchone()
        final_diff = balance[0] - balance[1]

        # 负金额统计
        cursor.execute("""
            SELECT
                COUNT(*) as negative_count,
                SUM(amount) as negative_total
            FROM transactions
            WHERE amount < 0
        """)
        negative_stats = cursor.fetchone()

        logger.info(f"数据库创建完成:")
        logger.info(f"  科目数: {account_count}")
        logger.info(f"  凭证数: {voucher_count}")
        logger.info(f"  交易记录数: {transaction_count}")
        logger.info(f"  借方总额: {balance[0]:,.2f}")
        logger.info(f"  贷方总额: {balance[1]:,.2f}")
        logger.info(f"  借贷差额: {final_diff:,.2f}")

        if negative_stats[0] > 0:
            logger.info(f"  负金额记录: {negative_stats[0]}条，总额: {negative_stats[1]:,.2f}")

        if abs(final_diff) < 0.01:
            logger.info("✅ 数据库借贷平衡验证通过")
        else:
            logger.error("❌ 数据库借贷不平衡")
            raise ValueError("数据库借贷不平衡")

        return True

    def run_transformation(self):
        """运行完整转换流程"""
        try:
            logger.info("=" * 80)
            logger.info("开始序时账数据转换")
            logger.info("=" * 80)

            # 1. 加载并清洗数据（保持原始符号）
            df = self.load_and_clean_data()

            # 2. 创建数据库（保持原始符号）
            self.create_database(df)

            logger.info("=" * 80)
            logger.info("序时账数据转换完成")
            logger.info("=" * 80)

            return True

        except Exception as e:
            logger.error(f"转换失败: {str(e)}")
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            if self.conn:
                self.conn.close()

def main():
    """主函数"""
    # 文件路径
    csv_path = "data/序时账2025.1-9.csv"
    db_path = "accounting.db"

    # 检查文件是否存在
    if not os.path.exists(csv_path):
        logger.error(f"CSV文件不存在: {csv_path}")
        return False

    # 运行转换
    transformer = JournalTransformer(csv_path, db_path)
    success = transformer.run_transformation()

    if success:
        logger.info(f"✅ 转换成功！数据库文件: {db_path}")
    else:
        logger.error("❌ 转换失败")

    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)