#!/usr/bin/env python3
"""
数据转换验证脚本
验证源数据CSV与转换后数据库的一致性
"""

import pandas as pd
import sqlite3
import logging
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TransformationValidator:
    """数据转换验证器"""

    def __init__(self, csv_path, db_path):
        self.csv_path = csv_path
        self.db_path = db_path
        self.conn = None

    def clean_csv_amount(self, amount_str):
        """清洗CSV金额，与转换脚本保持一致"""
        if pd.isna(amount_str):
            return 0.0

        amount_str = str(amount_str)
        amount_str = amount_str.replace(',', '').replace('"', '').replace("'", "")

        if amount_str.lower() in ['nan', '', 'null', 'none']:
            return 0.0

        # 检查负号
        is_negative = False
        if amount_str.startswith('-'):
            is_negative = True
            amount_str = amount_str[1:]
        elif amount_str.endswith('-'):
            is_negative = True
            amount_str = amount_str[:-1]

        try:
            amount = float(amount_str)
            if is_negative:
                amount = -amount
            return amount
        except ValueError:
            return 0.0

    def load_original_csv(self):
        """加载原始CSV数据（不进行任何转换）"""
        logger.info(f"加载原始CSV数据: {self.csv_path}")
        df = pd.read_csv(self.csv_path, encoding='utf-8-sig')

        # 清洗金额（保持原始符号）
        df['借方金额'] = df['借方'].apply(self.clean_csv_amount)
        df['贷方金额'] = df['贷方'].apply(self.clean_csv_amount)

        # 统计
        total_debit = df['借方金额'].sum()
        total_credit = df['贷方金额'].sum()
        debit_count = len(df[df['借方金额'] != 0])
        credit_count = len(df[df['贷方金额'] != 0])
        negative_debit = (df['借方金额'] < 0).sum()
        negative_credit = (df['贷方金额'] < 0).sum()
        positive_debit = (df['借方金额'] > 0).sum()
        positive_credit = (df['贷方金额'] > 0).sum()

        logger.info(f"原始CSV数据统计:")
        logger.info(f"  总记录数: {len(df)}")
        logger.info(f"  借方记录数: {debit_count} (正: {positive_debit}, 负: {negative_debit})")
        logger.info(f"  贷方记录数: {credit_count} (正: {positive_credit}, 负: {negative_credit})")
        logger.info(f"  借方总额: {total_debit:,.2f}")
        logger.info(f"  贷方总额: {total_credit:,.2f}")
        logger.info(f"  借贷差额: {total_debit - total_credit:,.2f}")

        if negative_debit > 0:
            neg_debit_total = df[df['借方金额'] < 0]['借方金额'].sum()
            logger.info(f"  负借方总额: {neg_debit_total:,.2f}")

        if negative_credit > 0:
            neg_credit_total = df[df['贷方金额'] < 0]['贷方金额'].sum()
            logger.info(f"  负贷方总额: {neg_credit_total:,.2f}")

        return df

    def load_database_data(self):
        """加载数据库数据"""
        logger.info(f"连接数据库: {self.db_path}")
        self.conn = sqlite3.connect(self.db_path)

        # 加载所有交易数据
        query = """
        SELECT
            voucher_no,
            line_no,
            account_code,
            date,
            debit_credit,
            amount,
            description
        FROM transactions
        ORDER BY voucher_no, line_no, debit_credit
        """
        df_db = pd.read_sql_query(query, self.conn)

        # 分离借方和贷方
        df_debit = df_db[df_db['debit_credit'] == 'D'].copy()
        df_credit = df_db[df_db['debit_credit'] == 'C'].copy()

        df_debit['借方金额'] = df_debit['amount']
        df_credit['贷方金额'] = df_credit['amount']

        # 统计
        total_debit = df_debit['借方金额'].sum()
        total_credit = df_credit['贷方金额'].sum()
        debit_count = len(df_debit)
        credit_count = len(df_credit)
        negative_debit = (df_debit['借方金额'] < 0).sum()
        negative_credit = (df_credit['贷方金额'] < 0).sum()
        positive_debit = (df_debit['借方金额'] > 0).sum()
        positive_credit = (df_credit['贷方金额'] > 0).sum()

        logger.info(f"数据库数据统计:")
        logger.info(f"  总记录数: {len(df_db)}")
        logger.info(f"  借方记录数: {debit_count} (正: {positive_debit}, 负: {negative_debit})")
        logger.info(f"  贷方记录数: {credit_count} (正: {positive_credit}, 负: {negative_credit})")
        logger.info(f"  借方总额: {total_debit:,.2f}")
        logger.info(f"  贷方总额: {total_credit:,.2f}")
        logger.info(f"  借贷差额: {total_debit - total_credit:,.2f}")

        if negative_debit > 0:
            neg_debit_total = df_debit[df_debit['借方金额'] < 0]['借方金额'].sum()
            logger.info(f"  负借方总额: {neg_debit_total:,.2f}")

        if negative_credit > 0:
            neg_credit_total = df_credit[df_credit['贷方金额'] < 0]['贷方金额'].sum()
            logger.info(f"  负贷方总额: {neg_credit_total:,.2f}")

        return df_debit, df_credit

    def compare_record_counts(self, df_csv, df_debit, df_credit):
        """比较记录数量"""
        logger.info("\n" + "="*60)
        logger.info("记录数量比较")
        logger.info("="*60)

        # CSV数据统计
        csv_debit_count = len(df_csv[df_csv['借方金额'] != 0])
        csv_credit_count = len(df_csv[df_csv['贷方金额'] != 0])
        csv_total = csv_debit_count + csv_credit_count

        # 数据库数据统计
        db_debit_count = len(df_debit)
        db_credit_count = len(df_credit)
        db_total = db_debit_count + db_credit_count

        logger.info(f"CSV - 总记录: {csv_total}, 借方: {csv_debit_count}, 贷方: {csv_credit_count}")
        logger.info(f"DB  - 总记录: {db_total}, 借方: {db_debit_count}, 贷方: {db_credit_count}")

        # 检查差异
        debit_diff = csv_debit_count - db_debit_count
        credit_diff = csv_credit_count - db_credit_count
        total_diff = csv_total - db_total

        all_match = True

        if debit_diff != 0:
            logger.error(f"❌ 借方记录数不一致: CSV比数据库多 {debit_diff} 条")
            all_match = False
        else:
            logger.info("✅ 借方记录数一致")

        if credit_diff != 0:
            logger.error(f"❌ 贷方记录数不一致: CSV比数据库多 {credit_diff} 条")
            all_match = False
        else:
            logger.info("✅ 贷方记录数一致")

        if total_diff != 0:
            logger.error(f"❌ 总记录数不一致: CSV比数据库多 {total_diff} 条")
            all_match = False
        else:
            logger.info("✅ 总记录数一致")

        return all_match

    def compare_amounts(self, df_csv, df_debit, df_credit):
        """比较金额总数"""
        logger.info("\n" + "="*60)
        logger.info("金额总数比较")
        logger.info("="*60)

        # CSV金额统计
        csv_total_debit = df_csv['借方金额'].sum()
        csv_total_credit = df_csv['贷方金额'].sum()

        # 数据库金额统计
        db_total_debit = df_debit['借方金额'].sum()
        db_total_credit = df_credit['贷方金额'].sum()

        logger.info(f"CSV - 借方总额: {csv_total_debit:,.2f}, 贷方总额: {csv_total_credit:,.2f}")
        logger.info(f"DB  - 借方总额: {db_total_debit:,.2f}, 贷方总额: {db_total_credit:,.2f}")

        # 检查差异
        debit_diff = csv_total_debit - db_total_debit
        credit_diff = csv_total_credit - db_total_credit

        tolerance = 0.01
        all_match = True

        if abs(debit_diff) > tolerance:
            logger.error(f"❌ 借方总额不一致: 差异 {debit_diff:,.2f}")
            all_match = False
        else:
            logger.info("✅ 借方总额一致")

        if abs(credit_diff) > tolerance:
            logger.error(f"❌ 贷方总额不一致: 差异 {credit_diff:,.2f}")
            all_match = False
        else:
            logger.info("✅ 贷方总额一致")

        return all_match

    def compare_negative_amounts(self, df_csv, df_debit, df_credit):
        """比较负金额"""
        logger.info("\n" + "="*60)
        logger.info("负金额比较")
        logger.info("="*60)

        # CSV负金额统计
        csv_neg_debit = df_csv[df_csv['借方金额'] < 0]['借方金额'].sum()
        csv_neg_credit = df_csv[df_csv['贷方金额'] < 0]['贷方金额'].sum()
        csv_neg_debit_count = (df_csv['借方金额'] < 0).sum()
        csv_neg_credit_count = (df_csv['贷方金额'] < 0).sum()

        # 数据库负金额统计
        db_neg_debit = df_debit[df_debit['借方金额'] < 0]['借方金额'].sum()
        db_neg_credit = df_credit[df_credit['贷方金额'] < 0]['贷方金额'].sum()
        db_neg_debit_count = (df_debit['借方金额'] < 0).sum()
        db_neg_credit_count = (df_credit['贷方金额'] < 0).sum()

        logger.info(f"CSV - 负借方: {csv_neg_debit_count}条, 总额: {csv_neg_debit:,.2f}")
        logger.info(f"CSV - 负贷方: {csv_neg_credit_count}条, 总额: {csv_neg_credit:,.2f}")
        logger.info(f"DB  - 负借方: {db_neg_debit_count}条, 总额: {db_neg_debit:,.2f}")
        logger.info(f"DB  - 负贷方: {db_neg_credit_count}条, 总额: {db_neg_credit:,.2f}")

        # 检查差异
        tolerance = 0.01
        all_match = True

        if abs(csv_neg_debit - db_neg_debit) > tolerance:
            logger.error(f"❌ 负借方总额不一致: 差异 {csv_neg_debit - db_neg_debit:,.2f}")
            all_match = False
        else:
            logger.info("✅ 负借方总额一致")

        if abs(csv_neg_credit - db_neg_credit) > tolerance:
            logger.error(f"❌ 负贷方总额不一致: 差异 {csv_neg_credit - db_neg_credit:,.2f}")
            all_match = False
        else:
            logger.info("✅ 负贷方总额一致")

        if csv_neg_debit_count != db_neg_debit_count:
            logger.error(f"❌ 负借方记录数不一致: CSV {csv_neg_debit_count}条, DB {db_neg_debit_count}条")
            all_match = False
        else:
            logger.info("✅ 负借方记录数一致")

        if csv_neg_credit_count != db_neg_credit_count:
            logger.error(f"❌ 负贷方记录数不一致: CSV {csv_neg_credit_count}条, DB {db_neg_credit_count}条")
            all_match = False
        else:
            logger.info("✅ 负贷方记录数一致")

        return all_match

    def validate_balance(self, df_csv, df_debit, df_credit):
        """验证借贷平衡"""
        logger.info("\n" + "="*60)
        logger.info("借贷平衡验证")
        logger.info("="*60)

        # CSV平衡
        csv_balance = df_csv['借方金额'].sum() - df_csv['贷方金额'].sum()

        # 数据库平衡
        db_balance = df_debit['借方金额'].sum() - df_credit['贷方金额'].sum()

        logger.info(f"CSV借贷平衡: {csv_balance:,.2f}")
        logger.info(f"DB借贷平衡: {db_balance:,.2f}")

        tolerance = 0.01
        csv_balanced = abs(csv_balance) <= tolerance
        db_balanced = abs(db_balance) <= tolerance

        if csv_balanced:
            logger.info("✅ CSV数据借贷平衡")
        else:
            logger.error(f"❌ CSV数据借贷不平衡: {csv_balance:,.2f}")

        if db_balanced:
            logger.info("✅ 数据库数据借贷平衡")
        else:
            logger.error(f"❌ 数据库数据借贷不平衡: {db_balance:,.2f}")

        return csv_balanced and db_balanced

    def run_validation(self):
        """运行完整验证"""
        try:
            logger.info("="*80)
            logger.info("开始数据转换验证")
            logger.info("="*80)

            # 加载数据
            df_csv = self.load_original_csv()
            df_debit, df_credit = self.load_database_data()

            # 执行各项检查
            count_match = self.compare_record_counts(df_csv, df_debit, df_credit)
            amount_match = self.compare_amounts(df_csv, df_debit, df_credit)
            negative_match = self.compare_negative_amounts(df_csv, df_debit, df_credit)
            balance_valid = self.validate_balance(df_csv, df_debit, df_credit)

            # 总体结论
            logger.info("\n" + "="*80)
            logger.info("验证结果汇总")
            logger.info("="*80)

            all_valid = count_match and amount_match and negative_match and balance_valid

            if all_valid:
                logger.info("✅ 所有验证通过！数据转换完整且准确")
                logger.info("   说明：源数据与数据库数据完全一致")
            else:
                logger.error("❌ 验证失败，存在不一致")

            logger.info(f"  记录数量一致: {'✅' if count_match else '❌'}")
            logger.info(f"  金额总数一致: {'✅' if amount_match else '❌'}")
            logger.info(f"  负金额一致: {'✅' if negative_match else '❌'}")
            logger.info(f"  借贷平衡验证: {'✅' if balance_valid else '❌'}")

            return all_valid

        except Exception as e:
            logger.error(f"验证失败: {str(e)}")
            return False
        finally:
            if self.conn:
                self.conn.close()

def main():
    """主函数"""
    # 文件路径
    csv_path = "database_transformation/data/序时账2025.1-9.csv"
    db_path = "database_transformation/accounting.db"

    # 检查文件是否存在
    if not os.path.exists(csv_path):
        logger.error(f"CSV文件不存在: {csv_path}")
        return False

    if not os.path.exists(db_path):
        logger.error(f"数据库文件不存在: {db_path}")
        return False

    # 运行验证
    validator = TransformationValidator(csv_path, db_path)
    success = validator.run_validation()

    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)