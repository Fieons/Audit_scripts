#!/usr/bin/env python3
"""
分析CSV中的零金额记录
"""

import pandas as pd
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def analyze_csv_data(csv_path):
    """分析CSV数据"""
    logger.info(f"分析CSV数据: {csv_path}")
    df = pd.read_csv(csv_path, encoding='utf-8-sig')

    # 基础数据清洗
    df['借方金额'] = pd.to_numeric(df['借方'].astype(str).str.replace(',', '').str.replace('"', '').replace('nan', ''), errors='coerce').fillna(0)
    df['贷方金额'] = pd.to_numeric(df['贷方'].astype(str).str.replace(',', '').str.replace('"', '').replace('nan', ''), errors='coerce').fillna(0)

    # 统计信息
    total_records = len(df)
    debit_records = len(df[df['借方金额'] > 0])
    credit_records = len(df[df['贷方金额'] > 0])
    zero_records = len(df[(df['借方金额'] == 0) & (df['贷方金额'] == 0)])
    both_records = len(df[(df['借方金额'] > 0) & (df['贷方金额'] > 0)])

    logger.info(f"总记录数: {total_records}")
    logger.info(f"借方记录数: {debit_records}")
    logger.info(f"贷方记录数: {credit_records}")
    logger.info(f"零金额记录数: {zero_records}")
    logger.info(f"同时有借贷记录数: {both_records}")

    # 显示一些零金额记录的示例
    zero_df = df[(df['借方金额'] == 0) & (df['贷方金额'] == 0)]
    if len(zero_df) > 0:
        logger.info(f"\n零金额记录示例（前10条）:")
        for i, (_, row) in enumerate(zero_df.head(10).iterrows()):
            logger.info(f"  第{i+1}条: 凭证号={row['凭证号']}, 分录号={row['分录号']}, 科目={row['科目名称']}")

    # 检查借贷平衡
    total_debit = df['借方金额'].sum()
    total_credit = df['贷方金额'].sum()
    difference = total_debit - total_credit

    logger.info(f"\nCSV借贷平衡分析:")
    logger.info(f"  借方总额: {total_debit:,.2f}")
    logger.info(f"  贷方总额: {total_credit:,.2f}")
    logger.info(f"  差额: {difference:,.2f}")
    logger.info(f"  平衡状态: {'✅ 平衡' if abs(difference) < 0.01 else '❌ 不平衡'}")

    # 按凭证分析平衡
    voucher_balances = df.groupby('凭证号').agg({
        '借方金额': 'sum',
        '贷方金额': 'sum'
    }).reset_index()
    voucher_balances['差额'] = voucher_balances['借方金额'] - voucher_balances['贷方金额']
    voucher_balances['绝对差额'] = voucher_balances['差额'].abs()

    unbalanced_vouchers = voucher_balances[voucher_balances['绝对差额'] > 0.01]
    logger.info(f"\n不平衡凭证分析:")
    logger.info(f"  总凭证数: {len(voucher_balances)}")
    logger.info(f"  不平衡凭证数: {len(unbalanced_vouchers)}")

    if len(unbalanced_vouchers) > 0:
        logger.info(f"  不平衡凭证示例（前5个）:")
        for i, (_, row) in enumerate(unbalanced_vouchers.head(5).iterrows()):
            logger.info(f"    凭证号={row['凭证号']}, 借方={row['借方金额']:,.2f}, 贷方={row['贷方金额']:,.2f}, 差额={row['差额']:,.2f}")

def main():
    csv_path = "database_transformation/data/序时账2025.1-9.csv"
    analyze_csv_data(csv_path)

if __name__ == "__main__":
    main()