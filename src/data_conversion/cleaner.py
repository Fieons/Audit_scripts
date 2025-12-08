"""
数据清洗模块
处理CSV文件中的数据清洗和规范化
"""

import pandas as pd
import re
from typing import Dict, List, Optional, Tuple, Any
import os


class DataCleaner:
    """数据清洗器"""

    def __init__(self, encoding: str = 'utf-8-sig'):
        """
        初始化数据清洗器

        Args:
            encoding: CSV文件编码，默认为utf-8-sig（处理BOM）
        """
        self.encoding = encoding

    def read_csv(self, file_path: str) -> pd.DataFrame:
        """
        读取CSV文件

        Args:
            file_path: CSV文件路径

        Returns:
            pandas DataFrame
        """
        try:
            df = pd.read_csv(file_path, encoding=self.encoding)
            print(f"[成功] 成功读取文件: {file_path}, 共 {len(df)} 行")
            return df
        except Exception as e:
            print(f"[失败] 读取文件失败: {file_path}, 错误: {e}")
            raise

    def clean_amount(self, amount_str: Any) -> float:
        """
        清理金额字符串：去除千分位分隔符，转为浮点数
        根据方案文档6.3.1的算法

        Args:
            amount_str: 金额字符串

        Returns:
            清理后的浮点数金额
        """
        if pd.isna(amount_str) or amount_str is None or str(amount_str).strip() == '':
            return 0.0

        # 转换为字符串并清理
        cleaned = str(amount_str).strip()

        # 去除千分位逗号
        cleaned = cleaned.replace(',', '')

        # 去除可能的中文货币符号
        cleaned = cleaned.replace('¥', '').replace('￥', '').replace('$', '')

        try:
            return float(cleaned)
        except ValueError:
            # 记录错误，返回0
            print(f"[警告]  警告：无法解析金额 '{amount_str}'，已转换为0")
            return 0.0

    def extract_company_info(self, book_name: str) -> Dict[str, str]:
        """
        从核算账簿名称中提取公司信息和账簿类型
        根据方案文档6.3.2的算法

        Args:
            book_name: 核算账簿名称

        Returns:
            包含公司名称和账簿类型的字典
        """
        if pd.isna(book_name) or not book_name:
            return {'company_name': '未知公司', 'book_type': '默认账簿'}

        # 示例："广东和立交通养护科技有限公司-省交院账簿类型"
        if '-' in book_name:
            parts = book_name.split('-', 1)
            company_name = parts[0].strip()
            book_type = parts[1].strip()
        else:
            company_name = book_name.strip()
            book_type = '默认账簿'

        return {
            'company_name': company_name,
            'book_type': book_type
        }

    def extract_voucher_info(self, voucher_number: str) -> Dict[str, str]:
        """
        从凭证号中提取凭证类型和序号

        Args:
            voucher_number: 凭证号，如"银付-0001"、"转-0001"

        Returns:
            包含凭证类型和序号的字典
        """
        if pd.isna(voucher_number) or not voucher_number:
            return {'voucher_type': '未知', 'voucher_seq': '0000'}

        if '-' in voucher_number:
            parts = voucher_number.split('-', 1)
            voucher_type = parts[0].strip()
            voucher_seq = parts[1].strip()
        else:
            voucher_type = '未知'
            voucher_seq = voucher_number.strip()

        # 标准化凭证类型
        type_mapping = {
            '银付': '银行付款',
            '银收': '银行收款',
            '转': '转账',
            '现付': '现金付款',
            '现收': '现金收款'
        }

        voucher_type = type_mapping.get(voucher_type, voucher_type)

        return {
            'voucher_type': voucher_type,
            'voucher_seq': voucher_seq
        }

    def parse_subject_info(self, subject_name: str) -> Dict[str, Any]:
        """
        解析科目名称，提取科目层级信息

        Args:
            subject_name: 科目名称，如"100201\银行存款\工商银行"

        Returns:
            包含科目信息的字典
        """
        if pd.isna(subject_name) or not subject_name:
            return {
                'subject_code': '',
                'subject_name': '',
                'full_name': '',
                'level': 0
            }

        # 分割科目层级
        parts = subject_name.split('\\')
        subject_code = parts[0] if len(parts) > 0 else ''
        subject_name_clean = parts[-1] if len(parts) > 1 else subject_code

        # 确定科目层级
        level = len(parts)

        # 根据科目编码确定科目类型和正常余额方向
        subject_type, normal_balance = self._get_subject_type(subject_code)

        return {
            'subject_code': subject_code,
            'subject_name': subject_name_clean,
            'full_name': subject_name,
            'level': level,
            'subject_type': subject_type,
            'normal_balance': normal_balance
        }

    def _get_subject_type(self, subject_code: str) -> Tuple[str, str]:
        """
        根据科目编码确定科目类型和正常余额方向
        根据方案文档4.3节的规则

        Args:
            subject_code: 科目编码

        Returns:
            (科目类型, 正常余额方向)
        """
        if not subject_code or len(subject_code) < 1:
            return '未知', '未知'

        first_digit = subject_code[0]

        # 根据第一位数字判断科目类型
        if first_digit == '1':
            return '资产', '借方'
        elif first_digit == '2':
            return '负债', '贷方'
        elif first_digit == '3':
            return '权益', '贷方'
        elif first_digit == '4':
            return '成本', '借方'
        elif first_digit == '5':
            return '损益-收入', '贷方'
        elif first_digit == '6':
            return '损益-费用', '借方'
        else:
            return '其他', '未知'

    def clean_dataframe(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        清洗整个DataFrame

        Args:
            df: 原始DataFrame
            year: 数据年份

        Returns:
            清洗后的DataFrame
        """
        print(f"[处理] 开始清洗数据，共 {len(df)} 行")

        # 创建副本以避免修改原始数据
        df_clean = df.copy()

        # 1. 清理金额字段（支持两种列名格式）
        # 处理借方金额列
        debit_columns = ['借方-本币', '借-本币']
        debit_col = None
        for col in debit_columns:
            if col in df_clean.columns:
                debit_col = col
                df_clean['借方-本币'] = df_clean[col].apply(self.clean_amount)
                break

        # 处理贷方金额列
        credit_columns = ['贷方-本币', '贷-本币']
        credit_col = None
        for col in credit_columns:
            if col in df_clean.columns:
                credit_col = col
                df_clean['贷方-本币'] = df_clean[col].apply(self.clean_amount)
                break

        if debit_col:
            print(f"[信息] 使用借方列: {debit_col}")
        if credit_col:
            print(f"[信息] 使用贷方列: {credit_col}")

        # 2. 提取公司信息
        if '核算账簿名称' in df_clean.columns:
            company_info = df_clean['核算账簿名称'].apply(self.extract_company_info)
            df_clean['公司名称'] = company_info.apply(lambda x: x['company_name'])
            df_clean['账簿类型'] = company_info.apply(lambda x: x['book_type'])

        # 3. 提取凭证信息
        if '凭证号' in df_clean.columns:
            voucher_info = df_clean['凭证号'].apply(self.extract_voucher_info)
            df_clean['凭证类型'] = voucher_info.apply(lambda x: x['voucher_type'])
            df_clean['凭证序号'] = voucher_info.apply(lambda x: x['voucher_seq'])

        # 4. 解析科目信息
        if '科目名称' in df_clean.columns:
            subject_info = df_clean['科目名称'].apply(self.parse_subject_info)
            df_clean['科目编码'] = subject_info.apply(lambda x: x['subject_code'])
            df_clean['科目简称'] = subject_info.apply(lambda x: x['subject_name'])
            df_clean['科目全称'] = subject_info.apply(lambda x: x['full_name'])
            df_clean['科目层级'] = subject_info.apply(lambda x: x['level'])
            df_clean['科目类型'] = subject_info.apply(lambda x: x['subject_type'])
            df_clean['正常余额方向'] = subject_info.apply(lambda x: x['normal_balance'])

        # 5. 生成完整日期
        if all(col in df_clean.columns for col in ['月', '日']):
            df_clean['年份'] = year
            df_clean['凭证日期'] = pd.to_datetime(
                df_clean['年份'].astype(str) + '-' +
                df_clean['月'].astype(str) + '-' +
                df_clean['日'].astype(str),
                errors='coerce'
            )

        # 6. 处理空值
        text_columns = ['摘要', '辅助项', '核销信息', '结算信息']
        for col in text_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].fillna('')

        # 7. 验证借贷规则
        self._validate_accounting_rules(df_clean)

        print(f"[成功] 数据清洗完成，共 {len(df_clean)} 行")
        return df_clean

    def _validate_accounting_rules(self, df: pd.DataFrame):
        """
        验证会计分录的借贷规则
        根据方案文档4.4节的验证规则

        Args:
            df: 清洗后的DataFrame
        """
        errors = []

        # 检查必填字段
        required_columns = ['月', '日', '核算账簿名称', '凭证号', '分录号', '科目编码']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"缺少必填字段: {missing_columns}")

        # 检查借贷规则
        if '借方-本币' in df.columns and '贷方-本币' in df.columns:
            # 规则1: 借方和贷方不能同时有值
            both_non_zero = df[(df['借方-本币'] > 0) & (df['贷方-本币'] > 0)]
            if len(both_non_zero) > 0:
                errors.append(f"有 {len(both_non_zero)} 条记录同时有借方和贷方金额")

            # 规则2: 借方和贷方不能同时为0
            both_zero = df[(df['借方-本币'] == 0) & (df['贷方-本币'] == 0)]
            if len(both_zero) > 0:
                errors.append(f"有 {len(both_zero)} 条记录借方和贷方金额都为0")

            # 规则3: 金额不能为负数
            negative_debit = df[df['借方-本币'] < 0]
            negative_credit = df[df['贷方-本币'] < 0]
            if len(negative_debit) > 0 or len(negative_credit) > 0:
                errors.append(f"有 {len(negative_debit)} 条借方负金额和 {len(negative_credit)} 条贷方负金额")

        if errors:
            print("[警告]  数据验证警告:")
            for error in errors:
                print(f"  - {error}")
        else:
            print("[成功] 数据验证通过")

    def get_cleaning_report(self, df_original: pd.DataFrame, df_cleaned: pd.DataFrame) -> Dict[str, Any]:
        """
        生成数据清洗报告

        Args:
            df_original: 原始DataFrame
            df_cleaned: 清洗后的DataFrame

        Returns:
            清洗报告字典
        """
        report = {
            'original_rows': len(df_original),
            'cleaned_rows': len(df_cleaned),
            'columns_added': list(set(df_cleaned.columns) - set(df_original.columns)),
            'columns_removed': list(set(df_original.columns) - set(df_cleaned.columns)),
            'amount_stats': {}
        }

        # 金额统计
        if '借方-本币' in df_cleaned.columns:
            report['amount_stats']['debit_total'] = df_cleaned['借方-本币'].sum()
            report['amount_stats']['debit_count'] = len(df_cleaned[df_cleaned['借方-本币'] > 0])
            report['amount_stats']['debit_max'] = df_cleaned['借方-本币'].max()
            report['amount_stats']['debit_min_nonzero'] = df_cleaned[df_cleaned['借方-本币'] > 0]['借方-本币'].min()

        if '贷方-本币' in df_cleaned.columns:
            report['amount_stats']['credit_total'] = df_cleaned['贷方-本币'].sum()
            report['amount_stats']['credit_count'] = len(df_cleaned[df_cleaned['贷方-本币'] > 0])
            report['amount_stats']['credit_max'] = df_cleaned['贷方-本币'].max()
            report['amount_stats']['credit_min_nonzero'] = df_cleaned[df_cleaned['贷方-本币'] > 0]['贷方-本币'].min()

        # 公司统计
        if '公司名称' in df_cleaned.columns:
            report['company_stats'] = df_cleaned['公司名称'].value_counts().to_dict()

        # 凭证类型统计
        if '凭证类型' in df_cleaned.columns:
            report['voucher_type_stats'] = df_cleaned['凭证类型'].value_counts().to_dict()

        return report

    def save_cleaned_data(self, df_cleaned: pd.DataFrame, output_path: str):
        """
        保存清洗后的数据到CSV文件

        Args:
            df_cleaned: 清洗后的DataFrame
            output_path: 输出文件路径
        """
        try:
            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            df_cleaned.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"[成功] 清洗后的数据已保存到: {output_path}")
        except Exception as e:
            print(f"[失败] 保存数据失败: {e}")
            raise


def main():
    """主函数：测试数据清洗功能"""
    import sys

    # 测试数据路径
    test_file = "../data/凭证明细-和立-2024年.csv"

    if not os.path.exists(test_file):
        print(f"[失败] 测试文件不存在: {test_file}")
        print("请确保在正确的目录下运行此脚本")
        sys.exit(1)

    cleaner = DataCleaner()

    try:
        # 读取原始数据
        df_original = cleaner.read_csv(test_file)
        print(f"原始数据列名: {list(df_original.columns)}")
        print(f"原始数据前3行:\n{df_original.head(3)}")

        # 清洗数据（假设是2024年数据）
        df_cleaned = cleaner.clean_dataframe(df_original, year=2024)

        # 显示清洗后的数据
        print(f"\n清洗后数据列名: {list(df_cleaned.columns)}")
        print(f"清洗后数据前3行:\n{df_cleaned.head(3)}")

        # 生成报告
        report = cleaner.get_cleaning_report(df_original, df_cleaned)
        print(f"\n[信息] 数据清洗报告:")
        print(f"  原始行数: {report['original_rows']}")
        print(f"  清洗后行数: {report['cleaned_rows']}")
        print(f"  新增列: {report['columns_added']}")

        if 'amount_stats' in report:
            print(f"  借方总额: {report['amount_stats'].get('debit_total', 0):,.2f}")
            print(f"  贷方总额: {report['amount_stats'].get('credit_total', 0):,.2f}")

        # 保存清洗后的数据
        output_path = "../database/cleaned_data_test.csv"
        cleaner.save_cleaned_data(df_cleaned, output_path)

    except Exception as e:
        print(f"[失败] 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()