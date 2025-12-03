#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据清洗模块
处理CSV数据，包括金额清理、辅助项解析等
"""

import pandas as pd
import re
from typing import Dict, List, Optional, Tuple
import numpy as np


class DataCleaner:
    """数据清洗器"""

    def __init__(self):
        """初始化数据清洗器"""
        self.auxiliary_pattern = re.compile(r'【([^：]+)：([^】]+)】')

    def clean_amount(self, amount_str) -> float:
        """
        清理金额字符串：去除千分位分隔符，转为浮点数

        Args:
            amount_str: 金额字符串，可能包含千分位逗号

        Returns:
            float: 清理后的金额
        """
        if pd.isna(amount_str) or str(amount_str).strip() == '':
            return 0.0

        # 去除千分位逗号和空格
        cleaned = str(amount_str).replace(',', '').strip()

        try:
            return float(cleaned)
        except ValueError:
            # 记录错误，返回0
            print(f"警告：无法解析金额 '{amount_str}'")
            return 0.0

    def extract_company_info(self, book_name: str) -> Dict[str, str]:
        """
        从核算账簿名称中提取公司信息和账簿类型

        Args:
            book_name: 核算账簿名称，如"广东和立交通养护科技有限公司-省交院账簿类型"

        Returns:
            Dict: 包含公司名称和账簿类型的字典
        """
        if pd.isna(book_name):
            return {'company_name': '未知公司', 'book_type': '未知账簿'}

        book_name_str = str(book_name)

        if '-' in book_name_str:
            parts = book_name_str.split('-', 1)
            company_name = parts[0].strip()
            book_type = parts[1].strip()
        else:
            company_name = book_name_str
            book_type = '默认账簿'

        return {
            'company_name': company_name,
            'book_type': book_type
        }

    def parse_auxiliary_info(self, text: str) -> List[Dict[str, str]]:
        """
        解析辅助项信息

        Args:
            text: 辅助项文本，格式如"【客商：中国电信股份有限公司广州分公司】【款项名称：无】"

        Returns:
            List[Dict]: 解析后的辅助项列表，每个元素包含item_type和item_value
        """
        if pd.isna(text) or str(text).strip() == '':
            return []

        text_str = str(text)
        matches = self.auxiliary_pattern.findall(text_str)

        items = []
        for match in matches:
            item_type = match[0].strip()
            item_value = match[1].strip()
            items.append({
                'item_type': item_type,
                'item_value': item_value
            })

        return items

    def extract_voucher_type(self, voucher_number: str) -> str:
        """
        从凭证号中提取凭证类型

        Args:
            voucher_number: 凭证号，如"银付-0001"

        Returns:
            str: 凭证类型（银付/银收/转）
        """
        if pd.isna(voucher_number):
            return '未知'

        voucher_str = str(voucher_number)

        if voucher_str.startswith('银付'):
            return '银付'
        elif voucher_str.startswith('银收'):
            return '银收'
        elif voucher_str.startswith('转'):
            return '转'
        else:
            return '其他'

    def parse_subject_info(self, subject_code: str, subject_name: str) -> Dict[str, str]:
        """
        解析科目信息

        Args:
            subject_code: 科目编码，如"100201"
            subject_name: 科目名称，如"100201\银行存款\工商银行"

        Returns:
            Dict: 解析后的科目信息
        """
        if pd.isna(subject_name):
            return {
                'subject_code': str(subject_code) if not pd.isna(subject_code) else '',
                'subject_name': '',
                'full_name': '',
                'level': 1
            }

        subject_name_str = str(subject_name)

        # 分离科目层级
        parts = subject_name_str.split('\\')
        base_name = parts[-1] if parts else ''

        # 确定科目层级
        level = len(parts)

        # 确定科目类型
        subject_type = self.get_subject_type(subject_code)

        # 确定正常余额方向
        normal_balance = self.get_normal_balance(subject_code)

        return {
            'subject_code': str(subject_code) if not pd.isna(subject_code) else '',
            'subject_name': base_name,
            'full_name': subject_name_str,
            'level': level,
            'subject_type': subject_type,
            'normal_balance': normal_balance
        }

    def get_subject_type(self, subject_code: str) -> str:
        """
        根据科目编码确定科目类型

        Args:
            subject_code: 科目编码

        Returns:
            str: 科目类型
        """
        if pd.isna(subject_code) or not str(subject_code).strip():
            return '未知'

        code_str = str(subject_code)

        if code_str.startswith('1'):
            return '资产'
        elif code_str.startswith('2'):
            return '负债'
        elif code_str.startswith('3'):
            return '权益'
        elif code_str.startswith('4'):
            return '成本'
        elif code_str.startswith('5'):
            return '损益-收入'
        elif code_str.startswith('6'):
            return '损益-费用'
        else:
            return '其他'

    def get_normal_balance(self, subject_code: str) -> str:
        """
        根据科目编码确定正常余额方向

        Args:
            subject_code: 科目编码

        Returns:
            str: 正常余额方向（debit/credit）
        """
        if pd.isna(subject_code) or not str(subject_code).strip():
            return '未知'

        code_str = str(subject_code)

        # 资产类、成本类、损益-费用类：正常余额为借方
        if code_str.startswith('1') or code_str.startswith('4') or code_str.startswith('6'):
            return 'debit'
        # 负债类、权益类、损益-收入类：正常余额为贷方
        elif code_str.startswith('2') or code_str.startswith('3') or code_str.startswith('5'):
            return 'credit'
        else:
            return '未知'

    def build_voucher_date(self, year: int, month: int, day: int) -> str:
        """
        构建凭证日期

        Args:
            year: 年份
            month: 月份
            day: 日期

        Returns:
            str: YYYY-MM-DD格式的日期字符串
        """
        try:
            # 确保月份和日期是两位数
            month_str = str(month).zfill(2)
            day_str = str(day).zfill(2)
            return f"{year}-{month_str}-{day_str}"
        except Exception:
            return f"{year}-01-01"

    def validate_accounting_entry(self, debit_amount: float, credit_amount: float) -> List[str]:
        """
        验证会计分录的借贷规则

        Args:
            debit_amount: 借方金额
            credit_amount: 贷方金额

        Returns:
            List[str]: 错误信息列表，空列表表示验证通过
        """
        errors = []

        # 规则1: 借方和贷方不能同时有值
        if debit_amount > 0 and credit_amount > 0:
            errors.append("同一分录不能同时有借方和贷方金额")

        # 规则2: 借方和贷方不能同时为0
        if debit_amount == 0 and credit_amount == 0:
            errors.append("分录必须有借方或贷方金额")

        # 规则3: 金额必须为正数
        if debit_amount < 0 or credit_amount < 0:
            errors.append("金额不能为负数")

        return errors

    def clean_csv_data(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        清洗CSV数据

        Args:
            df: 原始DataFrame
            year: 数据年份

        Returns:
            pd.DataFrame: 清洗后的DataFrame
        """
        print(f"开始清洗{year}年数据，原始行数: {len(df)}")

        # 创建副本
        cleaned_df = df.copy()

        # 1. 清理金额字段
        cleaned_df['借方-本币'] = cleaned_df['借方-本币'].apply(self.clean_amount)
        cleaned_df['贷方-本币'] = cleaned_df['贷方-本币'].apply(self.clean_amount)

        # 2. 提取公司信息
        company_info = cleaned_df['核算账簿名称'].apply(self.extract_company_info)
        cleaned_df['公司名称'] = company_info.apply(lambda x: x['company_name'])
        cleaned_df['账簿类型'] = company_info.apply(lambda x: x['book_type'])

        # 3. 提取凭证类型
        cleaned_df['凭证类型'] = cleaned_df['凭证号'].apply(self.extract_voucher_type)

        # 4. 解析科目信息
        subject_info = cleaned_df.apply(
            lambda row: self.parse_subject_info(row['科目编码'], row['科目名称']),
            axis=1
        )
        cleaned_df['科目编码_清洗'] = subject_info.apply(lambda x: x['subject_code'])
        cleaned_df['科目名称_清洗'] = subject_info.apply(lambda x: x['subject_name'])
        cleaned_df['科目全名'] = subject_info.apply(lambda x: x['full_name'])
        cleaned_df['科目层级'] = subject_info.apply(lambda x: x['level'])
        cleaned_df['科目类型'] = subject_info.apply(lambda x: x['subject_type'])
        cleaned_df['正常余额方向'] = subject_info.apply(lambda x: x['normal_balance'])

        # 5. 构建凭证日期
        cleaned_df['凭证日期'] = cleaned_df.apply(
            lambda row: self.build_voucher_date(year, row['月'], row['日']),
            axis=1
        )
        cleaned_df['年份'] = year

        # 6. 验证会计分录
        errors = []
        for idx, row in cleaned_df.iterrows():
            entry_errors = self.validate_accounting_entry(
                row['借方-本币'], row['贷方-本币']
            )
            if entry_errors:
                errors.append({
                    'index': idx,
                    '凭证号': row['凭证号'],
                    '分录号': row['分录号'],
                    'errors': entry_errors
                })

        if errors:
            print(f"警告：发现{len(errors)}条会计分录验证错误")
            for error in errors[:5]:  # 只显示前5个错误
                print(f"  行{error['index']}: 凭证{error['凭证号']}-{error['分录号']} - {', '.join(error['errors'])}")

        # 7. 数据质量统计
        print(f"数据清洗完成，清洗后行数: {len(cleaned_df)}")
        print(f"借方总额: {cleaned_df['借方-本币'].sum():,.2f}")
        print(f"贷方总额: {cleaned_df['贷方-本币'].sum():,.2f}")
        print(f"借贷差额: {cleaned_df['借方-本币'].sum() - cleaned_df['贷方-本币'].sum():,.2f}")

        return cleaned_df

    def load_csv_file(self, file_path: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        加载CSV文件

        Args:
            file_path: CSV文件路径
            year: 数据年份，如果为None则从文件名中提取

        Returns:
            pd.DataFrame: 加载的数据
        """
        try:
            # 尝试utf-8-sig编码（处理BOM）
            df = pd.read_csv(file_path, encoding='utf-8-sig')
        except UnicodeDecodeError:
            try:
                # 尝试gbk编码
                df = pd.read_csv(file_path, encoding='gbk')
            except UnicodeDecodeError:
                # 尝试utf-8
                df = pd.read_csv(file_path, encoding='utf-8')

        print(f"成功加载文件: {file_path}")
        print(f"数据形状: {df.shape}")
        print(f"列名: {list(df.columns)}")

        # 如果未指定年份，从文件名中提取
        if year is None:
            # 从文件名中提取年份，如"凭证明细-和立-2024年.csv"
            import re
            match = re.search(r'(\d{4})年', file_path)
            if match:
                year = int(match.group(1))
            else:
                year = 2024  # 默认值

        return self.clean_csv_data(df, year)


def main():
    """主函数：测试数据清洗功能"""
    import os

    cleaner = DataCleaner()

    # 测试数据清洗功能
    test_data = {
        '月': [1, 1],
        '日': [2, 2],
        '核算账簿名称': ['广东和立交通养护科技有限公司-省交院账簿类型', '广东和立交通养护科技有限公司-省交院账簿类型'],
        '凭证号': ['银付-0001', '银付-0001'],
        '分录号': [1, 2],
        '摘要': ['测试摘要1', '测试摘要2'],
        '科目编码': ['100201', '224101'],
        '科目名称': ['100201\\银行存款\\工商银行', '224101\\其他应付款\\单位往来'],
        '辅助项': ['【客商：测试客商】【项目：测试项目】', '【银行账户：测试银行】'],
        '币种': ['人民币', '人民币'],
        '借方-本币': ['1,000.50', ''],
        '贷方-本币': ['', '1,000.50'],
        '核销信息': ['', ''],
        '结算信息': ['', '']
    }

    df = pd.DataFrame(test_data)
    cleaned_df = cleaner.clean_csv_data(df, 2024)

    print("\n清洗后的数据:")
    print(cleaned_df[['公司名称', '账簿类型', '凭证类型', '科目编码_清洗', '科目名称_清洗', '借方-本币', '贷方-本币']])

    # 测试辅助项解析
    print("\n辅助项解析测试:")
    test_text = "【客商：中国电信股份有限公司广州分公司】【款项名称：无】【绩效部门hl：公司本部】"
    items = cleaner.parse_auxiliary_info(test_text)
    for item in items:
        print(f"  {item['item_type']}: {item['item_value']}")

    # 测试科目类型识别
    print("\n科目类型识别测试:")
    test_codes = ['100201', '224101', '500101', '660221']
    for code in test_codes:
        subject_type = cleaner.get_subject_type(code)
        normal_balance = cleaner.get_normal_balance(code)
        print(f"  科目{code}: 类型={subject_type}, 正常余额方向={normal_balance}")


if __name__ == "__main__":
    main()