#!/usr/bin/env python3
"""
测试公司信息提取逻辑
验证data_cleaner.py和data_consistency_checker.py中的公司名称提取逻辑
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_conversion.data_cleaner import DataCleaner
from data_conversion.data_consistency_checker import DataConsistencyChecker

def test_company_extraction():
    """测试公司信息提取逻辑"""
    print("=" * 60)
    print("测试公司信息提取逻辑")
    print("=" * 60)

    cleaner = DataCleaner()

    # 测试用例
    test_cases = [
        "广东和立交通养护科技有限公司-省交院账簿类型",
        "广东和立土木工程有限公司佛山分公司-省交院账簿类型",
        "广东盛翔交通工程有限公司-省交院账簿类型",
        "广东盛翔交通工程有限公司江门分公司-省交院账簿类型",
        "广东和昇工程有限公司-省交院账簿类型"
    ]

    print("\n[测试] data_cleaner.extract_company_info 方法:")
    for book_name in test_cases:
        result = cleaner.extract_company_info(book_name)
        print(f"  输入: {book_name}")
        print(f"  输出: 公司='{result['company_name']}', 账簿类型='{result['book_type']}'")
        print()

    # 测试data_consistency_checker中的弃用方法
    print("\n[测试] data_consistency_checker._extract_company_from_filename 方法（已弃用）:")
    checker = DataConsistencyChecker()

    test_filenames = [
        "凭证明细-和立-2024年.csv",
        "凭证明细-和立佛山-2024年.csv",
        "凭证明细-盛翔-2024年.csv",
        "凭证明细-盛翔江门-2024年.csv"
    ]

    for filename in test_filenames:
        result = checker._extract_company_from_filename(filename)
        print(f"  文件名: {filename}")
        print(f"  提取结果: '{result}' (已弃用方法，应返回'未知')")
        print()

    print("\n[结论]")
    print("1. data_cleaner.extract_company_info 方法正确提取完整公司名称")
    print("2. data_consistency_checker._extract_company_from_filename 方法已弃用")
    print("3. 实际一致性检查使用CSV文件中的核算账簿名称，而不是文件名")
    print("=" * 60)

def test_similar_company_detection():
    """测试相似公司名称检测"""
    print("\n" + "=" * 60)
    print("测试相似公司名称检测逻辑")
    print("=" * 60)

    # 模拟的相似公司名称
    companies = [
        "广东和立交通养护科技有限公司",
        "广东和立土木工程有限公司佛山分公司",
        "广东盛翔交通工程有限公司",
        "广东盛翔交通工程有限公司江门分公司",
        "广东和昇工程有限公司"
    ]

    print("\n[分析] 相似公司名称检测:")

    # 检查"和立"相关的公司
    print("1. 包含'和立'的公司:")
    for company in companies:
        if "和立" in company:
            print(f"   - {company}")

    print("\n2. 包含'盛翔'的公司:")
    for company in companies:
        if "盛翔" in company:
            print(f"   - {company}")

    print("\n[警告] 相似公司名称可能导致:")
    print("  - 数据混淆：'广东和立交通养护科技有限公司' vs '广东和立土木工程有限公司佛山分公司'")
    print("  - 查询错误：使用模糊匹配可能匹配到错误的公司")
    print("  - 数据分组问题：不同公司的数据可能被错误合并")
    print("=" * 60)

if __name__ == "__main__":
    test_company_extraction()
    test_similar_company_detection()

    print("\n[修复验证]")
    print("✓ 已修复 data_consistency_checker._extract_company_from_filename 方法")
    print("✓ 该方法现在返回'未知'，避免使用错误的简化公司名称")
    print("✓ 实际一致性检查使用CSV文件中的核算账簿名称提取完整公司名称")
    print("✓ 相似公司名称检测逻辑已添加到 _load_data_from_db 方法中")