"""
基本功能测试脚本
测试转换功能的核心模块
"""

import os
import sys
import tempfile
import shutil

# 添加scripts目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_schema import DatabaseSchema
from data_cleaner import DataCleaner
from auxiliary_parser import AuxiliaryParser


def test_database_schema():
    """测试数据库schema功能"""
    print("[测试] 测试数据库schema功能")
    print("-" * 50)

    # 创建临时数据库
    temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    temp_db.close()

    try:
        db = DatabaseSchema(temp_db.name)

        # 测试创建表
        db.create_tables()
        print("[成功] 数据库表创建成功")

        # 测试创建索引
        db.create_indexes()
        print("[成功] 数据库索引创建成功")

        # 测试获取表信息
        db.get_table_info()

        # 测试验证schema
        is_valid = db.validate_schema()
        if is_valid:
            print("[成功] 数据库schema验证通过")
        else:
            print("[失败] 数据库schema验证失败")

        # 测试删除表
        db.drop_all_tables()
        print("[成功] 数据库表删除成功")

    except Exception as e:
        print(f"[失败] 数据库schema测试出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 清理临时文件
        try:
            if os.path.exists(temp_db.name):
                os.unlink(temp_db.name)
        except Exception as e:
            print(f"[警告] 清理临时文件失败: {e}")

    print()


def test_data_cleaner():
    """测试数据清洗功能"""
    print("[测试] 测试数据清洗功能")
    print("-" * 50)

    cleaner = DataCleaner()

    # 测试金额清理
    test_amounts = [
        ("542,884.60", 542884.60),
        ("1,234.56", 1234.56),
        ("1000", 1000.0),
        ("", 0.0),
        (None, 0.0),
        ("abc", 0.0)
    ]

    print("测试金额清理:")
    for input_val, expected in test_amounts:
        result = cleaner.clean_amount(input_val)
        status = "[成功]" if abs(result - expected) < 0.01 else "[失败]"
        print(f"  {status} '{input_val}' -> {result:.2f} (期望: {expected:.2f})")

    # 测试公司信息提取
    test_companies = [
        ("广东和立交通养护科技有限公司-省交院账簿类型",
         {"company_name": "广东和立交通养护科技有限公司", "book_type": "省交院账簿类型"}),
        ("盛翔公司-默认账簿",
         {"company_name": "盛翔公司", "book_type": "默认账簿"}),
        ("单独公司名称",
         {"company_name": "单独公司名称", "book_type": "默认账簿"})
    ]

    print("\n测试公司信息提取:")
    for input_val, expected in test_companies:
        result = cleaner.extract_company_info(input_val)
        status = "[成功]" if result == expected else "[失败]"
        print(f"  {status} '{input_val}' -> {result}")

    # 测试凭证信息提取
    test_vouchers = [
        ("银付-0001", {"voucher_type": "银行付款", "voucher_seq": "0001"}),
        ("银收-0023", {"voucher_type": "银行收款", "voucher_seq": "0023"}),
        ("转-0100", {"voucher_type": "转账", "voucher_seq": "0100"}),
        ("未知-123", {"voucher_type": "未知", "voucher_seq": "123"})
    ]

    print("\n测试凭证信息提取:")
    for input_val, expected in test_vouchers:
        result = cleaner.extract_voucher_info(input_val)
        status = "[成功]" if result == expected else "[失败]"
        print(f"  {status} '{input_val}' -> {result}")

    # 测试科目信息解析
    test_subjects = [
        ("100201\\银行存款\\工商银行", {
            "subject_code": "100201",
            "subject_name": "工商银行",
            "full_name": "100201\\银行存款\\工商银行",
            "level": 3
        }),
        ("224101\\其他应付款\\单位往来", {
            "subject_code": "224101",
            "subject_name": "单位往来",
            "full_name": "224101\\其他应付款\\单位往来",
            "level": 3
        })
    ]

    print("\n测试科目信息解析:")
    for input_val, expected in test_subjects:
        result = cleaner.parse_subject_info(input_val)
        # 只检查关键字段
        check_fields = ["subject_code", "subject_name", "full_name", "level"]
        passed = all(result.get(field) == expected.get(field) for field in check_fields)
        status = "[成功]" if passed else "[失败]"
        print(f"  {status} '{input_val}' -> 编码:{result.get('subject_code')}, 名称:{result.get('subject_name')}, 层级:{result.get('level')}")

    print()


def test_auxiliary_parser():
    """测试辅助项解析功能"""
    print("[测试] 测试辅助项解析功能")
    print("-" * 50)

    parser = AuxiliaryParser()

    # 测试辅助项解析
    test_auxiliary = [
        ("【客商：中国电信股份有限公司广州分公司】【款项名称：无】【绩效部门hl：公司本部】", 3),
        ("【银行账户：中国工商银行广州东城支行5746】", 1),
        ("【项目：广州至深圳高速公路扩建工程】【部门：工程部】", 2),
        ("", 0),
        ("无效格式", 0),
        ("【缺少右括号", 0)
    ]

    print("测试辅助项解析:")
    for input_val, expected_count in test_auxiliary:
        items = parser.parse_auxiliary_info(input_val)
        status = "[成功]" if len(items) == expected_count else "[失败]"
        print(f"  {status} '{input_val}' -> 解析到 {len(items)} 个项")
        if items:
            for i, item in enumerate(items[:2]):  # 只显示前2个
                print(f"    项{i+1}: 类型='{item.get('item_type')}', 值='{item.get('item_value')}'")

    # 测试格式验证
    print("\n测试辅助项格式验证:")
    test_validation = [
        ("【客商：ABC公司】", True),
        ("【缺少右括号", False),
        ("】只有右括号", False),
        ("【类型1：值1】【类型2：值2】", True),
        ("【括号不匹配】", False)
    ]

    for input_val, expected_valid in test_validation:
        is_valid, errors = parser.validate_auxiliary_format(input_val)
        status = "[成功]" if is_valid == expected_valid else "[失败]"
        print(f"  {status} '{input_val}' -> 有效: {is_valid}, 错误: {errors}")

    print()


def main():
    """主测试函数"""
    print("[开始] 开始基本功能测试")
    print("=" * 60)

    try:
        test_database_schema()
        test_data_cleaner()
        test_auxiliary_parser()

        print("=" * 60)
        print("[完成] 所有基本功能测试完成!")

    except Exception as e:
        print(f"[失败] 测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())