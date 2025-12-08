"""
辅助项解析器测试
测试正则表达式修复和类型映射扩展
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
from src.data_conversion.parser import AuxiliaryParser


class TestAuxiliaryParser(unittest.TestCase):
    """辅助项解析器测试类"""

    def setUp(self):
        """测试前准备"""
        self.parser = AuxiliaryParser(max_value_length=100)

    def test_regex_fix_for_right_bracket_in_value(self):
        """测试正则表达式修复：值中包含右括号"""
        test_cases = [
            {
                "input": "【托外流水号：粤和立【2022】施工【0017】号】",
                "expected_type": "external_flow_number",
                "expected_value": "粤和立【2022】施工【0017】号"
            },
            {
                "input": "【托外流水号：粤和立【2021】施工【0012】号托外1】",
                "expected_type": "external_flow_number",
                "expected_value": "粤和立【2021】施工【0012】号托外1"
            },
            {
                "input": "【托外流水号：粤东【2021】检测【0016】号-托1】",
                "expected_type": "external_flow_number",
                "expected_value": "粤东【2021】检测【0016】号-托1"
            },
            {
                "input": "【项目：2020年潮州市潮安区及城区普通公路桥梁技术状况检查工程检查粤东【2021】检测【0016】号】",
                "expected_type": "project",
                "expected_value": "2020年潮州市潮安区及城区普通公路桥梁技术状况检查工程检查粤东【2021】检测【0016】号"
            }
        ]

        for i, test_case in enumerate(test_cases):
            with self.subTest(f"测试用例 {i+1}: {test_case['input'][:50]}..."):
                items = self.parser.parse_auxiliary_info(test_case["input"])

                self.assertEqual(len(items), 1, f"应该解析出1个项，实际解析出{len(items)}个")

                item = items[0]
                self.assertEqual(item["item_type"], test_case["expected_type"],
                               f"类型不匹配: 期望={test_case['expected_type']}, 实际={item['item_type']}")
                self.assertEqual(item["item_value"], test_case["expected_value"],
                               f"值不匹配: 期望={test_case['expected_value']}, 实际={item['item_value']}")

    def test_new_type_mappings(self):
        """测试新增的类型映射"""
        test_cases = [
            ("【托外流水号：其他】", "external_flow_number", "其他"),
            ("【业务类别：设计服务】", "business_category", "设计服务"),
            ("【物业地址：广州市天河区】", "property_address", "广州市天河区"),
            ("【银行档案：中国银行】", "bank_archive", "中国银行"),
            ("【合同编号：2022-R-0113】", "contract_number", "2022-R-0113"),
            ("【发票号码：INV20220001】", "invoice_number", "INV20220001"),
            ("【收款单位：ABC公司】", "receiving_unit", "ABC公司"),
            ("【付款单位：XYZ公司】", "paying_unit", "XYZ公司"),
        ]

        for i, (input_text, expected_type, expected_value) in enumerate(test_cases):
            with self.subTest(f"测试类型映射 {i+1}: {expected_type}"):
                items = self.parser.parse_auxiliary_info(input_text)

                self.assertEqual(len(items), 1, f"应该解析出1个项，实际解析出{len(items)}个")

                item = items[0]
                self.assertEqual(item["item_type"], expected_type,
                               f"类型映射失败: 期望={expected_type}, 实际={item['item_type']}")
                self.assertEqual(item["item_value"], expected_value,
                               f"值不匹配: 期望={expected_value}, 实际={item['item_value']}")

    def test_value_length_validation(self):
        """测试值长度验证和截断"""
        # 创建一个小长度限制的解析器
        short_parser = AuxiliaryParser(max_value_length=10)

        test_cases = [
            {
                "input": "【项目：这是一个很长的项目名称超过10个字符】",
                "expected_truncated": True,
                "expected_length": 10,
                "description": "超长值应该被截断"
            },
            {
                "input": "【项目：短项目】",
                "expected_truncated": False,
                "expected_length": 3,
                "description": "短值不应该被截断"
            },
            {
                "input": "【项目：正好十个字】",  # 5个中文字符 = 10字节（假设UTF-8）
                "expected_truncated": False,
                "expected_length": 5,
                "description": "正好达到限制的值不应该被截断"
            }
        ]

        for i, test_case in enumerate(test_cases):
            with self.subTest(f"测试长度验证 {i+1}: {test_case['description']}"):
                items = short_parser.parse_auxiliary_info(test_case["input"])

                self.assertEqual(len(items), 1, f"应该解析出1个项")

                item = items[0]
                actual_length = len(item["item_value"])

                if test_case["expected_truncated"]:
                    self.assertLessEqual(actual_length, 10,
                                       f"值应该被截断到10个字符以内，实际长度={actual_length}")
                    self.assertIn("value_warning", item,
                                "超长值应该包含警告信息")
                else:
                    self.assertEqual(actual_length, test_case["expected_length"],
                                   f"值长度不匹配: 期望={test_case['expected_length']}, 实际={actual_length}")
                    self.assertNotIn("value_warning", item,
                                   "正常值不应该包含警告信息")

    def test_smart_truncation(self):
        """测试智能截断（不截断在特殊字符中间）"""
        short_parser = AuxiliaryParser(max_value_length=10)  # 更短的截断长度

        test_cases = [
            {
                "input": "【项目：项目名称【2022】编号001】",
                "min_expected_length": 8,  # 至少8个字符
                "max_expected_length": 10, # 不超过10个字符
                "description": "应该被截断到10字符以内"
            },
            {
                "input": "【项目：这是一个很长的项目名称需要被截断】",
                "min_expected_length": 8,
                "max_expected_length": 10,
                "description": "长值应该被截断"
            }
        ]

        for i, test_case in enumerate(test_cases):
            with self.subTest(f"测试智能截断 {i+1}: {test_case['description']}"):
                items = short_parser.parse_auxiliary_info(test_case["input"])

                self.assertEqual(len(items), 1, f"应该解析出1个项")

                item = items[0]
                value_length = len(item["item_value"])

                # 检查长度在预期范围内
                self.assertGreaterEqual(value_length, test_case["min_expected_length"],
                                      f"值长度应该至少{test_case['min_expected_length']}字符，实际长度={value_length}")
                self.assertLessEqual(value_length, test_case["max_expected_length"],
                                   f"值长度应该不超过{test_case['max_expected_length']}字符，实际长度={value_length}")

                # 检查是否有截断警告
                self.assertIn("value_warning", item,
                            f"截断的值应该包含警告信息，实际item={item}")

    def test_multiple_items_parsing(self):
        """测试多个辅助项的解析"""
        input_text = "【客商：中国电信】【项目：网络工程】【托外流水号：粤和立【2022】施工【0017】号】"

        items = self.parser.parse_auxiliary_info(input_text)

        self.assertEqual(len(items), 3, f"应该解析出3个项，实际解析出{len(items)}个")

        # 检查每个项
        expected_items = [
            {"type": "supplier_customer", "value": "中国电信"},
            {"type": "project", "value": "网络工程"},
            {"type": "external_flow_number", "value": "粤和立【2022】施工【0017】号"}
        ]

        for i, (item, expected) in enumerate(zip(items, expected_items)):
            with self.subTest(f"检查第{i+1}个项"):
                self.assertEqual(item["item_type"], expected["type"],
                               f"类型不匹配: 期望={expected['type']}, 实际={item['item_type']}")
                self.assertEqual(item["item_value"], expected["value"],
                               f"值不匹配: 期望={expected['value']}, 实际={item['item_value']}")

    def test_empty_and_invalid_input(self):
        """测试空值和无效输入"""
        test_cases = [
            ("", 0, "空字符串"),
            (None, 0, "None值"),
            ("无效格式", 0, "无效格式"),
            ("【缺少右括号", 0, "缺少右括号"),
            ("】只有右括号", 0, "只有右括号"),
        ]

        for i, (input_text, expected_count, description) in enumerate(test_cases):
            with self.subTest(f"测试无效输入 {i+1}: {description}"):
                items = self.parser.parse_auxiliary_info(input_text)
                self.assertEqual(len(items), expected_count,
                               f"应该解析出{expected_count}个项，实际解析出{len(items)}个")

    def test_validate_item_value_length_method(self):
        """测试validate_item_value_length方法"""
        test_cases = [
            ("短值", True, "", "短值应该通过验证"),
            ("这是一个比较长的值但是还在限制范围内", True, "", "在限制范围内的值应该通过"),
            ("x" * 101, False, "值长度 101 超过最大限制 100", "超长值应该失败"),
            ("", True, "", "空值应该通过验证"),
        ]

        for i, (value, expected_valid, expected_msg, description) in enumerate(test_cases):
            with self.subTest(f"测试长度验证方法 {i+1}: {description}"):
                is_valid, message = self.parser.validate_item_value_length(value)

                self.assertEqual(is_valid, expected_valid,
                               f"验证结果不匹配: 期望={expected_valid}, 实际={is_valid}")
                if not expected_valid:
                    self.assertIn("超过最大限制", message,
                                f"错误消息应该包含'超过最大限制', 实际={message}")

    def test_backward_compatibility(self):
        """测试向后兼容性：原有功能仍然正常工作"""
        test_cases = [
            ("【客商：中国电信股份有限公司广州分公司】", "supplier_customer", "中国电信股份有限公司广州分公司"),
            ("【银行账户：中国工商银行广州东城支行5746】", "bank_account", "中国工商银行广州东城支行5746"),
            ("【部门：技术部】", "department", "技术部"),
            ("【人员档案：张三】", "employee", "张三"),
            ("【款项名称：无】", "payment_item", "无"),
            ("【绩效部门hl：公司本部】", "performance_dept_hl", "公司本部"),
        ]

        for i, (input_text, expected_type, expected_value) in enumerate(test_cases):
            with self.subTest(f"测试向后兼容 {i+1}: {expected_type}"):
                items = self.parser.parse_auxiliary_info(input_text)

                self.assertEqual(len(items), 1, f"应该解析出1个项")

                item = items[0]
                self.assertEqual(item["item_type"], expected_type,
                               f"类型不匹配: 期望={expected_type}, 实际={item['item_type']}")
                self.assertEqual(item["item_value"], expected_value,
                               f"值不匹配: 期望={expected_value}, 实际={item['item_value']}")


if __name__ == "__main__":
    # 运行测试
    unittest.main(verbosity=2)