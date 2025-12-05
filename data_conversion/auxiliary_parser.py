"""
辅助项解析模块
解析CSV中的辅助项信息，格式为【类型：值】
"""

import re
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd


class AuxiliaryParser:
    """辅助项解析器"""

    def __init__(self):
        """初始化解析器"""
        # 辅助项类型映射，用于标准化
        self.type_mapping = {
            '客商': 'supplier_customer',
            '供应商': 'supplier',
            '客户': 'customer',
            '项目': 'project',
            '部门': 'department',
            '银行账户': 'bank_account',
            '人员档案': 'employee',
            '员工': 'employee',
            '人员': 'employee',
            '款项名称': 'payment_item',
            '绩效部门': 'performance_dept',
            '绩效部门hl': 'performance_dept_hl',
            '往来单位': 'business_partner',
            '单位': 'unit',
            '结算方式': 'settlement_method',
            '现金流量项目': 'cash_flow_item',
            '业务员': 'salesman',
            '存货': 'inventory',
            '自定义项': 'custom_item'
        }

        # 反向映射，用于显示
        self.reverse_mapping = {v: k for k, v in self.type_mapping.items()}

    def parse_auxiliary_info(self, text: str) -> List[Dict[str, str]]:
        """
        解析辅助项信息
        根据方案文档5.3节的算法

        Args:
            text: 辅助项文本，格式如"【客商：中国电信股份有限公司广州分公司】【款项名称：无】"

        Returns:
            解析后的辅助项列表，每个项包含item_type和item_value
        """
        if not text or pd.isna(text) or str(text).strip() == '':
            return []

        text_str = str(text).strip()

        # 使用正则表达式匹配【类型：值】格式
        # 方案文档5.3节的正则表达式：r'【([^：]+)：([^】]+)】'
        pattern = r'【([^：]+)：([^】]+)】'
        matches = re.findall(pattern, text_str)

        items = []
        for match in matches:
            raw_type = match[0].strip()
            raw_value = match[1].strip()

            # 标准化类型
            standardized_type = self._standardize_type(raw_type)

            items.append({
                'raw_type': raw_type,
                'item_type': standardized_type,
                'item_value': raw_value,
                'display_type': self.reverse_mapping.get(standardized_type, raw_type)
            })

        return items

    def _standardize_type(self, raw_type: str) -> str:
        """
        标准化辅助项类型

        Args:
            raw_type: 原始类型字符串

        Returns:
            标准化后的类型
        """
        # 首先尝试精确匹配
        if raw_type in self.type_mapping:
            return self.type_mapping[raw_type]

        # 尝试模糊匹配（包含关系）
        for key, value in self.type_mapping.items():
            if key in raw_type or raw_type in key:
                return value

        # 默认返回原始类型的小写形式
        return raw_type.lower().replace(' ', '_')

    def extract_specific_items(self, text: str, target_types: List[str]) -> Dict[str, List[str]]:
        """
        提取特定类型的辅助项

        Args:
            text: 辅助项文本
            target_types: 目标类型列表

        Returns:
            按类型分组的项值列表
        """
        items = self.parse_auxiliary_info(text)
        result = {target_type: [] for target_type in target_types}

        for item in items:
            if item['item_type'] in target_types:
                result[item['item_type']].append(item['item_value'])

        return result

    def get_all_item_types(self, texts: pd.Series) -> List[str]:
        """
        从数据集中获取所有出现的辅助项类型

        Args:
            texts: 包含辅助项文本的pandas Series

        Returns:
            所有出现的辅助项类型列表
        """
        all_types = set()

        for text in texts.dropna():
            items = self.parse_auxiliary_info(text)
            for item in items:
                all_types.add(item['item_type'])

        return sorted(list(all_types))

    def create_auxiliary_summary(self, df: pd.DataFrame, auxiliary_column: str = '辅助项') -> Dict[str, Any]:
        """
        创建辅助项统计摘要

        Args:
            df: 包含辅助项的数据框
            auxiliary_column: 辅助项列名

        Returns:
            统计摘要字典
        """
        if auxiliary_column not in df.columns:
            return {'error': f'列 {auxiliary_column} 不存在'}

        summary = {
            'total_records': len(df),
            'records_with_auxiliary': len(df[df[auxiliary_column].notna() & (df[auxiliary_column] != '')]),
            'item_type_counts': {},
            'unique_values_by_type': {}
        }

        # 统计每种类型的出现次数和唯一值
        all_items = []
        for text in df[auxiliary_column].dropna():
            items = self.parse_auxiliary_info(text)
            all_items.extend(items)

        # 按类型统计
        for item in all_items:
            item_type = item['item_type']
            item_value = item['item_value']

            if item_type not in summary['item_type_counts']:
                summary['item_type_counts'][item_type] = 0
                summary['unique_values_by_type'][item_type] = set()

            summary['item_type_counts'][item_type] += 1
            summary['unique_values_by_type'][item_type].add(item_value)

        # 转换集合为列表
        for item_type in summary['unique_values_by_type']:
            summary['unique_values_by_type'][item_type] = sorted(
                list(summary['unique_values_by_type'][item_type])
            )

        return summary

    def validate_auxiliary_format(self, text: str) -> Tuple[bool, List[str]]:
        """
        验证辅助项格式是否正确

        Args:
            text: 辅助项文本

        Returns:
            (是否有效, 错误消息列表)
        """
        errors = []

        if not text or pd.isna(text):
            return True, errors  # 空值视为有效

        text_str = str(text).strip()

        # 检查是否包含完整的【】括号
        if '【' in text_str and '】' not in text_str:
            errors.append("缺少右括号】")
        if '】' in text_str and '【' not in text_str:
            errors.append("缺少左括号【")

        # 检查括号是否匹配
        open_brackets = text_str.count('【')
        close_brackets = text_str.count('】')
        if open_brackets != close_brackets:
            errors.append(f"括号不匹配：{open_brackets}个【 vs {close_brackets}个】")

        # 检查格式是否正确
        pattern = r'【([^：]+)：([^】]+)】'
        matches = re.findall(pattern, text_str)

        # 计算应该匹配的数量
        expected_matches = min(open_brackets, close_brackets)
        if len(matches) != expected_matches:
            errors.append(f"格式解析失败：找到 {len(matches)} 个有效项，预期 {expected_matches} 个")

        return len(errors) == 0, errors

    def batch_parse_to_dataframe(self, texts: pd.Series) -> pd.DataFrame:
        """
        批量解析辅助项并转换为DataFrame

        Args:
            texts: 包含辅助项文本的pandas Series

        Returns:
            解析后的DataFrame，每行对应一个辅助项
        """
        rows = []

        for idx, text in texts.items():
            items = self.parse_auxiliary_info(text)
            for item in items:
                row = {
                    'original_index': idx,
                    'raw_text': text,
                    'raw_type': item['raw_type'],
                    'item_type': item['item_type'],
                    'item_value': item['item_value'],
                    'display_type': item['display_type']
                }
                rows.append(row)

        return pd.DataFrame(rows)

    def find_duplicate_items(self, df: pd.DataFrame, auxiliary_column: str = '辅助项') -> pd.DataFrame:
        """
        查找重复的辅助项组合

        Args:
            df: 包含辅助项的数据框
            auxiliary_column: 辅助项列名

        Returns:
            包含重复项统计的DataFrame
        """
        if auxiliary_column not in df.columns:
            raise ValueError(f'列 {auxiliary_column} 不存在')

        # 创建辅助项字符串的哈希
        auxiliary_hashes = {}
        for idx, text in df[auxiliary_column].items():
            if pd.isna(text) or str(text).strip() == '':
                continue

            # 标准化辅助项字符串（排序项）
            items = self.parse_auxiliary_info(text)
            sorted_items = sorted(items, key=lambda x: x['item_type'])
            item_str = '|'.join([f"{item['item_type']}:{item['item_value']}" for item in sorted_items])

            if item_str not in auxiliary_hashes:
                auxiliary_hashes[item_str] = []
            auxiliary_hashes[item_str].append(idx)

        # 找出重复的辅助项组合
        duplicate_combinations = {k: v for k, v in auxiliary_hashes.items() if len(v) > 1}

        # 创建结果DataFrame
        results = []
        for item_str, indices in duplicate_combinations.items():
            # 解析item_str获取类型和值
            items = []
            for part in item_str.split('|'):
                if ':' in part:
                    item_type, item_value = part.split(':', 1)
                    items.append({'type': item_type, 'value': item_value})

            results.append({
                'auxiliary_pattern': item_str,
                'occurrence_count': len(indices),
                'indices': indices[:10],  # 只显示前10个索引
                'item_count': len(items),
                'items': items
            })

        return pd.DataFrame(results).sort_values('occurrence_count', ascending=False)


def main():
    """主函数：测试辅助项解析功能"""
    parser = AuxiliaryParser()

    # 测试用例
    test_cases = [
        "【客商：中国电信股份有限公司广州分公司】【款项名称：无】【绩效部门hl：公司本部】",
        "【银行账户：中国工商银行广州东城支行5746】",
        "【项目：广州至深圳高速公路扩建工程】【部门：工程部】",
        "【客商：ABC公司】【客商：XYZ公司】",  # 测试重复类型
        "",  # 空字符串
        "无效格式",  # 无效格式
        "【缺少右括号",  # 格式错误
        "【类型1：值1】【类型2：值2】【类型3：值3】",  # 多个项
    ]

    print("[测试] 辅助项解析测试")
    print("=" * 60)

    for i, test_case in enumerate(test_cases):
        print(f"\n测试用例 {i+1}: {test_case}")

        # 验证格式
        is_valid, errors = parser.validate_auxiliary_format(test_case)
        if not is_valid:
            print(f"  [失败] 格式无效: {errors}")
            continue

        # 解析辅助项
        items = parser.parse_auxiliary_info(test_case)

        if not items:
            print("  [警告]  无辅助项")
            continue

        print(f"  [成功] 解析到 {len(items)} 个辅助项:")
        for j, item in enumerate(items):
            print(f"    项 {j+1}: 原始类型='{item['raw_type']}', "
                  f"标准类型='{item['item_type']}', "
                  f"值='{item['item_value']}', "
                  f"显示类型='{item['display_type']}'")

    # 测试批量解析
    print("\n" + "=" * 60)
    print("[信息] 批量解析测试")

    import pandas as pd
    test_data = pd.Series(test_cases[:4], name='辅助项')
    df_parsed = parser.batch_parse_to_dataframe(test_data)

    print(f"批量解析结果 ({len(df_parsed)} 行):")
    print(df_parsed.to_string(index=False))

    # 测试统计摘要
    print("\n" + "=" * 60)
    print("[统计] 统计摘要测试")

    test_df = pd.DataFrame({'辅助项': test_cases[:4]})
    summary = parser.create_auxiliary_summary(test_df)

    print(f"总记录数: {summary['total_records']}")
    print(f"有辅助项的记录: {summary['records_with_auxiliary']}")
    print("\n辅助项类型统计:")
    for item_type, count in summary['item_type_counts'].items():
        unique_values = summary['unique_values_by_type'][item_type]
        print(f"  {item_type}: {count} 次出现, {len(unique_values)} 个唯一值")
        if len(unique_values) <= 5:  # 只显示前5个唯一值
            for value in unique_values[:5]:
                print(f"    - {value}")
        else:
            print(f"    - 示例: {unique_values[0]}, {unique_values[1]}, ...")

    # 测试特定类型提取
    print("\n" + "=" * 60)
    print("[提取] 特定类型提取测试")

    test_text = "【客商：中国电信股份有限公司广州分公司】【项目：网络升级】【部门：技术部】"
    target_types = ['supplier_customer', 'project']
    extracted = parser.extract_specific_items(test_text, target_types)

    print(f"原始文本: {test_text}")
    print(f"目标类型: {target_types}")
    print(f"提取结果: {extracted}")


if __name__ == "__main__":
    main()