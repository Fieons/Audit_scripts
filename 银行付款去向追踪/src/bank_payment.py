#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
银行付款去向跟踪脚本
功能：从序时账CSV文件中提取银付凭证，追踪资金流向，生成标准化的JSON格式输出
作者：Claude
日期：2025-11-10
"""

import csv
import json
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
import os
from ai_classifier import AIClassifier


class PaymentTracker:
    """银行付款去向跟踪器"""

    def __init__(self, csv_file_path: str, llm_api_key: Optional[str] = None, llm_provider: str = "deepseek"):
        """
        初始化跟踪器

        Args:
            csv_file_path: CSV文件路径
            llm_api_key: LLM API密钥（可选）
            llm_provider: LLM提供商，支持 "deepseek", "openai" 等
        """
        self.csv_file_path = csv_file_path
        self.raw_data = []
        self.bank_payments = []
        self.ai_classifier = AIClassifier(llm_api_key, llm_provider)

    def load_data(self) -> bool:
        """
        加载CSV数据

        Returns:
            bool: 加载是否成功
        """
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                self.raw_data = [row for row in reader if row['凭证号'].startswith('银付')]

            print(f"成功加载 {len(self.raw_data)} 条银付记录")
            return True

        except Exception as e:
            print(f"加载数据失败: {e}")
            return False

    def _clean_amount(self, amount_str: str) -> float:
        """
        清洗金额字符串，转换为浮点数

        Args:
            amount_str: 金额字符串

        Returns:
            float: 清洗后的金额
        """
        if not amount_str or amount_str.strip() == '':
            return 0.0

        # 去除逗号和引号
        cleaned = amount_str.replace(',', '').replace('"', '').strip()

        try:
            return float(cleaned)
        except ValueError:
            print(f"警告: 无法解析金额 '{amount_str}'")
            return 0.0

    
    def _extract_bank_account(self, auxiliary_item: str) -> str:
        """
        从辅助项中提取银行账户信息

        Args:
            auxiliary_item: 辅助项字符串

        Returns:
            str: 银行账户信息
        """
        if not auxiliary_item:
            return ""

        # 使用正则表达式提取银行账户信息
        match = re.search(r'【银行账户：([^】]+)】', auxiliary_item)
        if match:
            return match.group(1)

        return ""

    def _extract_department(self, auxiliary_item: str) -> str:
        """
        从辅助项中提取部门信息

        Args:
            auxiliary_item: 辅助项字符串

        Returns:
            str: 部门信息
        """
        if not auxiliary_item:
            return ""

        # 提取部门信息
        patterns = [
            r'【部门：([^】]+)】',
            r'【绩效部门：([^】]+)】',
        ]

        for pattern in patterns:
            match = re.search(pattern, auxiliary_item)
            if match:
                return match.group(1)

        return ""

    def _extract_customer(self, auxiliary_item: str) -> str:
        """
        从辅助项中提取客商信息

        Args:
            auxiliary_item: 辅助项字符串

        Returns:
            str: 客商信息
        """
        if not auxiliary_item:
            return ""

        match = re.search(r'【客商：([^】]+)】', auxiliary_item)
        if match:
            return match.group(1)

        return ""

    def _extract_person(self, auxiliary_item: str) -> str:
        """
        从辅助项中提取人员信息

        Args:
            auxiliary_item: 辅助项字符串

        Returns:
            str: 人员信息
        """
        if not auxiliary_item:
            return ""

        match = re.search(r'【人员档案：([^】]+)】', auxiliary_item)
        if match:
            return match.group(1)

        return ""

    def _process_voucher_group(self, voucher_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        处理单个凭证组的数据，支持所有类型的会计分录场景

        Args:
            voucher_records: 凭证记录列表

        Returns:
            List[Dict]: 处理后的付款记录列表
        """
        # 分离借方和贷方记录
        debit_records = []
        credit_records = []

        for record in voucher_records:
            debit_amount = self._clean_amount(record['借方'])
            credit_amount = self._clean_amount(record['贷方'])

            if debit_amount > 0:
                record['借方金额'] = debit_amount
                debit_records.append(record)
            elif credit_amount > 0:
                record['贷方金额'] = credit_amount
                credit_records.append(record)

        # 判断凭证类型并采用相应的处理策略
        voucher_type = self._determine_voucher_type(debit_records, credit_records)

        if voucher_type == "一贷一借":
            return self._process_one_credit_one_debit(debit_records, credit_records)
        elif voucher_type == "一贷多借":
            return self._process_one_credit_multi_debit(debit_records, credit_records)
        elif voucher_type == "多贷一借":
            return self._process_multi_credit_one_debit(debit_records, credit_records)
        elif voucher_type == "多贷多借":
            return self._process_multi_credit_multi_debit(debit_records, credit_records)
        else:
            raise ValueError(f"未知的凭证类型: {voucher_type}")

    def _determine_voucher_type(self, debit_records: List[Dict], credit_records: List[Dict]) -> str:
        """
        确定凭证类型

        Args:
            debit_records: 借方记录列表
            credit_records: 贷方记录列表

        Returns:
            str: 凭证类型
        """
        debit_count = len(debit_records)
        credit_count = len(credit_records)

        if debit_count == 1 and credit_count == 1:
            return "一贷一借"
        elif debit_count == 1 and credit_count > 1:
            return "多贷一借"
        elif debit_count > 1 and credit_count == 1:
            return "一贷多借"
        elif debit_count > 1 and credit_count > 1:
            return "多贷多借"
        else:
            return "未知"

    def _process_one_credit_one_debit(self, debit_records: List[Dict], credit_records: List[Dict]) -> List[Dict[str, Any]]:
        """处理一贷一借场景"""
        credit_record = credit_records[0]
        debit_record = debit_records[0]

        debit_entry = self._create_debit_entry(debit_record, debit_record['借方金额'])

        payment_record = {
            "付款ID": f"{credit_record['月']}月{credit_record['日']}日-{credit_record['凭证号']}-分录{credit_record['分录号']}",
            "原始凭证": f"{credit_record['月']}月{credit_record['日']}日-{credit_record['凭证号']}",
            "贷方": {
                "科目名称": credit_record['科目名称'],
                "金额": credit_record['贷方金额'],
                "摘要": credit_record['摘要'],
                "银行账户": self._extract_bank_account(credit_record['辅助项'])
            },
            "借方": [debit_entry],
            "凭证类型": "一贷一借",
            "校验信息": {
                "借方合计": debit_entry['金额'],
                "贷方合计": credit_record['贷方金额'],
                "平衡状态": "平衡" if abs(debit_entry['金额'] - credit_record['贷方金额']) < 0.01 else "不平衡"
            }
        }

        return [payment_record]

    def _process_one_credit_multi_debit(self, debit_records: List[Dict], credit_records: List[Dict]) -> List[Dict[str, Any]]:
        """处理一贷多借场景"""
        credit_record = credit_records[0]
        credit_amount = credit_record['贷方金额']

        # 将贷方金额按比例分配给各个借方
        allocated_debits = []
        remaining_credit = credit_amount

        for i, debit_record in enumerate(debit_records):
            debit_amount = debit_record['借方金额']

            # 最后一个借方记录，分配剩余所有金额
            if i == len(debit_records) - 1:
                allocated_amount = remaining_credit
            else:
                allocated_amount = min(debit_amount, remaining_credit)

            if allocated_amount > 0:
                debit_entry = self._create_debit_entry(debit_record, allocated_amount)
                allocated_debits.append(debit_entry)
                remaining_credit -= allocated_amount

        payment_record = {
            "付款ID": f"{credit_record['月']}月{credit_record['日']}日-{credit_record['凭证号']}-分录{credit_record['分录号']}",
            "原始凭证": f"{credit_record['月']}月{credit_record['日']}日-{credit_record['凭证号']}",
            "贷方": {
                "科目名称": credit_record['科目名称'],
                "金额": credit_amount,
                "摘要": credit_record['摘要'],
                "银行账户": self._extract_bank_account(credit_record['辅助项'])
            },
            "借方": allocated_debits,
            "凭证类型": "一贷多借",
            "校验信息": {
                "借方合计": sum(debit['金额'] for debit in allocated_debits),
                "贷方合计": credit_amount,
                "平衡状态": "平衡" if abs(sum(debit['金额'] for debit in allocated_debits) - credit_amount) < 0.01 else "不平衡"
            }
        }

        return [payment_record]

    def _process_multi_credit_one_debit(self, debit_records: List[Dict], credit_records: List[Dict]) -> List[Dict[str, Any]]:
        """处理多贷一借场景"""
        debit_record = debit_records[0]
        debit_amount = debit_record['借方金额']

        payment_records = []
        remaining_debit = debit_amount

        # 为每个贷方记录创建对应的付款记录
        for i, credit_record in enumerate(credit_records):
            credit_amount = credit_record['贷方金额']

            # 最后一个贷方记录，分配剩余所有金额
            if i == len(credit_records) - 1:
                allocated_amount = remaining_debit
            else:
                allocated_amount = min(credit_amount, remaining_debit)

            if allocated_amount > 0:
                # 按比例分配借方金额
                debit_entry = self._create_debit_entry(debit_record, allocated_amount)

                payment_record = {
                    "付款ID": f"{credit_record['月']}月{credit_record['日']}日-{credit_record['凭证号']}-分录{credit_record['分录号']}",
                    "原始凭证": f"{credit_record['月']}月{credit_record['日']}日-{credit_record['凭证号']}",
                    "贷方": {
                        "科目名称": credit_record['科目名称'],
                        "金额": allocated_amount,
                        "摘要": credit_record['摘要'],
                        "银行账户": self._extract_bank_account(credit_record['辅助项'])
                    },
                    "借方": [debit_entry],
                    "凭证类型": "多贷一借",
                    "分录序号": i + 1,
                    "总分录数": len(credit_records),
                    "校验信息": {
                        "借方合计": debit_entry['金额'],
                        "贷方合计": allocated_amount,
                        "平衡状态": "平衡" if abs(debit_entry['金额'] - allocated_amount) < 0.01 else "不平衡"
                    }
                }

                payment_records.append(payment_record)
                remaining_debit -= allocated_amount

        return payment_records

    def _process_multi_credit_multi_debit(self, debit_records: List[Dict], credit_records: List[Dict]) -> List[Dict[str, Any]]:
        """处理多贷多借场景 - 使用智能分配算法"""
        total_credit = sum(record['贷方金额'] for record in credit_records)
        total_debit = sum(record['借方金额'] for record in debit_records)

        # 验证平衡
        if abs(total_credit - total_debit) > 0.01:
            raise ValueError(f"凭证不平衡：贷方合计 {total_credit} != 借方合计 {total_debit}")

        payment_records = []
        remaining_debits = debit_records.copy()

        # 为每个贷方记录智能分配借方
        for credit_record in credit_records:
            credit_amount = credit_record['贷方金额']
            allocated_debits = []
            remaining_credit = credit_amount

            # 分配可用的借方记录
            temp_debits = []
            for i, debit_record in enumerate(remaining_debits):
                if remaining_credit <= 0:
                    break

                debit_amount = debit_record['借方金额']

                # 最后一个借方记录
                if i == len(remaining_debits) - 1:
                    allocated_amount = remaining_credit
                else:
                    allocated_amount = min(debit_amount, remaining_credit)

                if allocated_amount > 0:
                    debit_entry = self._create_debit_entry(debit_record, allocated_amount)
                    allocated_debits.append(debit_entry)

                    # 创建借方记录的副本并更新剩余金额
                    updated_debit = debit_record.copy()
                    updated_debit['借方金额'] = debit_amount - allocated_amount
                    if updated_debit['借方金额'] > 0.01:  # 还有余额，加入下一轮分配
                        temp_debits.append(updated_debit)

                remaining_credit -= allocated_amount

            # 更新剩余借方记录
            remaining_debits = temp_debits

            payment_record = {
                "付款ID": f"{credit_record['月']}月{credit_record['日']}日-{credit_record['凭证号']}-分录{credit_record['分录号']}",
                "原始凭证": f"{credit_record['月']}月{credit_record['日']}日-{credit_record['凭证号']}",
                "贷方": {
                    "科目名称": credit_record['科目名称'],
                    "金额": credit_amount,
                    "摘要": credit_record['摘要'],
                    "银行账户": self._extract_bank_account(credit_record['辅助项'])
                },
                "借方": allocated_debits,
                "凭证类型": "多贷多借",
                "分录序号": len(payment_records) + 1,
                "总分录数": len(credit_records),
                "校验信息": {
                    "借方合计": sum(debit['金额'] for debit in allocated_debits),
                    "贷方合计": credit_amount,
                    "平衡状态": "平衡" if abs(sum(debit['金额'] for debit in allocated_debits) - credit_amount) < 0.01 else "不平衡"
                }
            }

            payment_records.append(payment_record)

        return payment_records

    def _create_debit_entry(self, debit_record: Dict, amount: float) -> Dict[str, Any]:
        """
        创建借方记录条目

        Args:
            debit_record: 借方记录
            amount: 分配的金额

        Returns:
            Dict: 借方记录条目
        """
        return {
            "科目名称": debit_record['科目名称'],
            "金额": amount,
            "摘要": debit_record['摘要'],
            "部门": self._extract_department(debit_record['辅助项']),
            "客商": self._extract_customer(debit_record['辅助项']),
            "人员": self._extract_person(debit_record['辅助项']),
            "款项用途分类": self.ai_classifier.classify_payment_purpose(
                debit_record['摘要'],
                debit_record['科目名称'],
                debit_record['辅助项']
            ),
            "现金流量表项目分类": self.ai_classifier.classify_cash_flow_item(
                debit_record['摘要'],
                debit_record['科目名称'],
                debit_record['辅助项']
            )
        }

    def process_data(self) -> bool:
        """
        处理数据，生成付款记录

        Returns:
            bool: 处理是否成功
        """
        if not self.raw_data:
            print("错误: 没有数据可处理")
            return False

        # 按凭证分组
        voucher_groups = {}
        for record in self.raw_data:
            key = (record['月'], record['日'], record['凭证号'])
            if key not in voucher_groups:
                voucher_groups[key] = []
            voucher_groups[key].append(record)

        print(f"找到 {len(voucher_groups)} 个银付凭证组")

        # 处理每个凭证组
        all_payment_records = []
        error_count = 0

        for voucher_key, voucher_records in voucher_groups.items():
            try:
                payment_records = self._process_voucher_group(voucher_records)
                all_payment_records.extend(payment_records)
            except Exception as e:
                error_count += 1
                print(f"处理凭证组 {voucher_key} 时发生错误: {e}")
                continue

        self.bank_payments = all_payment_records
        print(f"生成 {len(all_payment_records)} 条付款记录")
        if error_count > 0:
            print(f"警告: 有 {error_count} 个凭证组处理失败")

        return len(all_payment_records) > 0

    
    def save_to_json(self, output_file: str) -> bool:
        """
        保存结果到JSON文件

        Args:
            output_file: 输出文件路径

        Returns:
            bool: 保存是否成功
        """
        try:
            result = {
                "生成时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "数据源": self.csv_file_path,
                "付款记录": self.bank_payments
            }

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            print(f"结果已保存到: {output_file}")
            return True

        except Exception as e:
            print(f"保存文件失败: {e}")
            return False


def main():
    """主函数"""
    # 文件路径
    csv_file = '../examples/序时账2025.1-9.csv'
    json_output = '../output/银行付款去向跟踪结果.json'

    # LLM API密钥配置
    llm_api_key = "e0a30d02c9d8482993e0170a598694ac.6vAH1ZGmkt7WZYHM"  # 可以设置为您的API密钥，或留空使用后备分类
    # llm_api_key = "your_deepseek_api_key_here"  # 例如：DeepSeek API密钥
    llm_provider = "deepseek"  # 支持 "deepseek", "openai" 等

    print("=== 银行付款去向跟踪处理程序 (支持LLM智能分类) ===")
    print(f"输入文件: {csv_file}")
    print(f"输出文件: {json_output}")
    print(f"LLM提供商: {llm_provider}")
    print(f"LLM分类: {'启用' if llm_api_key else '未配置API密钥，程序无法运行'}")
    print()

    # 检查API密钥配置
    if not llm_api_key:
        print("错误: 未配置LLM API密钥，无法进行智能分类")
        print("请在main函数中设置llm_api_key变量")
        return

    # 创建跟踪器实例
    tracker = PaymentTracker(csv_file, llm_api_key, llm_provider)

    # 加载数据
    if not tracker.load_data():
        return

    # 处理数据
    if not tracker.process_data():
        return

    # 保存结果
    if not tracker.save_to_json(json_output):
        return

    print(f"\n处理完成! 生成了 {len(tracker.bank_payments)} 条付款记录")
    print(f"如需统计分析，请运行: python payment_statistics.py")


if __name__ == "__main__":
    main()

