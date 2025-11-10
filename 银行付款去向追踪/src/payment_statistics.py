#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
银行付款统计模块
功能：对银行付款数据进行统计分析
作者：Claude
日期：2025-11-10
"""

import json
from typing import Dict, List, Any
from datetime import datetime


class PaymentStatistics:
    """银行付款统计分析器"""

    def __init__(self, payment_data_file: str):
        """
        初始化统计分析器

        Args:
            payment_data_file: 付款数据JSON文件路径
        """
        self.payment_data_file = payment_data_file
        self.payment_data = None
        self.statistics = {}

    def load_payment_data(self) -> bool:
        """
        加载付款数据

        Returns:
            bool: 加载是否成功
        """
        try:
            with open(self.payment_data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.payment_data = data.get('付款记录', [])
                print(f"成功加载 {len(self.payment_data)} 条付款记录")
                return True
        except Exception as e:
            print(f"加载付款数据失败: {e}")
            return False

    def generate_basic_statistics(self) -> Dict[str, Any]:
        """
        生成基础统计信息

        Returns:
            Dict: 基础统计信息
        """
        if not self.payment_data:
            return {}

        stats = {
            "总付款记录数": len(self.payment_data),
            "总付款金额": sum(payment['贷方']['金额'] for payment in self.payment_data),
            "按用途分类统计": {},
            "按现金流量分类统计": {},
            "按付款账户统计": {},
            "按月份统计": {},
            "按凭证类型统计": {}
        }

        # 按用途分类统计
        for payment in self.payment_data:
            for debit in payment['借方']:
                category = debit['款项用途分类']
                amount = debit['金额']
                if category not in stats["按用途分类统计"]:
                    stats["按用途分类统计"][category] = {"记录数": 0, "金额": 0.0}
                stats["按用途分类统计"][category]["记录数"] += 1
                stats["按用途分类统计"][category]["金额"] += amount

        # 按现金流量分类统计
        for payment in self.payment_data:
            for debit in payment['借方']:
                category = debit['现金流量表项目分类']
                amount = debit['金额']
                if category not in stats["按现金流量分类统计"]:
                    stats["按现金流量分类统计"][category] = {"记录数": 0, "金额": 0.0}
                stats["按现金流量分类统计"][category]["记录数"] += 1
                stats["按现金流量分类统计"][category]["金额"] += amount

        # 按付款账户统计
        for payment in self.payment_data:
            account = payment['贷方']['银行账户'] or payment['贷方']['科目名称']
            amount = payment['贷方']['金额']
            if account not in stats["按付款账户统计"]:
                stats["按付款账户统计"][account] = {"记录数": 0, "金额": 0.0}
            stats["按付款账户统计"][account]["记录数"] += 1
            stats["按付款账户统计"][account]["金额"] += amount

        # 按月份统计
        for payment in self.payment_data:
            month = payment['付款ID'].split('月')[0] + '月'
            amount = payment['贷方']['金额']
            if month not in stats["按月份统计"]:
                stats["按月份统计"][month] = {"记录数": 0, "金额": 0.0}
            stats["按月份统计"][month]["记录数"] += 1
            stats["按月份统计"][month]["金额"] += amount

        # 按凭证类型统计
        for payment in self.payment_data:
            voucher_type = payment.get('凭证类型', '未知')
            amount = payment['贷方']['金额']
            if voucher_type not in stats["按凭证类型统计"]:
                stats["按凭证类型统计"][voucher_type] = {"记录数": 0, "金额": 0.0}
            stats["按凭证类型统计"][voucher_type]["记录数"] += 1
            stats["按凭证类型统计"][voucher_type]["金额"] += amount

        self.statistics = stats
        return stats

    def generate_detailed_analysis(self) -> Dict[str, Any]:
        """
        生成详细分析报告

        Returns:
            Dict: 详细分析报告
        """
        if not self.statistics:
            self.generate_basic_statistics()

        analysis = {
            "生成时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "数据源": self.payment_data_file,
            "基础统计": self.statistics,
            "详细分析": {}
        }

        # 用途分类占比分析
        purpose_stats = self.statistics["按用途分类统计"]
        total_amount = self.statistics["总付款金额"]
        purpose_analysis = []
        for category, data in purpose_stats.items():
            purpose_analysis.append({
                "类别": category,
                "记录数": data["记录数"],
                "金额": data["金额"],
                "金额占比": round(data["金额"] / total_amount * 100, 2) if total_amount > 0 else 0,
                "平均金额": round(data["金额"] / data["记录数"], 2) if data["记录数"] > 0 else 0
            })
        # 按金额排序
        purpose_analysis.sort(key=lambda x: x["金额"], reverse=True)
        analysis["详细分析"]["用途分类分析"] = purpose_analysis

        # 现金流量分类占比分析
        cashflow_stats = self.statistics["按现金流量分类统计"]
        cashflow_analysis = []
        for category, data in cashflow_stats.items():
            cashflow_analysis.append({
                "类别": category,
                "记录数": data["记录数"],
                "金额": data["金额"],
                "金额占比": round(data["金额"] / total_amount * 100, 2) if total_amount > 0 else 0,
                "平均金额": round(data["金额"] / data["记录数"], 2) if data["记录数"] > 0 else 0
            })
        # 按金额排序
        cashflow_analysis.sort(key=lambda x: x["金额"], reverse=True)
        analysis["详细分析"]["现金流量分类分析"] = cashflow_analysis

        # 月度趋势分析
        monthly_stats = self.statistics["按月份统计"]
        monthly_analysis = []
        for month in sorted(monthly_stats.keys()):
            data = monthly_stats[month]
            monthly_analysis.append({
                "月份": month,
                "记录数": data["记录数"],
                "金额": data["金额"],
                "平均金额": round(data["金额"] / data["记录数"], 2) if data["记录数"] > 0 else 0
            })
        analysis["详细分析"]["月度趋势分析"] = monthly_analysis

        # 账户使用分析
        account_stats = self.statistics["按付款账户统计"]
        account_analysis = []
        for account, data in account_stats.items():
            account_analysis.append({
                "账户": account,
                "记录数": data["记录数"],
                "金额": data["金额"],
                "金额占比": round(data["金额"] / total_amount * 100, 2) if total_amount > 0 else 0,
                "平均金额": round(data["金额"] / data["记录数"], 2) if data["记录数"] > 0 else 0
            })
        # 按金额排序
        account_analysis.sort(key=lambda x: x["金额"], reverse=True)
        analysis["详细分析"]["账户使用分析"] = account_analysis

        # 凭证类型分析
        voucher_stats = self.statistics["按凭证类型统计"]
        voucher_analysis = []
        for voucher_type, data in voucher_stats.items():
            voucher_analysis.append({
                "凭证类型": voucher_type,
                "记录数": data["记录数"],
                "金额": data["金额"],
                "金额占比": round(data["金额"] / total_amount * 100, 2) if total_amount > 0 else 0,
                "平均金额": round(data["金额"] / data["记录数"], 2) if data["记录数"] > 0 else 0
            })
        # 按金额排序
        voucher_analysis.sort(key=lambda x: x["金额"], reverse=True)
        analysis["详细分析"]["凭证类型分析"] = voucher_analysis

        return analysis

    def print_basic_statistics(self):
        """打印基础统计信息"""
        if not self.statistics:
            self.generate_basic_statistics()

        stats = self.statistics
        print("\n=== 基础统计信息 ===")
        print(f"总付款记录数: {stats['总付款记录数']}")
        print(f"总付款金额: {stats['总付款金额']:,.2f} 元")

        print("\n按用途分类统计:")
        for category, data in sorted(stats['按用途分类统计'].items(), key=lambda x: x[1]['金额'], reverse=True):
            print(f"  {category}: {data['记录数']} 笔, {data['金额']:,.2f} 元")

        print("\n按现金流量分类统计:")
        for category, data in sorted(stats['按现金流量分类统计'].items(), key=lambda x: x[1]['金额'], reverse=True):
            print(f"  {category}: {data['记录数']} 笔, {data['金额']:,.2f} 元")

        print("\n按付款账户统计:")
        for account, data in sorted(stats['按付款账户统计'].items(), key=lambda x: x[1]['金额'], reverse=True):
            print(f"  {account}: {data['记录数']} 笔, {data['金额']:,.2f} 元")

        print("\n按月份统计:")
        for month, data in sorted(stats['按月份统计'].items()):
            print(f"  {month}: {data['记录数']} 笔, {data['金额']:,.2f} 元")

        print("\n按凭证类型统计:")
        for voucher_type, data in sorted(stats['按凭证类型统计'].items(), key=lambda x: x[1]['金额'], reverse=True):
            print(f"  {voucher_type}: {data['记录数']} 笔, {data['金额']:,.2f} 元")

    def print_detailed_analysis(self):
        """打印详细分析报告"""
        analysis = self.generate_detailed_analysis()

        print("\n=== 详细分析报告 ===")
        print(f"生成时间: {analysis['生成时间']}")
        print(f"数据源: {analysis['数据源']}")

        print("\n用途分类分析 (按金额排序):")
        for item in analysis["详细分析"]["用途分类分析"]:
            print(f"  {item['类别']}: {item['记录数']} 笔, {item['金额']:,.2f} 元 ({item['金额占比']}%)")

        print("\n现金流量分类分析 (按金额排序):")
        for item in analysis["详细分析"]["现金流量分类分析"]:
            print(f"  {item['类别']}: {item['记录数']} 笔, {item['金额']:,.2f} 元 ({item['金额占比']}%)")

        print("\n月度趋势分析:")
        for item in analysis["详细分析"]["月度趋势分析"]:
            print(f"  {item['月份']}: {item['记录数']} 笔, {item['金额']:,.2f} 元 (平均 {item['平均金额']:,.2f} 元)")

        print("\n账户使用分析 (按金额排序):")
        for item in analysis["详细分析"]["账户使用分析"]:
            print(f"  {item['账户']}: {item['记录数']} 笔, {item['金额']:,.2f} 元 ({item['金额占比']}%)")

        print("\n凭证类型分析:")
        for item in analysis["详细分析"]["凭证类型分析"]:
            print(f"  {item['凭证类型']}: {item['记录数']} 笔, {item['金额']:,.2f} 元 ({item['金额占比']}%)")

    def save_statistics(self, output_file: str) -> bool:
        """
        保存统计结果到JSON文件

        Args:
            output_file: 输出文件路径

        Returns:
            bool: 保存是否成功
        """
        try:
            analysis = self.generate_detailed_analysis()
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(analysis, f, ensure_ascii=False, indent=2)
            print(f"统计结果已保存到: {output_file}")
            return True
        except Exception as e:
            print(f"保存统计结果失败: {e}")
            return False


def main():
    """统计模块的主函数"""
    # 默认的付款数据文件
    payment_data_file = '../output/银行付款去向跟踪结果.json'
    statistics_output = '../output/银行付款统计分析结果.json'

    print("=== 银行付款统计分析程序 ===")
    print(f"输入文件: {payment_data_file}")
    print(f"输出文件: {statistics_output}")
    print()

    # 创建统计分析器
    analyzer = PaymentStatistics(payment_data_file)

    # 加载付款数据
    if not analyzer.load_payment_data():
        return

    # 生成并打印基础统计
    analyzer.print_basic_statistics()

    # 生成并打印详细分析
    analyzer.print_detailed_analysis()

    # 保存统计结果
    analyzer.save_statistics(statistics_output)

    print("\n统计分析完成!")


if __name__ == "__main__":
    main()