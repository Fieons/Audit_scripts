"""
数据一致性检验脚本
验证CSV源数据与数据库转换后数据的一致性
只允许计算机浮点数精度误差，不允许货币层面的误差
"""

import os
import sys
import sqlite3
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from decimal import Decimal, getcontext
import traceback

# 设置Decimal精度
getcontext().prec = 28  # 高精度计算

# 导入自定义模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data_cleaner import DataCleaner
from auxiliary_parser import AuxiliaryParser


class DataConsistencyChecker:
    """数据一致性检验器"""

    def __init__(self, db_path: str = "./database/accounting.db"):
        """
        初始化检验器

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.data_cleaner = DataCleaner()
        self.auxiliary_parser = AuxiliaryParser()

        # 精度阈值：只允许计算机浮点数精度误差
        self.precision_threshold = Decimal('0.00000001')

    def check_file_consistency(self, csv_path: str, year: int) -> Dict[str, Any]:
        """
        检查单个CSV文件与数据库的一致性

        Args:
            csv_path: CSV文件路径
            year: 数据年份

        Returns:
            一致性检查结果
        """
        print(f"\n[检查] 检查文件一致性: {csv_path}")
        print(f"[年份] 数据年份: {year}")

        try:
            # 1. 读取和清洗CSV数据
            df_csv = self.data_cleaner.read_csv(csv_path)
            df_cleaned = self.data_cleaner.clean_dataframe(df_csv, year)

            # 2. 从数据库读取对应数据
            df_db = self._load_data_from_db(csv_path, year)

            if df_db is None or len(df_db) == 0:
                return {
                    'file_path': csv_path,
                    'status': 'failed',
                    'error': '数据库中未找到对应数据',
                    'all_passed': False
                }

            # 3. 执行各项一致性检查
            checks = [
                ("记录数量检查", self._check_record_count),
                ("金额一致性检查", self._check_amount_consistency),
                ("凭证信息一致性检查", self._check_voucher_consistency),
                ("科目信息一致性检查", self._check_subject_consistency),
                ("辅助项完整性检查", self._check_auxiliary_integrity),
                ("借贷平衡检查", self._check_debit_credit_balance)
            ]

            results = {}
            all_passed = True

            for check_name, check_func in checks:
                print(f"\n[检查] {check_name}:")
                try:
                    passed, details = check_func(df_cleaned, df_db)
                    if passed:
                        print("  [成功] 通过")
                    else:
                        print(f"  [失败] 失败: {details}")
                        all_passed = False

                    results[check_name] = {
                        'passed': passed,
                        'details': details
                    }

                except Exception as e:
                    print(f"  [失败] 检查出错: {e}")
                    traceback.print_exc()
                    all_passed = False
                    results[check_name] = {
                        'passed': False,
                        'error': str(e)
                    }

            # 4. 汇总结果
            result = {
                'file_path': csv_path,
                'year': year,
                'csv_rows': len(df_cleaned),
                'db_rows': len(df_db),
                'all_passed': all_passed,
                'check_results': results,
                'status': 'success' if all_passed else 'failed'
            }

            if all_passed:
                print(f"\n[成功] 文件一致性检查全部通过: {csv_path}")
            else:
                print(f"\n[失败] 文件一致性检查失败: {csv_path}")

            return result

        except Exception as e:
            print(f"[失败] 一致性检查失败: {csv_path}")
            print(f"错误: {e}")
            traceback.print_exc()

            return {
                'file_path': csv_path,
                'year': year,
                'status': 'failed',
                'error': str(e),
                'all_passed': False
            }

    def _load_data_from_db(self, csv_path: str, year: int) -> Optional[pd.DataFrame]:
        """
        从数据库加载与CSV文件对应的数据

        Args:
            csv_path: CSV文件路径
            year: 数据年份

        Returns:
            数据库数据DataFrame
        """
        try:
            # 从CSV文件读取第一行获取实际的核算账簿名称
            df_sample = pd.read_csv(csv_path, nrows=1, encoding='utf-8-sig')

            if '核算账簿名称' not in df_sample.columns:
                print(f"[失败] CSV文件缺少'核算账簿名称'字段")
                return None

            book_name = df_sample.iloc[0]['核算账簿名称']
            print(f"[信息] CSV文件核算账簿名称: {book_name}")

            # 使用data_cleaner的extract_company_info方法提取公司名称
            # 确保与转换时使用相同的逻辑
            company_info = self.data_cleaner.extract_company_info(book_name)
            company_name = company_info['company_name']
            print(f"[信息] 提取的公司名称: {company_name}")

            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row

            # 查询数据库中的数据 - 使用精确匹配
            query = """
            SELECT
                v.year, v.month, v.day,
                v.voucher_number, v.voucher_type,
                vd.entry_number, vd.summary,
                s.code as subject_code, s.name as subject_name,
                vd.currency, vd.debit_amount, vd.credit_amount,
                vd.auxiliary_info, vd.write_off_info, vd.settlement_info,
                c.name as company_name, ab.name as book_name
            FROM voucher_details vd
            JOIN vouchers v ON vd.voucher_id = v.id
            JOIN account_books ab ON v.book_id = ab.id
            JOIN companies c ON ab.company_id = c.id
            JOIN account_subjects s ON vd.subject_id = s.id
            WHERE v.year = ? AND c.name = ?
            ORDER BY v.voucher_date, v.voucher_number, vd.entry_number
            """

            df = pd.read_sql_query(query, conn, params=(year, company_name))

            # 如果精确匹配没有找到数据，检查是否存在相似的公司名称
            if len(df) == 0:
                print(f"[警告] 精确匹配未找到数据: {company_name}")

                # 首先检查数据库中是否存在相似的公司名称
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT name FROM companies WHERE name LIKE ?", (f"%{company_name}%",))
                similar_companies = [row[0] for row in cursor.fetchall()]

                if similar_companies:
                    print(f"[警告] 发现相似的公司名称，可能导致数据混淆:")
                    for similar_company in similar_companies:
                        print(f"  - {similar_company}")

                    # 尝试使用最相似的公司名称进行匹配
                    # 优先选择完全包含当前公司名称的
                    matching_companies = [c for c in similar_companies if company_name in c]
                    if matching_companies:
                        best_match = matching_companies[0]
                        print(f"[信息] 使用最相似的公司名称进行匹配: {best_match}")
                        df = pd.read_sql_query(query, conn, params=(year, best_match))

                        if len(df) > 0:
                            print(f"[警告] 使用相似公司名称匹配找到 {len(df)} 条记录")
                            print(f"[警告] 这可能表示数据分组存在问题，请检查转换逻辑")
                else:
                    print(f"[警告] 数据库中未找到相似的公司名称")

            conn.close()

            if len(df) == 0:
                print(f"[警告] 数据库中未找到 {company_name} 公司 {year} 年的数据")
                # 显示数据库中存在的公司和年份组合
                self._show_available_data(year)
                return None

            print(f"[成功] 从数据库加载 {len(df)} 条记录")

            # 显示数据统计信息
            unique_companies = df['company_name'].unique()
            unique_books = df['book_name'].unique()
            print(f"[信息] 匹配到公司: {', '.join(unique_companies)}")
            print(f"[信息] 匹配到账簿: {', '.join(unique_books[:3])}" +
                  (f" 等 {len(unique_books)} 个" if len(unique_books) > 3 else ""))

            return df

        except Exception as e:
            print(f"[失败] 从数据库加载数据失败: {e}")
            traceback.print_exc()
            return None

    def _show_available_data(self, year: int = None):
        """显示数据库中可用的数据"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 查询所有公司和年份组合
            if year:
                query = """
                SELECT DISTINCT c.name as company_name, v.year, COUNT(*) as record_count
                FROM voucher_details vd
                JOIN vouchers v ON vd.voucher_id = v.id
                JOIN account_books ab ON v.book_id = ab.id
                JOIN companies c ON ab.company_id = c.id
                WHERE v.year = ?
                GROUP BY c.name, v.year
                ORDER BY c.name, v.year
                """
                cursor.execute(query, (year,))
            else:
                query = """
                SELECT DISTINCT c.name as company_name, v.year, COUNT(*) as record_count
                FROM voucher_details vd
                JOIN vouchers v ON vd.voucher_id = v.id
                JOIN account_books ab ON v.book_id = ab.id
                JOIN companies c ON ab.company_id = c.id
                GROUP BY c.name, v.year
                ORDER BY c.name, v.year
                """
                cursor.execute(query)

            results = cursor.fetchall()
            conn.close()

            if results:
                print(f"[信息] 数据库中可用的数据:")
                for company, data_year, count in results:
                    print(f"  公司: {company}, 年份: {data_year}, 记录数: {count:,}")
            else:
                print("[信息] 数据库中没有数据")

        except Exception as e:
            print(f"[警告] 无法获取数据库可用数据: {e}")

    def validate_data_grouping(self):
        """
        验证数据分组是否正确
        检查同一公司的不同账簿数据是否被正确分离
        """
        print("\n[验证] 数据分组验证")
        print("=" * 60)

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 查询每个公司的账簿分布
            query = """
            SELECT
                c.name as company_name,
                ab.name as book_name,
                COUNT(*) as record_count,
                MIN(v.voucher_date) as first_date,
                MAX(v.voucher_date) as last_date,
                COUNT(DISTINCT v.year) as year_count
            FROM voucher_details vd
            JOIN vouchers v ON vd.voucher_id = v.id
            JOIN account_books ab ON v.book_id = ab.id
            JOIN companies c ON ab.company_id = c.id
            GROUP BY c.name, ab.name
            ORDER BY c.name, record_count DESC
            """

            cursor.execute(query)
            results = cursor.fetchall()
            conn.close()

            if not results:
                print("[信息] 数据库中没有数据")
                return True

            print("[信息] 数据分组统计:")
            current_company = None
            all_valid = True

            for company, book, count, first_date, last_date, year_count in results:
                if company != current_company:
                    print(f"\n公司: {company}")
                    current_company = company

                print(f"  账簿: {book}")
                print(f"    记录数: {count:,}")
                print(f"    时间范围: {first_date} 至 {last_date}")
                print(f"    包含年份数: {year_count}")

                # 检查潜在问题
                issues = []

                # 检查年份混合（同一账簿包含多个年份是正常的）
                if year_count > 1:
                    issues.append(f"包含 {year_count} 个年份的数据")

                # 检查账簿名称是否包含公司名称（应该是"公司名-账簿类型"格式）
                if company not in book:
                    issues.append(f"账簿名称不包含公司名: '{book}'")

                if issues:
                    print(f"    [警告] 潜在问题: {'; '.join(issues)}")
                    all_valid = False

            # 检查公司名称相似性（可能的数据混淆）
            print("\n[检查] 公司名称相似性检查:")
            companies = set([r[0] for r in results])
            similar_companies = []

            for company1 in companies:
                for company2 in companies:
                    if company1 != company2 and company1 in company2:
                        similar_companies.append((company1, company2))

            if similar_companies:
                print(f"[警告] 发现相似的公司名称，可能导致数据混淆:")
                for comp1, comp2 in similar_companies:
                    print(f"  '{comp1}' 包含在 '{comp2}' 中")
                all_valid = False
            else:
                print("  [成功] 公司名称无相似性冲突")

            if all_valid:
                print("\n[成功] 数据分组验证通过")
            else:
                print("\n[警告] 数据分组存在潜在问题")

            return all_valid

        except Exception as e:
            print(f"[失败] 数据分组验证失败: {e}")
            traceback.print_exc()
            return False

    def _extract_company_from_filename(self, filename: str) -> str:
        """从文件名提取公司名称（已弃用，保留兼容性）"""
        print(f"[警告] _extract_company_from_filename 已弃用，请使用CSV文件中的核算账簿名称")
        # 这个方法已弃用，只用于兼容性
        # 实际应该使用CSV文件中的核算账簿名称提取公司信息
        return "未知"

    def _check_record_count(self, df_csv: pd.DataFrame, df_db: pd.DataFrame) -> Tuple[bool, str]:
        """检查记录数量一致性"""
        csv_count = len(df_csv)
        db_count = len(df_db)

        if csv_count == db_count:
            return True, f"记录数量一致: CSV={csv_count}, DB={db_count}"
        else:
            return False, f"记录数量不一致: CSV={csv_count}, DB={db_count}, 差异={csv_count - db_count}"

    def _check_amount_consistency(self, df_csv: pd.DataFrame, df_db: pd.DataFrame) -> Tuple[bool, str]:
        """检查金额一致性（只允许计算机精度误差）"""
        errors = []

        # 检查总借方金额
        csv_total_debit = Decimal(str(df_csv['借方-本币'].sum()))
        db_total_debit = Decimal(str(df_db['debit_amount'].sum()))

        debit_diff = abs(csv_total_debit - db_total_debit)
        if debit_diff > self.precision_threshold:
            errors.append(f"总借方金额不一致: CSV={csv_total_debit:.2f}, DB={db_total_debit:.2f}, 差异={debit_diff:.8f}")

        # 检查总贷方金额
        csv_total_credit = Decimal(str(df_csv['贷方-本币'].sum()))
        db_total_credit = Decimal(str(df_db['credit_amount'].sum()))

        credit_diff = abs(csv_total_credit - db_total_credit)
        if credit_diff > self.precision_threshold:
            errors.append(f"总贷方金额不一致: CSV={csv_total_credit:.2f}, DB={db_total_credit:.2f}, 差异={credit_diff:.8f}")

        # 检查逐条记录的金额
        amount_mismatches = []
        for idx, csv_row in df_csv.iterrows():
            if idx >= len(df_db):
                break

            db_row = df_db.iloc[idx]

            # 检查借方金额
            csv_debit = Decimal(str(csv_row['借方-本币']))
            db_debit = Decimal(str(db_row['debit_amount']))
            debit_diff = abs(csv_debit - db_debit)

            # 检查贷方金额
            csv_credit = Decimal(str(csv_row['贷方-本币']))
            db_credit = Decimal(str(db_row['credit_amount']))
            credit_diff = abs(csv_credit - db_credit)

            if debit_diff > self.precision_threshold or credit_diff > self.precision_threshold:
                amount_mismatches.append({
                    'index': idx,
                    'voucher': csv_row.get('凭证号', '未知'),
                    'entry': csv_row.get('分录号', 1),
                    'csv_debit': float(csv_debit),
                    'db_debit': float(db_debit),
                    'debit_diff': float(debit_diff),
                    'csv_credit': float(csv_credit),
                    'db_credit': float(db_credit),
                    'credit_diff': float(credit_diff)
                })

        if amount_mismatches:
            errors.append(f"发现 {len(amount_mismatches)} 条金额不一致的记录")
            # 显示前3条不一致的记录
            for i, mismatch in enumerate(amount_mismatches[:3]):
                errors.append(f"  第{i+1}条: 凭证{mismatch['voucher']}-分录{mismatch['entry']}, "
                            f"借方差异={mismatch['debit_diff']:.8f}, "
                            f"贷方差异={mismatch['credit_diff']:.8f}")
            if len(amount_mismatches) > 3:
                errors.append(f"  ... 还有 {len(amount_mismatches) - 3} 条")

        if errors:
            return False, "; ".join(errors)
        return True, "金额一致性检查通过"

    def _check_voucher_consistency(self, df_csv: pd.DataFrame, df_db: pd.DataFrame) -> Tuple[bool, str]:
        """检查凭证信息一致性"""
        errors = []

        # 检查凭证数量
        csv_vouchers = df_csv['凭证号'].nunique()
        db_vouchers = df_db['voucher_number'].nunique()

        if csv_vouchers != db_vouchers:
            errors.append(f"凭证数量不一致: CSV={csv_vouchers}, DB={db_vouchers}")

        # 检查凭证类型分布
        csv_voucher_types = df_csv['凭证类型'].value_counts().to_dict()
        db_voucher_types = df_db['voucher_type'].value_counts().to_dict()

        for vtype in set(list(csv_voucher_types.keys()) + list(db_voucher_types.keys())):
            csv_count = csv_voucher_types.get(vtype, 0)
            db_count = db_voucher_types.get(vtype, 0)
            if csv_count != db_count:
                errors.append(f"凭证类型'{vtype}'数量不一致: CSV={csv_count}, DB={db_count}")

        if errors:
            return False, "; ".join(errors)
        return True, "凭证信息一致性检查通过"

    def _check_subject_consistency(self, df_csv: pd.DataFrame, df_db: pd.DataFrame) -> Tuple[bool, str]:
        """检查科目信息一致性"""
        errors = []

        # 检查科目编码一致性
        csv_subjects = set(df_csv['科目编码'].dropna().unique())
        db_subjects = set(df_db['subject_code'].dropna().unique())

        missing_in_db = csv_subjects - db_subjects
        extra_in_db = db_subjects - csv_subjects

        if missing_in_db:
            errors.append(f"数据库缺少科目编码: {sorted(missing_in_db)[:10]}")
            if len(missing_in_db) > 10:
                errors.append(f"  ... 还有 {len(missing_in_db) - 10} 个")

        if extra_in_db:
            errors.append(f"数据库有多余科目编码: {sorted(extra_in_db)[:10]}")
            if len(extra_in_db) > 10:
                errors.append(f"  ... 还有 {len(extra_in_db) - 10} 个")

        # 检查科目使用次数
        csv_subject_counts = df_csv['科目编码'].value_counts().to_dict()
        db_subject_counts = df_db['subject_code'].value_counts().to_dict()

        mismatched_counts = []
        for subject in set(list(csv_subject_counts.keys()) + list(db_subject_counts.keys())):
            csv_count = csv_subject_counts.get(subject, 0)
            db_count = db_subject_counts.get(subject, 0)
            if csv_count != db_count:
                mismatched_counts.append((subject, csv_count, db_count))

        if mismatched_counts:
            errors.append(f"发现 {len(mismatched_counts)} 个科目使用次数不一致")
            for subject, csv_count, db_count in mismatched_counts[:5]:
                errors.append(f"  科目{subject}: CSV={csv_count}, DB={db_count}")
            if len(mismatched_counts) > 5:
                errors.append(f"  ... 还有 {len(mismatched_counts) - 5} 个")

        if errors:
            return False, "; ".join(errors)
        return True, "科目信息一致性检查通过"

    def _check_auxiliary_integrity(self, df_csv: pd.DataFrame, df_db: pd.DataFrame) -> Tuple[bool, str]:
        """检查辅助项完整性"""
        errors = []

        # 检查有辅助项的记录数量
        csv_with_aux = len(df_csv[df_csv['辅助项'].notna() & (df_csv['辅助项'] != '')])
        db_with_aux = len(df_db[df_db['auxiliary_info'].notna() & (df_db['auxiliary_info'] != '')])

        if csv_with_aux != db_with_aux:
            errors.append(f"有辅助项的记录数量不一致: CSV={csv_with_aux}, DB={db_with_aux}")

        # 抽样检查辅助项内容
        sample_size = min(10, len(df_csv))
        if sample_size > 0:
            sample_indices = np.random.choice(len(df_csv), sample_size, replace=False)

            for idx in sample_indices:
                if idx < len(df_db):
                    csv_aux = str(df_csv.iloc[idx]['辅助项']) if pd.notna(df_csv.iloc[idx]['辅助项']) else ''
                    db_aux = str(df_db.iloc[idx]['auxiliary_info']) if pd.notna(df_db.iloc[idx]['auxiliary_info']) else ''

                    if csv_aux != db_aux:
                        errors.append(f"记录{idx}辅助项不一致: CSV='{csv_aux[:50]}...', DB='{db_aux[:50]}...'")
                        break  # 发现一个不一致就停止

        if errors:
            return False, "; ".join(errors)
        return True, "辅助项完整性检查通过"

    def _check_debit_credit_balance(self, df_csv: pd.DataFrame, df_db: pd.DataFrame) -> Tuple[bool, str]:
        """检查借贷平衡（在CSV和DB中都应该平衡）"""
        errors = []

        # 检查CSV数据的借贷平衡
        csv_total_debit = Decimal(str(df_csv['借方-本币'].sum()))
        csv_total_credit = Decimal(str(df_csv['贷方-本币'].sum()))
        csv_diff = abs(csv_total_debit - csv_total_credit)

        if csv_diff > self.precision_threshold:
            errors.append(f"CSV数据借贷不平衡: 借方={csv_total_debit:.2f}, 贷方={csv_total_credit:.2f}, 差异={csv_diff:.8f}")

        # 检查DB数据的借贷平衡
        db_total_debit = Decimal(str(df_db['debit_amount'].sum()))
        db_total_credit = Decimal(str(df_db['credit_amount'].sum()))
        db_diff = abs(db_total_debit - db_total_credit)

        if db_diff > self.precision_threshold:
            errors.append(f"DB数据借贷不平衡: 借方={db_total_debit:.2f}, 贷方={db_total_credit:.2f}, 差异={db_diff:.8f}")

        if errors:
            return False, "; ".join(errors)
        return True, "借贷平衡检查通过"

    def check_all_files(self, data_dir: str = "../data") -> List[Dict[str, Any]]:
        """
        检查所有CSV文件的一致性

        Args:
            data_dir: 数据目录路径

        Returns:
            所有文件的检查结果
        """
        print("[开始] 开始批量检查所有CSV文件一致性")
        print("=" * 60)

        # 查找所有CSV文件
        csv_files = []
        for filename in os.listdir(data_dir):
            if filename.endswith('.csv'):
                file_path = os.path.join(data_dir, filename)
                csv_files.append(file_path)

        if not csv_files:
            print(f"[失败] 在目录 {data_dir} 中未找到CSV文件")
            return []

        print(f"找到 {len(csv_files)} 个CSV文件:")
        for file_path in csv_files:
            print(f"  - {os.path.basename(file_path)}")

        # 检查每个文件
        results = []
        all_passed = True

        for file_path in csv_files:
            # 从文件名中提取年份
            year = self._extract_year_from_filename(file_path)
            result = self.check_file_consistency(file_path, year)
            results.append(result)

            if not result.get('all_passed', False):
                all_passed = False

        # 生成汇总报告
        self._generate_summary_report(results)

        return results

    def _extract_year_from_filename(self, filename: str) -> int:
        """从文件名中提取年份"""
        import re
        # 查找4位数字年份
        match = re.search(r'(\d{4})年', filename)
        if match:
            return int(match.group(1))

        # 如果没有找到，尝试其他模式
        match = re.search(r'20(\d{2})', filename)
        if match:
            return int(f"20{match.group(1)}")

        # 默认返回当前年份
        from datetime import datetime
        return datetime.now().year

    def _generate_summary_report(self, results: List[Dict[str, Any]]):
        """生成汇总报告"""
        print("\n" + "=" * 60)
        print("[信息] 一致性检查汇总报告")
        print("=" * 60)

        successful = [r for r in results if r.get('all_passed', False)]
        failed = [r for r in results if not r.get('all_passed', False)]

        print(f"[成功] 一致性检查通过: {len(successful)} 个文件")
        print(f"[失败] 一致性检查失败: {len(failed)} 个文件")

        if successful:
            print(f"\n[通过] 通过的文件:")
            for result in successful:
                print(f"  - {os.path.basename(result['file_path'])} "
                      f"(CSV:{result['csv_rows']}, DB:{result['db_rows']})")

        if failed:
            print(f"\n[失败] 失败的文件:")
            for result in failed:
                print(f"  - {os.path.basename(result['file_path'])}")
                if 'error' in result:
                    print(f"    错误: {result['error']}")
                elif 'check_results' in result:
                    for check_name, check_result in result['check_results'].items():
                        if not check_result.get('passed', False):
                            details = check_result.get('details', '未知错误')
                            print(f"    {check_name}: {details}")

        print("=" * 60)

        if len(failed) == 0:
            print("[完成] 所有文件一致性检查通过!")
        else:
            print("[警告] 存在一致性检查失败的文件，请检查转换逻辑")


def main():
    """主函数：执行数据一致性检查"""
    import argparse

    parser = argparse.ArgumentParser(description='检查CSV源数据与数据库数据的一致性')
    parser.add_argument('--data-dir', default='./data',
                       help='CSV数据目录路径 (默认: ./data)')
    parser.add_argument('--db-path', default='./database/accounting.db',
                       help='数据库文件路径 (默认: ./database/accounting.db)')
    parser.add_argument('--single-file',
                       help='检查单个文件（提供完整路径）')

    args = parser.parse_args()

    # 确保数据库文件存在
    if not os.path.exists(args.db_path):
        print(f"[失败] 数据库文件不存在: {args.db_path}")
        print("请先运行 csv_to_db.py 导入数据")
        return

    # 创建检查器
    checker = DataConsistencyChecker(args.db_path)

    if args.single_file:
        # 检查单个文件
        if not os.path.exists(args.single_file):
            print(f"[失败] 文件不存在: {args.single_file}")
            return

        # 从文件名中提取年份
        year = checker._extract_year_from_filename(args.single_file)
        result = checker.check_file_consistency(args.single_file, year)

        if result.get('all_passed', False):
            print("\n[成功] 文件一致性检查通过!")
        else:
            print("\n[失败] 文件一致性检查失败!")
            print("请检查转换逻辑并修复问题")
    else:
        # 检查所有文件
        results = checker.check_all_files(args.data_dir)

        # 检查是否有失败的文件
        failed_files = [r for r in results if not r.get('all_passed', False)]

        if len(failed_files) == 0:
            print("\n[成功] 所有文件一致性检查通过!")
        else:
            print(f"\n[失败] 有 {len(failed_files)} 个文件一致性检查失败")
            print("请检查转换逻辑并修复问题，然后重新运行此脚本")


if __name__ == "__main__":
    main()