"""
工具函数模块
提供结果格式化、错误处理、日志配置等工具函数
"""

import logging
import json
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import traceback
import hashlib
from pathlib import Path

def setup_logging(log_level: str = "INFO", log_file: str = None):
    """
    配置日志系统

    Args:
        log_level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: 日志文件路径
    """
    # 创建日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # 清除现有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 添加控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 添加文件处理器（如果指定了日志文件）
    if log_file:
        # 确保日志目录存在
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    logging.info(f"日志系统已配置，级别: {log_level}")

def format_error_message(error: Exception, include_traceback: bool = False) -> Dict[str, Any]:
    """
    格式化错误信息

    Args:
        error: 异常对象
        include_traceback: 是否包含堆栈跟踪

    Returns:
        格式化的错误信息字典
    """
    error_info = {
        "error_type": error.__class__.__name__,
        "error_message": str(error),
        "timestamp": datetime.now().isoformat()
    }

    if include_traceback:
        error_info["traceback"] = traceback.format_exc()

    return error_info

def format_sql_for_display(sql: str) -> str:
    """
    格式化SQL语句用于显示

    Args:
        sql: SQL语句

    Returns:
        格式化的SQL字符串
    """
    # 简单的SQL格式化
    keywords = ["SELECT", "FROM", "WHERE", "JOIN", "LEFT JOIN", "RIGHT JOIN",
                "INNER JOIN", "GROUP BY", "ORDER BY", "LIMIT", "HAVING",
                "UNION", "UNION ALL", "WITH"]

    formatted = sql
    for keyword in keywords:
        formatted = formatted.replace(keyword, f"\n{keyword}")

    # 清理多余的空格
    lines = [line.strip() for line in formatted.split('\n') if line.strip()]
    return '\n'.join(lines)

def format_dataframe_for_display(df: pd.DataFrame, max_rows: int = 100,
                                max_cols: int = 20) -> Dict[str, Any]:
    """
    格式化DataFrame用于显示

    Args:
        df: pandas DataFrame
        max_rows: 最大显示行数
        max_cols: 最大显示列数

    Returns:
        格式化的数据字典
    """
    if df.empty:
        return {
            "data": [],
            "columns": [],
            "row_count": 0,
            "column_count": 0,
            "truncated": False
        }

    # 截断数据
    truncated_rows = len(df) > max_rows
    truncated_cols = len(df.columns) > max_cols

    display_df = df.head(max_rows)
    if truncated_cols:
        display_df = display_df.iloc[:, :max_cols]

    # 转换为字典格式
    data = display_df.to_dict('records')
    columns = list(display_df.columns)

    return {
        "data": data,
        "columns": columns,
        "row_count": len(df),
        "column_count": len(df.columns),
        "truncated_rows": truncated_rows,
        "truncated_cols": truncated_cols,
        "display_row_count": len(display_df),
        "display_column_count": len(columns)
    }

def calculate_md5(text: str) -> str:
    """
    计算文本的MD5哈希值

    Args:
        text: 输入文本

    Returns:
        MD5哈希值
    """
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def save_query_result(result: Any, metadata: Dict[str, Any],
                     output_dir: str = "output") -> str:
    """
    保存查询结果到文件

    Args:
        result: 查询结果
        metadata: 查询元数据
        output_dir: 输出目录

    Returns:
        保存的文件路径
    """
    # 创建输出目录
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # 生成文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"query_result_{timestamp}.json"
    filepath = output_path / filename

    # 准备保存的数据
    save_data = {
        "metadata": metadata,
        "timestamp": datetime.now().isoformat()
    }

    # 处理结果数据
    if isinstance(result, pd.DataFrame):
        save_data["result_type"] = "dataframe"
        save_data["result"] = result.to_dict('records')
        save_data["columns"] = list(result.columns)
    elif isinstance(result, list):
        save_data["result_type"] = "list"
        save_data["result"] = result
    elif isinstance(result, dict):
        save_data["result_type"] = "dict"
        save_data["result"] = result
    else:
        save_data["result_type"] = "other"
        save_data["result"] = str(result)

    # 保存到文件
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2)

    logging.info(f"查询结果已保存到: {filepath}")
    return str(filepath)

def load_query_result(filepath: str) -> Dict[str, Any]:
    """
    加载查询结果文件

    Args:
        filepath: 文件路径

    Returns:
        加载的数据
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 如果是DataFrame类型，转换回DataFrame
    if data.get("result_type") == "dataframe":
        import pandas as pd
        data["result"] = pd.DataFrame(data["result"])

    return data

def create_example_queries() -> List[Dict[str, str]]:
    """
    创建示例查询列表

    Returns:
        示例查询列表
    """
    return [
        {
            "title": "公司信息查询",
            "query": "查询所有公司信息",
            "description": "获取数据库中的所有公司基本信息"
        },
        {
            "title": "凭证流水查询",
            "query": "查看2024年和立公司的凭证流水，按日期倒序排列",
            "description": "查询指定公司的凭证记录，支持时间筛选和排序"
        },
        {
            "title": "科目余额分析",
            "query": "统计管理费用科目的借方发生额和贷方发生额",
            "description": "分析特定科目的借贷方发生额"
        },
        {
            "title": "大额交易检测",
            "query": "查找2024年100万以上的大额交易",
            "description": "检测大额交易，用于审计分析"
        },
        {
            "title": "部门费用分摊",
            "query": "分析各部门的费用分摊情况",
            "description": "按部门统计费用分摊比例"
        },
        {
            "title": "项目资金流水",
            "query": "查询高速公路维修项目的资金流水",
            "description": "跟踪特定项目的资金流入流出情况"
        },
        {
            "title": "供应商往来分析",
            "query": "分析和立公司与供应商的往来情况",
            "description": "分析供应商交易金额和频次"
        },
        {
            "title": "月度费用趋势",
            "query": "分析2024年每月管理费用的变化趋势",
            "description": "查看费用的月度变化趋势"
        }
    ]

def validate_natural_language_query(query: str) -> Tuple[bool, str]:
    """
    验证自然语言查询

    Args:
        query: 自然语言查询

    Returns:
        (是否有效, 错误信息)
    """
    if not query or not query.strip():
        return False, "查询不能为空"

    if len(query.strip()) < 3:
        return False, "查询太短，请提供更详细的描述"

    if len(query) > 1000:
        return False, "查询太长，请简化查询内容"

    # 检查是否包含SQL关键字（防止SQL注入尝试）
    sql_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE"]
    query_upper = query.upper()
    for keyword in sql_keywords:
        if keyword in query_upper:
            return False, f"查询包含禁止的关键字: {keyword}"

    return True, ""

class QueryHistory:
    """查询历史管理"""

    def __init__(self, max_history: int = 50):
        self.max_history = max_history
        self.history = []

    def add_query(self, natural_language: str, sql: str, result_metadata: Dict[str, Any]):
        """添加查询到历史"""
        history_item = {
            "id": len(self.history) + 1,
            "timestamp": datetime.now().isoformat(),
            "natural_language": natural_language,
            "sql": sql,
            "result_metadata": result_metadata,
            "success": result_metadata.get("success", False)
        }

        self.history.insert(0, history_item)  # 添加到开头

        # 限制历史记录数量
        if len(self.history) > self.max_history:
            self.history = self.history[:self.max_history]

    def get_recent_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取最近的查询"""
        return self.history[:limit]

    def clear_history(self):
        """清空历史记录"""
        self.history = []

    def save_to_file(self, filepath: str):
        """保存历史记录到文件"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.history, f, ensure_ascii=False, indent=2)

    def load_from_file(self, filepath: str):
        """从文件加载历史记录"""
        if Path(filepath).exists():
            with open(filepath, 'r', encoding='utf-8') as f:
                self.history = json.load(f)

if __name__ == "__main__":
    # 测试工具函数
    setup_logging("INFO")

    # 测试错误格式化
    try:
        raise ValueError("测试错误")
    except ValueError as e:
        error_info = format_error_message(e, include_traceback=True)
        print("错误信息:", json.dumps(error_info, indent=2, ensure_ascii=False))

    # 测试SQL格式化
    sql = "SELECT * FROM companies WHERE name LIKE '%和立%' ORDER BY id LIMIT 10;"
    formatted_sql = format_sql_for_display(sql)
    print("\n格式化SQL:")
    print(formatted_sql)

    # 测试MD5计算
    text = "测试文本"
    md5_hash = calculate_md5(text)
    print(f"\nMD5哈希: {md5_hash}")

    # 测试示例查询
    examples = create_example_queries()
    print(f"\n示例查询数量: {len(examples)}")
    for example in examples[:3]:
        print(f"  - {example['title']}: {example['query']}")