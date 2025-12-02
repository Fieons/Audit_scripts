#!/usr/bin/env python3
"""
自然语言SQL查询工具
通过LLM将自然语言转换为SQL查询并执行
"""

import sys
import os
from pathlib import Path
import argparse
import json
from typing import Optional, Dict, Any

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def setup_environment():
    """设置环境"""
    # 确保日志目录存在
    logs_dir = project_root / "natural_language_query" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # 检查配置文件是否存在
    config_path = project_root / "natural_language_query" / "config.yaml"
    if not config_path.exists():
        print(f"警告: 配置文件不存在: {config_path}")
        print("将使用默认配置，需要API密钥才能使用LLM功能")

    # 设置环境变量，确保路径正确
    import os
    os.environ['PROJECT_ROOT'] = str(project_root)

def process_query(natural_query: str, limit: Optional[int] = None) -> Dict[str, Any]:
    """处理自然语言查询"""
    try:
        # 加载配置
        from natural_language_query.src.utils.config_loader import load_config
        config_path = project_root / "natural_language_query" / "config.yaml"
        config = load_config(str(config_path))

        from natural_language_query.src.query_processor import (
            QueryProcessor, ProcessingMethod, QueryProcessingResult
        )

        # 创建查询处理器
        processor = QueryProcessor()

        # 处理查询（现在只使用LLM）
        result = processor.process_query(
            query=natural_query,
            use_cache=True
        )

        return {
            "success": True,
            "query": natural_query,
            "method": result.processing_method.value,
            "sql": result.sql_generation.sql if result.sql_generation else None,
            "generation_method": result.sql_generation.method.value if result.sql_generation else None,
            "execution_time": result.query_execution_time,
            "row_count": result.query_execution.row_count if result.query_execution else 0,
            "data": result.query_execution.data if result.query_execution else [],
            "error": None
        }

    except Exception as e:
        import traceback
        print(f"调试: 错误详情: {traceback.format_exc()}")
        return {
            "success": False,
            "query": natural_query,
            "error": str(e),
            "data": []
        }

def interactive_mode():
    """交互模式"""
    print("=" * 60)
    print("自然语言SQL查询工具")
    print("输入自然语言查询，或输入 'quit' 退出")
    print("=" * 60)

    while True:
        try:
            query = input("\n> ").strip()

            if query.lower() in ['quit', 'exit', 'q']:
                print("再见！")
                break

            if not query:
                continue

            print("正在处理查询...")
            result = process_query(query)

            if result["success"]:
                print(f"\nSQL: {result['sql']}")
                print(f"方法: {result['generation_method']}")
                print(f"执行时间: {result['execution_time']:.3f}秒")
                print(f"结果: {result['row_count']} 条记录")

                if result["data"]:
                    print("\n前5条记录:")
                    for i, row in enumerate(result["data"][:5], 1):
                        print(f"  {i}. {row}")
                    if result["row_count"] > 5:
                        print(f"  ... 还有 {result['row_count'] - 5} 条记录")
            else:
                print(f"错误: {result['error']}")

        except KeyboardInterrupt:
            print("\n\n再见！")
            break
        except Exception as e:
            print(f"处理错误: {e}")

def batch_mode(queries_file: str, output_file: Optional[str] = None):
    """批量模式"""
    try:
        with open(queries_file, 'r', encoding='utf-8') as f:
            queries = [line.strip() for line in f if line.strip()]

        results = []
        total = len(queries)

        for i, query in enumerate(queries, 1):
            print(f"处理查询 {i}/{total}: {query[:50]}...")
            result = process_query(query)
            results.append(result)

            if result["success"]:
                print(f"  ✓ 成功: {result['row_count']} 条记录")
            else:
                print(f"  ✗ 失败: {result['error']}")

        # 输出结果
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"\n结果已保存到: {output_file}")

        # 统计信息
        success_count = sum(1 for r in results if r["success"])
        print(f"\n处理完成: {success_count}/{total} 成功")

    except Exception as e:
        print(f"批量处理错误: {e}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="自然语言SQL查询工具")
    parser.add_argument("query", nargs="?", help="自然语言查询语句")
    parser.add_argument("--limit", type=int, help="结果限制条数")
    parser.add_argument("--batch", help="批量处理文件路径")
    parser.add_argument("--output", help="输出文件路径")
    parser.add_argument("--json", action="store_true", help="JSON格式输出")

    args = parser.parse_args()

    # 设置环境
    setup_environment()

    if args.batch:
        # 批量模式
        batch_mode(args.batch, args.output)
    elif args.query:
        # 单次查询模式
        result = process_query(args.query, args.limit)

        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            if result["success"]:
                print(f"查询: {result['query']}")
                print(f"SQL: {result['sql']}")
                print(f"方法: {result['generation_method']}")
                print(f"执行时间: {result['execution_time']:.3f}秒")
                print(f"结果: {result['row_count']} 条记录")

                if result["data"]:
                    print("\n数据:")
                    for i, row in enumerate(result["data"], 1):
                        print(f"  {i}. {row}")
            else:
                print(f"错误: {result['error']}")
    else:
        # 交互模式
        interactive_mode()

if __name__ == "__main__":
    main()