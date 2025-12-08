"""
演示自然语言SQL查询工具的核心功能
"""

import sys
import os
sys.path.append('.')

def demo_basic_functionality():
    """演示基本功能"""
    print("=" * 60)
    print("自然语言SQL查询工具 - 功能演示")
    print("=" * 60)

    from .generator import SQLGenerator
    from .utils import validate_natural_language_query

    # 初始化
    print("\n1. 初始化SQL生成器...")
    generator = SQLGenerator()

    # 测试连接
    print("\n2. 测试系统连接...")
    connections = generator.test_connection()

    for service, status in connections.items():
        status_text = "✓ 成功" if status else "✗ 失败"
        print(f"   {service}: {status_text}")

    if not connections.get('database'):
        print("\n错误: 数据库连接失败，无法继续演示")
        return

    # 演示直接SQL查询
    print("\n3. 演示直接SQL查询...")
    try:
        sql = "SELECT name, code FROM companies LIMIT 3"
        result, metadata = generator.execute_query(sql)

        print(f"   SQL: {sql}")
        print(f"   结果: {len(result)} 行数据")
        print("   示例数据:")
        for i, row in result.iterrows():
            print(f"     - {row['name']} ({row['code']})")
    except Exception as e:
        print(f"   查询失败: {e}")

    # 演示自然语言查询验证
    print("\n4. 演示自然语言查询验证...")
    test_queries = [
        "查询所有公司信息",
        "查看2024年的凭证流水",
        "统计管理费用科目",
        "DROP TABLE users"  # 这个应该被拒绝
    ]

    for query in test_queries:
        is_valid, msg = validate_natural_language_query(query)
        status = "✓ 有效" if is_valid else "✗ 无效"
        print(f"   '{query}': {status}")
        if not is_valid:
            print(f"     原因: {msg}")


    # 显示配置信息
    print("\n6. 系统配置信息...")
    from .config import get_config_summary
    summary = get_config_summary()

    important_keys = ['database_path', 'max_results', 'query_timeout', 'cache_enabled']
    for key in important_keys:
        if key in summary:
            print(f"   {key}: {summary[key]}")

    print("\n" + "=" * 60)
    print("演示完成!")
    print("\n下一步:")
    print("1. 确保所有连接测试成功")
    print("2. 运行启动脚本: start_app.bat")
    print("3. 在浏览器中访问: http://localhost:8501")
    print("4. 尝试自然语言查询，如: '查询所有公司信息'")
    print("=" * 60)

if __name__ == "__main__":
    demo_basic_functionality()