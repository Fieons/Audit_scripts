"""
测试应用核心功能
"""

import sys
import os
sys.path.append('.')

def test_app_components():
    """测试应用各个组件"""
    print("测试自然语言SQL查询工具组件...")
    print("=" * 60)

    # 测试1: 配置
    print("\n1. 测试配置模块...")
    from config import validate_config, get_config_summary
    errors = validate_config()
    if errors:
        print(f"  配置警告: {len(errors)} 个问题")
        for error in errors[:3]:  # 只显示前3个
            print(f"    - {error}")
    else:
        print("  配置验证通过")

    # 测试2: 数据库连接
    print("\n2. 测试数据库连接...")
    from database import DatabaseManager
    db = DatabaseManager()
    if db.test_connection():
        print("  数据库连接成功")
        with db:
            # 测试查询
            result = db.execute_query("SELECT COUNT(*) as count FROM companies")
            print(f"  公司数量: {result.iloc[0]['count']}")

            # 测试schema
            schema = db.get_schema_info()
            print(f"  表数量: {schema['total_tables']}")
            print(f"  表名: {[t['name'] for t in schema['tables'][:3]]}...")
    else:
        print("  数据库连接失败")

    # 测试3: SQL生成器
    print("\n3. 测试SQL生成器...")
    from sql_generator import SQLGenerator
    generator = SQLGenerator()
    connections = generator.test_connection()

    print(f"  数据库连接: {'成功' if connections.get('database') else '失败'}")
    print(f"  API连接: {'成功' if connections.get('deepseek_api') else '失败'}")

    if connections.get('database'):
        # 测试简单查询
        try:
            result, metadata = generator.execute_query("SELECT name FROM companies LIMIT 3")
            print(f"  简单查询测试: 成功 ({len(result)} 行数据)")
            print(f"  示例公司: {', '.join(result['name'].tolist())}")
        except Exception as e:
            print(f"  简单查询测试失败: {e}")

    # 测试4: 工具函数
    print("\n4. 测试工具函数...")
    from utils import validate_natural_language_query, create_example_queries

    # 测试查询验证
    test_queries = [
        ("查询公司信息", True),
        ("DROP TABLE users", False),
        ("", False)
    ]

    for query, expected_valid in test_queries:
        is_valid, msg = validate_natural_language_query(query)
        status = "通过" if is_valid == expected_valid else "失败"
        print(f"  查询验证 '{query[:20]}...': {status}")

    # 测试示例查询
    examples = create_example_queries()
    print(f"  示例查询数量: {len(examples)}")

    # 测试5: 应用初始化
    print("\n5. 测试应用初始化...")
    try:
        # 模拟应用初始化
        import streamlit as st

        # 初始化会话状态
        st.session_state = type('obj', (object,), {})()
        st.session_state.sql_generator = generator
        st.session_state.query_history = type('obj', (object,), {'history': []})()

        print("  应用初始化模拟: 成功")

        # 测试自然语言到SQL转换（模拟）
        print("\n6. 测试自然语言到SQL流程...")
        test_nl_queries = [
            "查询所有公司信息",
            "查看2024年的凭证",
            "统计管理费用"
        ]

        for query in test_nl_queries:
            print(f"  测试查询: '{query}'")
            try:
                # 这里不实际调用API，只测试流程
                is_valid, msg = validate_natural_language_query(query)
                if is_valid:
                    print(f"    验证: 通过")
                else:
                    print(f"    验证: 失败 - {msg}")
            except Exception as e:
                print(f"    错误: {e}")

    except Exception as e:
        print(f"  应用初始化测试失败: {e}")

    print("\n" + "=" * 60)
    print("组件测试完成！")
    print("\n下一步:")
    print("1. 确保DeepSeek API密钥有效")
    print("2. 运行: streamlit run app.py")
    print("3. 访问: http://localhost:8501")

if __name__ == "__main__":
    test_app_components()