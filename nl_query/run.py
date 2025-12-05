#!/usr/bin/env python
"""
自然语言SQL查询工具启动脚本
"""

import sys
import os
from pathlib import Path

def check_dependencies():
    """检查依赖"""
    required_packages = ['streamlit', 'pandas', 'openai', 'python-dotenv']
    missing_packages = []

    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        print(f"缺少依赖包: {', '.join(missing_packages)}")
        print("请运行: pip install -r requirements.txt")
        return False
    return True

def check_config():
    """检查配置"""
    from config import validate_config

    errors = validate_config()
    if errors:
        print("配置错误:")
        for error in errors:
            print(f"  - {error}")

        # 如果是API密钥问题，给出提示
        if any("API密钥" in error for error in errors):
            print("\n请按以下步骤配置:")
            print("1. 复制 .env.example 为 .env")
            print("2. 编辑 .env 文件，设置正确的API密钥")
            print("3. 确保数据库文件存在")

        return False
    return True

def test_connections():
    """测试连接"""
    from sql_generator import SQLGenerator

    print("测试系统连接...")
    generator = SQLGenerator()
    connections = generator.test_connection()

    all_ok = True
    for service, status in connections.items():
        if status:
            print(f"  ✓ {service}: 连接成功")
        else:
            print(f"  ✗ {service}: 连接失败")
            all_ok = False

    return all_ok

def show_welcome():
    """显示欢迎信息"""
    print("=" * 60)
    print("审计凭证自然语言查询工具")
    print("=" * 60)
    print()
    print("功能特点:")
    print("  • 使用自然语言查询审计凭证数据库")
    print("  • 基于DeepSeek API智能生成SQL")
    print("  • 安全的查询执行和结果展示")
    print("  • 丰富的查询示例和历史记录")
    print()

def main():
    """主函数"""
    show_welcome()

    # 检查依赖
    print("1. 检查依赖...")
    if not check_dependencies():
        return 1

    # 检查配置
    print("\n2. 检查配置...")
    if not check_config():
        return 1

    # 测试连接
    print("\n3. 测试连接...")
    if not test_connections():
        print("\n警告: 部分连接测试失败，应用可能无法正常工作")
        print("是否继续? (y/n): ", end="")
        if input().lower() != 'y':
            return 1

    # 启动应用
    print("\n4. 启动应用...")
    print("应用将在浏览器中打开，地址: http://localhost:8501")
    print("按 Ctrl+C 停止应用")
    print("-" * 60)

    try:
        import streamlit.web.bootstrap
        from streamlit.web.cli import _main_run

        # 设置Streamlit配置
        sys.argv = [
            "streamlit", "run",
            str(Path(__file__).parent / "app.py"),
            "--server.port=8501",
            "--server.address=localhost",
            "--theme.base=light",
            "--browser.serverAddress=localhost",
            "--browser.gatherUsageStats=false"
        ]

        _main_run()

    except KeyboardInterrupt:
        print("\n应用已停止")
        return 0
    except Exception as e:
        print(f"启动失败: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())