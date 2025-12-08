#!/usr/bin/env python
"""
自然语言SQL查询工具 - 统一启动脚本
整合了原run.py和run_with_chat.py的功能
"""

import sys
import os
import time
import subprocess
import webbrowser
from pathlib import Path

def check_dependencies():
    """检查依赖"""
    required_packages = ['streamlit', 'pandas', 'openai', 'python-dotenv', 'sqlalchemy']
    missing_packages = []

    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        print(f"[错误] 缺少依赖包: {', '.join(missing_packages)}")
        print("请运行: pip install -r requirements.txt")
        return False
    return True

def check_config():
    """检查配置"""
    try:
        from .config import validate_config
    except ImportError as e:
        print(f"[错误] 导入配置模块失败: {e}")
        return False

    errors = validate_config()
    if errors:
        print("[错误] 配置错误:")
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
    try:
        from .generator import SQLGenerator
    except ImportError as e:
        print(f"❌ 导入SQL生成器失败: {e}")
        return False

    print("🔌 测试系统连接...")
    try:
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
    except Exception as e:
        print(f"❌ 连接测试失败: {e}")
        return False

def show_welcome():
    """显示欢迎信息"""
    print("=" * 60)
    print("审计凭证自然语言查询工具")
    print("=" * 60)
    print()
    print("功能特性:")
    print("  • 使用自然语言查询审计凭证数据库")
    print("  • 基于DeepSeek API智能生成SQL")
    print("  • 查询讨论和上下文分析（聊天功能）")
    print("  • 可编辑的SQL语句")
    print("  • 数据可视化和结果导出")
    print("  • 安全的查询执行（仅SELECT）")
    print()
    print("界面布局:")
    print("  左侧（2/3宽度）: 查询功能")
    print("    - 查询输入和SQL生成")
    print("    - 查询结果和操作")
    print("  右侧（1/3宽度）: 聊天功能")
    print("    - 查询讨论和上下文分析")
    print()

def start_streamlit_app():
    """启动Streamlit应用"""
    print("🚀 启动应用...")
    print("应用将在浏览器中打开，地址: http://localhost:8501")
    print("按 Ctrl+C 停止应用")
    print("-" * 60)

    try:
        # 延迟打开浏览器
        time.sleep(2)
        webbrowser.open("http://localhost:8501")

        # 构建Streamlit命令
        streamlit_cmd = [
            sys.executable, "-m", "streamlit", "run",
            "app.py",
            "--server.port", "8501",
            "--server.address", "localhost",
            "--theme.base", "light",
            "--browser.serverAddress", "localhost",
            "--browser.gatherUsageStats", "false"
        ]

        # 运行Streamlit
        subprocess.run(streamlit_cmd, cwd=os.path.dirname(__file__))

    except KeyboardInterrupt:
        print("\n🛑 应用已停止")
        return 0
    except Exception as e:
        print(f"❌ 启动失败: {e}")
        return 1

def run_demo_mode():
    """运行演示模式"""
    print("🎬 进入演示模式...")
    try:
        import demo
        demo.demo_basic_functionality()
        return True
    except Exception as e:
        print(f"❌ 演示模式运行失败: {e}")
        return False

def main():
    """主函数"""
    show_welcome()

    # 解析命令行参数
    demo_mode = False
    skip_checks = False

    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            if arg in ['--demo', '-d']:
                demo_mode = True
            elif arg in ['--skip-checks', '-s']:
                skip_checks = True
            elif arg in ['--help', '-h']:
                print("用法: python start.py [选项]")
                print()
                print("选项:")
                print("  --demo, -d       运行演示模式")
                print("  --skip-checks, -s 跳过启动前检查")
                print("  --help, -h       显示此帮助信息")
                return 0

    if demo_mode:
        return 0 if run_demo_mode() else 1

    # 启动前检查
    if not skip_checks:
        print("🔍 启动前检查...")

        print("\n1. 检查依赖...")
        if not check_dependencies():
            return 1

        print("\n2. 检查配置...")
        if not check_config():
            return 1

        print("\n3. 测试连接...")
        if not test_connections():
            print("\n⚠️  警告: 部分连接测试失败，应用可能无法正常工作")
            print("是否继续? (y/n): ", end="")
            if input().lower() != 'y':
                return 1

    # 启动应用
    return start_streamlit_app()

if __name__ == "__main__":
    sys.exit(main())