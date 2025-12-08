#!/usr/bin/env python3
"""
统一启动脚本 - 支持自然语言查询系统和数据库转换工具
"""

import os
import sys
import subprocess
import webbrowser
from pathlib import Path

def print_header():
    """打印标题"""
    print("=" * 50)
    print("审计凭证数据处理与分析系统")
    print("=" * 50)
    print()

def print_menu():
    """显示菜单"""
    print("请选择要启动的功能:")
    print("1. 自然语言查询系统 (Streamlit Web界面)")
    print("2. 数据库转换工具 (命令行)")
    print("3. 运行测试")
    print("4. 检查依赖")
    print()

def check_virtual_env():
    """检查虚拟环境"""
    venv_path = Path("venv")
    if not venv_path.exists():
        print("[错误] 虚拟环境不存在，请先创建虚拟环境")
        print("运行: python -m venv venv")
        print("然后运行: pip install -r requirements.txt")
        return False
    return True

def check_config():
    """检查配置文件"""
    config_dir = Path("configs")
    env_file = config_dir / ".env"
    env_example = config_dir / ".env.example"

    if not env_file.exists():
        print("[警告] 未找到配置文件，正在创建...")
        if env_example.exists():
            import shutil
            shutil.copy(env_example, env_file)
            print("[重要] 请编辑 configs/.env 文件，配置您的DeepSeek API密钥")
        else:
            print("[错误] 配置文件模板不存在: configs/.env.example")
            return False
    return True

def start_nl_query():
    """启动自然语言查询系统"""
    print()
    print("=" * 50)
    print("启动自然语言查询系统...")
    print("=" * 50)
    print()

    if not check_virtual_env():
        return

    if not check_config():
        print("[警告] 配置文件可能不完整，但继续启动...")

    # 设置环境变量
    os.environ["STREAMLIT_SERVER_PORT"] = "8501"
    os.environ["STREAMLIT_SERVER_ADDRESS"] = "localhost"
    os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"

    print()
    print("应用将在浏览器中打开...")
    print("如果未自动打开，请访问: http://localhost:8501")
    print("按 Ctrl+C 停止应用")
    print()

    # 在后台打开浏览器
    webbrowser.open("http://localhost:8501")

    # 启动Streamlit
    app_path = Path("src") / "nl_query" / "app.py"
    project_root = Path.cwd()

    # 设置Python路径，确保导入能正常工作
    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root)

    cmd = [
        str(Path("venv") / "Scripts" / "python.exe"),
        "-m", "streamlit", "run",
        str(app_path),
        "--server.port", "8501",
        "--server.address", "localhost"
    ]

    try:
        subprocess.run(cmd, env=env, check=True)
    except KeyboardInterrupt:
        print("\n应用已停止")
    except Exception as e:
        print(f"[错误] 启动失败: {e}")

def start_data_conversion():
    """启动数据库转换工具"""
    print()
    print("=" * 50)
    print("启动数据库转换工具...")
    print("=" * 50)
    print()

    if not check_virtual_env():
        return

    cmd = [
        str(Path("venv") / "Scripts" / "python.exe"),
        "-m", "src.data_conversion.converter",
        "--help"
    ]

    try:
        subprocess.run(cmd, check=True)
    except Exception as e:
        print(f"[错误] 启动失败: {e}")

def run_tests():
    """运行测试"""
    print()
    print("=" * 50)
    print("运行测试...")
    print("=" * 50)
    print()

    if not check_virtual_env():
        return

    cmd = [
        str(Path("venv") / "Scripts" / "python.exe"),
        "-m", "pytest",
        "tests/", "-v"
    ]

    try:
        subprocess.run(cmd, check=True)
    except Exception as e:
        print(f"[错误] 运行测试失败: {e}")

def check_dependencies():
    """检查依赖"""
    print()
    print("=" * 50)
    print("检查依赖...")
    print("=" * 50)
    print()

    if not check_virtual_env():
        return

    cmds = [
        [str(Path("venv") / "Scripts" / "python.exe"), "--version"],
        [str(Path("venv") / "Scripts" / "pip.exe"), "list"]
    ]

    for cmd in cmds:
        try:
            print(f"[信息] 运行: {' '.join(cmd)}")
            subprocess.run(cmd, check=True)
            print()
        except Exception as e:
            print(f"[错误] 执行失败: {e}")

def main():
    """主函数"""
    print_header()
    print_menu()

    try:
        choice = input("请输入选项 (1-4): ").strip()

        if choice == "1":
            start_nl_query()
        elif choice == "2":
            start_data_conversion()
        elif choice == "3":
            run_tests()
        elif choice == "4":
            check_dependencies()
        else:
            print("[错误] 无效选项")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n用户取消操作")
        sys.exit(0)
    except Exception as e:
        print(f"[错误] 发生异常: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()