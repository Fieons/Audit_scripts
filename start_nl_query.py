#!/usr/bin/env python3
"""
独立的自然语言查询系统启动脚本
"""

import os
import sys
import subprocess
import webbrowser
from pathlib import Path

def print_header():
    """打印标题"""
    print("=" * 50)
    print("审计凭证自然语言查询系统")
    print("=" * 50)
    print()

def get_venv_python_path():
    """获取虚拟环境Python路径（跨平台）"""
    import sys

    if sys.platform == "win32":
        # Windows系统
        python_path = Path("venv") / "Scripts" / "python.exe"
    else:
        # Linux/Unix/macOS系统
        python_path = Path("venv") / "bin" / "python"

    return python_path

def check_virtual_env():
    """检查虚拟环境"""
    venv_path = Path("venv")
    if not venv_path.exists():
        print("[错误] 虚拟环境不存在，请先创建虚拟环境")
        print("运行: python -m venv venv  # Windows")
        print("运行: python3 -m venv venv # Linux/Unix/macOS")
        print("然后运行: pip install -r requirements.txt")
        return False

    # 检查Python可执行文件是否存在
    python_path = get_venv_python_path()
    if not python_path.exists():
        print(f"[错误] 虚拟环境中未找到Python可执行文件: {python_path}")
        print("请重新创建虚拟环境")
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
            print("       API密钥可以从 https://platform.deepseek.com/api_keys 获取")
        else:
            print("[错误] 配置文件模板不存在: configs/.env.example")
            return False
    return True

def check_database():
    """检查数据库"""
    db_path = Path("database") / "accounting.db"
    if not db_path.exists():
        print("[警告] 数据库文件不存在: database/accounting.db")
        print("请先运行数据转换工具导入数据:")
        print("  python start_data_conversion.py")
        return False
    return True

def show_help():
    """显示帮助信息"""
    print("\n使用方法:")
    print("  python start_nl_query.py [选项]")
    print()
    print("选项:")
    print("  --port PORT          指定端口号 (默认: 8501)")
    print("  --host HOST          指定主机地址 (默认: localhost)")
    print("  --no-browser         不自动打开浏览器")
    print("  --help               显示此帮助信息")
    print()
    print("示例:")
    print("  python start_nl_query.py")
    print("  python start_nl_query.py --port 8888")
    print("  python start_nl_query.py --host 0.0.0.0")
    print("  python start_nl_query.py --no-browser")

def parse_arguments():
    """解析命令行参数"""
    import argparse

    parser = argparse.ArgumentParser(description='启动自然语言查询系统')
    parser.add_argument('--port', type=int, default=8501,
                       help='Streamlit服务器端口 (默认: 8501)')
    parser.add_argument('--host', default='localhost',
                       help='Streamlit服务器地址 (默认: localhost)')
    parser.add_argument('--no-browser', action='store_true',
                       help='不自动打开浏览器')

    return parser.parse_args()

def main():
    """主函数"""
    print_header()

    # 解析参数
    args = parse_arguments()

    # 检查虚拟环境
    if not check_virtual_env():
        sys.exit(1)

    # 检查配置文件
    if not check_config():
        print("[警告] 配置文件可能不完整，但继续启动...")

    # 检查数据库
    check_database()

    # 设置环境变量
    os.environ["STREAMLIT_SERVER_PORT"] = str(args.port)
    os.environ["STREAMLIT_SERVER_ADDRESS"] = args.host
    os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"

    # 构建URL
    url = f"http://{args.host}:{args.port}"

    print()
    print("=" * 50)
    print("启动信息")
    print("=" * 50)
    print(f"应用地址: {url}")
    print(f"端口: {args.port}")
    print(f"主机: {args.host}")
    print()

    # 自动打开浏览器
    if not args.no_browser:
        print("正在打开浏览器...")
        webbrowser.open(url)
        print("如果未自动打开，请手动访问以上地址")
    else:
        print("请手动访问以上地址")

    print()
    print("按 Ctrl+C 停止应用")
    print()

    # 构建命令
    app_path = Path("src") / "nl_query" / "app.py"
    python_path = get_venv_python_path()
    cmd = [
        str(python_path),
        "-m", "streamlit", "run",
        str(app_path),
        "--server.port", str(args.port),
        "--server.address", args.host
    ]

    try:
        # 设置Python路径
        env = os.environ.copy()
        env["PYTHONPATH"] = str(Path.cwd())

        # 执行命令
        subprocess.run(cmd, env=env, check=True)
    except KeyboardInterrupt:
        print("\n[信息] 应用已停止")
        sys.exit(0)
    except subprocess.CalledProcessError as e:
        print(f"\n[错误] 命令执行失败，退出码: {e.returncode}")
        sys.exit(e.returncode)
    except Exception as e:
        print(f"\n[错误] 发生异常: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # 如果用户请求帮助
    if "--help" in sys.argv or "-h" in sys.argv:
        print_header()
        show_help()
        sys.exit(0)

    main()