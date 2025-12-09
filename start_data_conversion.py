#!/usr/bin/env python3
"""
独立的数据转换工具启动脚本
"""

import os
import sys
import subprocess
from pathlib import Path

def print_header():
    """打印标题"""
    print("=" * 50)
    print("审计凭证数据转换工具")
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

def check_data_directory():
    """检查数据目录"""
    data_dir = Path("data")
    if not data_dir.exists():
        print("[警告] 数据目录不存在: data/")
        print("请将CSV文件放入 data/ 目录中")
        return False

    # 检查是否有CSV文件
    csv_files = list(data_dir.glob("*.csv"))
    if not csv_files:
        print("[警告] 数据目录中没有CSV文件")
        print("请将CSV文件放入 data/ 目录中")
        return False

    print(f"[信息] 找到 {len(csv_files)} 个CSV文件")
    return True

def show_help():
    """显示帮助信息"""
    print("\n使用方法:")
    print("  python start_data_conversion.py [选项]")
    print()
    print("选项:")
    print("  --data-dir PATH      CSV数据目录路径 (默认: ./data)")
    print("  --db-path PATH       数据库文件路径 (默认: database/accounting.db)")
    print("  --reset-db           重置数据库（删除所有表）")
    print("  --validate-only      只验证数据库完整性，不导入数据")
    print("  --help               显示此帮助信息")
    print()
    print("示例:")
    print("  python start_data_conversion.py")
    print("  python start_data_conversion.py --data-dir ./my_data")
    print("  python start_data_conversion.py --reset-db")
    print("  python start_data_conversion.py --validate-only")

def main():
    """主函数"""
    print_header()

    # 检查虚拟环境
    if not check_virtual_env():
        sys.exit(1)

    # 检查数据目录
    check_data_directory()

    # 构建命令
    python_path = get_venv_python_path()
    cmd = [
        str(python_path),
        "-m", "src.data_conversion.converter"
    ]

    # 添加用户参数
    if len(sys.argv) > 1:
        cmd.extend(sys.argv[1:])

    # 显示命令
    print(f"\n[执行] 命令: {' '.join(cmd)}")
    print()

    try:
        # 设置Python路径
        env = os.environ.copy()
        env["PYTHONPATH"] = str(Path.cwd())

        # 执行命令
        subprocess.run(cmd, env=env, check=True)
    except KeyboardInterrupt:
        print("\n[信息] 用户取消操作")
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