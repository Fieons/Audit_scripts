#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
银行付款去向跟踪 - 便捷启动脚本
从项目根目录直接运行此脚本
"""

import os
import sys

# 添加src目录到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, src_dir)

# 导入并运行主程序
from bank_payment import main

if __name__ == "__main__":
    print("正在启动银行付款去向跟踪程序...")
    main()