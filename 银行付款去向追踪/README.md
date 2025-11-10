# 银行付款去向跟踪系统

基于LLM的智能银行付款资金流向分析系统，支持所有类型的会计分录场景。

## 快速开始

### 1. 环境准备
```bash
# 激活虚拟环境
source ../venv/bin/activate  # Linux/Mac
# 或
../venv/Scripts/activate     # Windows

# 安装依赖（如果需要）
pip install requests
```

### 2. 配置API密钥
编辑 `src/bank_payment.py` 文件，设置您的LLM API密钥：
```python
llm_api_key = "your_api_key_here"
```

### 3. 运行程序

#### 主程序：银行付款去向跟踪
```bash
python src/bank_payment.py
```

#### 统计分析（可选）
```bash
python src/payment_statistics.py
```

## 项目结构

```
序时账相关/
├── src/                    # 源代码
│   ├── bank_payment.py      # 主程序
│   ├── ai_classifier.py     # AI分类模块
│   └── payment_statistics.py # 统计分析模块
├── docs/                   # 文档
│   └── 使用说明.md          # 详细使用说明
├── examples/               # 示例数据
│   └── 序时账2025.1-9.csv   # 示例序时账数据
├── output/                 # 输出文件目录
└── README.md              # 本文件
```

## 主要功能

- ✅ **全场景支持**：一贷一借、一贷多借、多贷一借、多贷多借
- ✅ **智能分类**：基于LLM的款项用途和现金流量分类
- ✅ **精确追踪**：准确追踪每笔付款的资金去向
- ✅ **模块化设计**：主程序和统计功能分离
- ✅ **完整校验**：借贷平衡校验，确保数据准确性

## 输出文件

- `output/银行付款去向跟踪结果.json` - 付款记录数据
- `output/银行付款统计分析结果.json` - 统计分析报告

详细使用说明请查看 `docs/使用说明.md`

---

**版本**: v2.0
**更新**: 2025-11-10