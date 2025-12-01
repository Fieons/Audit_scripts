import os
from datetime import datetime

# SQLite数据库配置
DB_CONFIG = {
    'database': os.path.join(os.path.dirname(__file__), 'accounting.db'),
    'timeout': 30
}

# 文件路径配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
SQL_DIR = os.path.join(BASE_DIR, 'sql')

# 输入文件路径
INPUT_JOURNAL_FILE = os.path.join(DATA_DIR, '序时账2025.1-9.csv')

# 输出文件路径
OUTPUT_CLEANED_DATA = os.path.join(OUTPUT_DIR, f'cleaned_journal_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
OUTPUT_DIMENSIONS = os.path.join(OUTPUT_DIR, 'dimensions')
OUTPUT_FACT_TABLE = os.path.join(OUTPUT_DIR, 'fact_tables')

# 处理配置
BATCH_SIZE = 5000  # SQLite建议较小的批处理大小
START_DATE = '2025-01-01'
END_DATE = '2025-09-30'

# 日志配置
LOG_LEVEL = 'INFO'
LOG_FILE = os.path.join(OUTPUT_DIR, f'transformation_{datetime.now().strftime("%Y%m%d")}.log')