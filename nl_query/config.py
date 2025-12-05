"""
配置文件管理模块
处理API密钥、数据库路径等配置信息
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
NL_QUERY_ROOT = Path(__file__).parent

# DeepSeek API配置
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-7e2810b6b904460c88b5e9a80838f288")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

# 数据库配置
DATABASE_PATH = os.getenv("DATABASE_PATH", str(PROJECT_ROOT / "database" / "accounting.db"))

# 查询配置
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "1000"))
QUERY_TIMEOUT = int(os.getenv("QUERY_TIMEOUT", "30"))  # 秒
MAX_SQL_LENGTH = int(os.getenv("MAX_SQL_LENGTH", "5000"))

# 缓存配置
CACHE_ENABLED = os.getenv("CACHE_ENABLED", "true").lower() == "true"
CACHE_MAX_SIZE = int(os.getenv("CACHE_MAX_SIZE", "100"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "3600"))  # 秒

# 安全配置
ALLOWED_SQL_KEYWORDS = ["SELECT", "WITH", "FROM", "WHERE", "JOIN", "GROUP BY",
                        "ORDER BY", "LIMIT", "OFFSET", "HAVING", "UNION", "DISTINCT"]
FORBIDDEN_SQL_KEYWORDS = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE",
                         "CREATE", "GRANT", "REVOKE", "EXEC", "EXECUTE"]

# 日志配置
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", str(NL_QUERY_ROOT / "nl_query.log"))

def validate_config():
    """验证配置是否有效"""
    errors = []

    # 检查API密钥 - 允许使用提供的密钥
    if not DEEPSEEK_API_KEY:
        errors.append("请设置DeepSeek API密钥")
    elif DEEPSEEK_API_KEY == "sk-7e2810b6b904460c88b5e9a80838f288":
        # 这是用户提供的密钥，不报错，只记录警告
        print(f"警告: 使用默认API密钥，请确保密钥有效")

    # 检查数据库文件
    if not Path(DATABASE_PATH).exists():
        errors.append(f"数据库文件不存在: {DATABASE_PATH}")

    # 检查数值配置
    if MAX_RESULTS <= 0:
        errors.append(f"MAX_RESULTS必须大于0，当前值: {MAX_RESULTS}")
    if QUERY_TIMEOUT <= 0:
        errors.append(f"QUERY_TIMEOUT必须大于0，当前值: {QUERY_TIMEOUT}")

    return errors

def get_config_summary():
    """获取配置摘要"""
    return {
        "deepseek_api_key": DEEPSEEK_API_KEY[:10] + "..." if DEEPSEEK_API_KEY else None,
        "deepseek_base_url": DEEPSEEK_BASE_URL,
        "deepseek_model": DEEPSEEK_MODEL,
        "database_path": DATABASE_PATH,
        "max_results": MAX_RESULTS,
        "query_timeout": QUERY_TIMEOUT,
        "cache_enabled": CACHE_ENABLED,
        "cache_max_size": CACHE_MAX_SIZE,
        "log_level": LOG_LEVEL
    }

if __name__ == "__main__":
    # 配置验证测试
    errors = validate_config()
    if errors:
        print("配置错误:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("配置验证通过")
        summary = get_config_summary()
        print("\n配置摘要:")
        for key, value in summary.items():
            print(f"  {key}: {value}")