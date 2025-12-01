"""
日志模块

提供统一的日志记录功能，支持控制台和文件输出。
"""

import os
import sys
import logging
import logging.handlers
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

# 默认日志格式
DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 日志级别映射
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class ColoredFormatter(logging.Formatter):
    """带颜色的日志格式化器"""

    # 颜色代码
    COLORS = {
        "DEBUG": "\033[36m",  # 青色
        "INFO": "\033[32m",  # 绿色
        "WARNING": "\033[33m",  # 黄色
        "ERROR": "\033[31m",  # 红色
        "CRITICAL": "\033[35m",  # 紫色
    }
    RESET = "\033[0m"

    def format(self, record):
        """格式化日志记录"""
        # 添加颜色
        if record.levelname in self.COLORS:
            color_code = self.COLORS[record.levelname]
            record.levelname = f"{color_code}{record.levelname}{self.RESET}"
            record.msg = f"{color_code}{record.msg}{self.RESET}"

        return super().format(record)


def setup_logger(
    name: str = "natural_language_query",
    level: str = "INFO",
    log_file: Optional[str] = None,
    log_format: str = DEFAULT_FORMAT,
    date_format: str = DEFAULT_DATE_FORMAT,
    enable_console: bool = True,
    enable_file: bool = True,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
    enable_color: bool = True,
) -> logging.Logger:
    """
    设置日志记录器

    Args:
        name: 日志记录器名称
        level: 日志级别
        log_file: 日志文件路径
        log_format: 日志格式
        date_format: 日期格式
        enable_console: 是否启用控制台输出
        enable_file: 是否启用文件输出
        max_file_size: 最大文件大小（字节）
        backup_count: 备份文件数量
        enable_color: 是否启用颜色

    Returns:
        logging.Logger: 配置好的日志记录器
    """
    # 获取或创建日志记录器
    logger = logging.getLogger(name)

    # 避免重复添加处理器
    if logger.handlers:
        return logger

    # 设置日志级别
    log_level = LOG_LEVELS.get(level.upper(), logging.INFO)
    logger.setLevel(log_level)

    # 创建格式化器
    if enable_color and enable_console:
        formatter = ColoredFormatter(log_format, datefmt=date_format)
    else:
        formatter = logging.Formatter(log_format, datefmt=date_format)

    # 控制台处理器
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 文件处理器
    if enable_file and log_file:
        # 确保日志目录存在
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # 使用RotatingFileHandler实现日志轮转
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding="utf-8"
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # 避免日志传递给根记录器
    logger.propagate = False

    return logger


def get_logger(name: str = "natural_language_query") -> logging.Logger:
    """
    获取日志记录器

    Args:
        name: 日志记录器名称

    Returns:
        logging.Logger: 日志记录器
    """
    return logging.getLogger(name)


class QueryLogger:
    """查询日志记录器"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化查询日志记录器

        Args:
            logger: 日志记录器，如果为None则使用默认记录器
        """
        self.logger = logger or get_logger("query")
        self.query_logs: Dict[str, Dict[str, Any]] = {}

    def log_query_start(self, query_id: str, natural_language: str, **kwargs):
        """记录查询开始"""
        log_entry = {
            "query_id": query_id,
            "natural_language": natural_language,
            "start_time": datetime.now().isoformat(),
            "status": "started",
            **kwargs
        }

        self.query_logs[query_id] = log_entry
        self.logger.info(f"查询开始 [{query_id}]: {natural_language}")

    def log_query_sql_generated(self, query_id: str, sql: str, method: str = "llm"):
        """记录SQL生成"""
        if query_id in self.query_logs:
            self.query_logs[query_id]["sql"] = sql
            self.query_logs[query_id]["generation_method"] = method
            self.logger.info(f"SQL生成 [{query_id}]: {method}\n{sql}")

    def log_query_executed(self, query_id: str, result_count: int, execution_time: float):
        """记录查询执行"""
        if query_id in self.query_logs:
            self.query_logs[query_id]["result_count"] = result_count
            self.query_logs[query_id]["execution_time"] = execution_time
            self.query_logs[query_id]["end_time"] = datetime.now().isoformat()
            self.query_logs[query_id]["status"] = "completed"

            self.logger.info(
                f"查询完成 [{query_id}]: {result_count} 结果, 耗时 {execution_time:.3f}秒"
            )

    def log_query_error(self, query_id: str, error: str, stage: str = "execution"):
        """记录查询错误"""
        if query_id in self.query_logs:
            self.query_logs[query_id]["error"] = error
            self.query_logs[query_id]["error_stage"] = stage
            self.query_logs[query_id]["end_time"] = datetime.now().isoformat()
            self.query_logs[query_id]["status"] = "failed"

            self.logger.error(f"查询失败 [{query_id}]: {stage} - {error}")

    def log_query_cached(self, query_id: str, cache_key: str):
        """记录查询缓存命中"""
        if query_id in self.query_logs:
            self.query_logs[query_id]["cached"] = True
            self.query_logs[query_id]["cache_key"] = cache_key
            self.logger.debug(f"缓存命中 [{query_id}]: {cache_key}")

    def get_query_log(self, query_id: str) -> Optional[Dict[str, Any]]:
        """获取查询日志"""
        return self.query_logs.get(query_id)

    def save_query_logs(self, file_path: str):
        """保存查询日志到文件"""
        try:
            import json
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(self.query_logs, f, indent=2, ensure_ascii=False)
            self.logger.info(f"查询日志已保存到: {file_path}")
        except Exception as e:
            self.logger.error(f"保存查询日志失败: {e}")

    def clear_query_logs(self):
        """清空查询日志"""
        self.query_logs.clear()
        self.logger.info("查询日志已清空")


class ErrorHandler:
    """错误处理器"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化错误处理器

        Args:
            logger: 日志记录器
        """
        self.logger = logger or get_logger("error")

    def handle_error(self, error: Exception, context: str = "", raise_again: bool = False):
        """
        处理错误

        Args:
            error: 异常对象
            context: 错误上下文
            raise_again: 是否重新抛出异常
        """
        error_type = type(error).__name__
        error_msg = str(error)

        # 记录错误
        if context:
            log_msg = f"{context}: {error_type} - {error_msg}"
        else:
            log_msg = f"{error_type} - {error_msg}"

        self.logger.error(log_msg, exc_info=True)

        # 根据错误类型采取不同措施
        error_handlers = {
            "ConnectionError": self._handle_connection_error,
            "TimeoutError": self._handle_timeout_error,
            "ValueError": self._handle_value_error,
            "PermissionError": self._handle_permission_error,
            "FileNotFoundError": self._handle_file_not_found_error,
        }

        handler = error_handlers.get(error_type, self._handle_generic_error)
        handler(error)

        # 是否重新抛出
        if raise_again:
            raise error

    def _handle_connection_error(self, error: Exception):
        """处理连接错误"""
        self.logger.warning("连接错误，尝试重连...")

    def _handle_timeout_error(self, error: Exception):
        """处理超时错误"""
        self.logger.warning("操作超时，请检查网络或增加超时时间")

    def _handle_value_error(self, error: Exception):
        """处理值错误"""
        self.logger.warning("参数值错误，请检查输入")

    def _handle_permission_error(self, error: Exception):
        """处理权限错误"""
        self.logger.error("权限不足，请检查文件权限")

    def _handle_file_not_found_error(self, error: Exception):
        """处理文件未找到错误"""
        self.logger.error("文件未找到，请检查文件路径")

    def _handle_generic_error(self, error: Exception):
        """处理通用错误"""
        self.logger.error("发生未知错误")

    def wrap_with_error_handling(self, func):
        """包装函数，自动处理错误"""
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self.handle_error(e, f"执行 {func.__name__} 时发生错误")
                return None
        return wrapper


# 全局日志记录器实例
_query_logger: Optional[QueryLogger] = None
_error_handler: Optional[ErrorHandler] = None


def get_query_logger() -> QueryLogger:
    """获取查询日志记录器"""
    global _query_logger
    if _query_logger is None:
        _query_logger = QueryLogger()
    return _query_logger


def get_error_handler() -> ErrorHandler:
    """获取错误处理器"""
    global _error_handler
    if _error_handler is None:
        _error_handler = ErrorHandler()
    return _error_handler


def init_logging_from_config():
    """从配置初始化日志"""
    try:
        from ..utils.config_loader import load_config
        config = load_config()

        setup_logger(
            level=config.logging.level,
            log_file=config.logging.file,
            log_format=config.logging.format,
            enable_color=True
        )

        logger = get_logger()
        logger.info("日志系统初始化完成")
        logger.info(f"日志级别: {config.logging.level}")
        logger.info(f"日志文件: {config.logging.file}")

    except Exception as e:
        # 如果配置加载失败，使用默认配置
        setup_logger()
        logger = get_logger()
        logger.warning(f"从配置初始化日志失败，使用默认配置: {e}")


# 导出
__all__ = [
    "setup_logger",
    "get_logger",
    "QueryLogger",
    "ErrorHandler",
    "get_query_logger",
    "get_error_handler",
    "init_logging_from_config",
]