"""
配置加载器

提供配置文件的动态加载和覆盖功能。
"""

import os
import json
import yaml
from pathlib import Path
from typing import Any, Dict, Optional
from dataclasses import dataclass, field, asdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """数据库配置"""
    path: str = "../database_transformation/accounting.db"
    pool_size: int = 5
    timeout: int = 30
    echo: bool = False


@dataclass
class APIConfig:
    """API配置"""
    base_url: str = "https://api.deepseek.com"
    model: str = "deepseek-chat"
    api_key: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3
    request_interval: float = 1.0


@dataclass
class QueryConfig:
    """查询配置"""
    cache_size: int = 100
    cache_ttl: int = 300
    max_results_per_page: int = 100
    default_limit: int = 50
    enable_cache: bool = True


@dataclass
class SecurityConfig:
    """安全配置"""
    enable_sql_injection_check: bool = True
    max_query_complexity: int = 10
    enable_data_masking: bool = True


@dataclass
class LoggingConfig:
    """日志配置"""
    level: str = "INFO"
    file: str = "logs/query_system.log"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5


@dataclass
class UIConfig:
    """用户界面配置"""
    enable_rich_output: bool = True
    interactive_timeout: int = 300
    history_size: int = 100
    prompt_style: str = "bold green"


@dataclass
class SystemConfig:
    """系统配置"""
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    api: APIConfig = field(default_factory=APIConfig)
    query: QueryConfig = field(default_factory=QueryConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    ui: UIConfig = field(default_factory=UIConfig)


class ConfigLoader:
    """配置加载器"""

    def __init__(self, config_file: Optional[str] = None):
        """
        初始化配置加载器

        Args:
            config_file: 配置文件路径，如果为None则使用默认配置
        """
        self.config_file = config_file
        self.config = SystemConfig()
        self._loaded = False

    def load(self) -> SystemConfig:
        """加载配置"""
        if self._loaded:
            return self.config

        # 1. 加载默认配置
        self._load_defaults()

        # 2. 从配置文件加载（如果存在）
        if self.config_file and Path(self.config_file).exists():
            self._load_from_file(self.config_file)

        # 3. 从环境变量加载
        self._load_from_env()

        # 4. 验证配置
        self._validate_config()

        self._loaded = True
        logger.info("配置加载完成")
        return self.config

    def _load_defaults(self):
        """加载默认配置"""
        # 数据库配置
        self.config.database = DatabaseConfig()

        # API配置
        self.config.api = APIConfig()

        # 查询配置
        self.config.query = QueryConfig()

        # 安全配置
        self.config.security = SecurityConfig()

        # 日志配置
        self.config.logging = LoggingConfig()

        # 用户界面配置
        self.config.ui = UIConfig()

    def _load_from_file(self, config_file: str):
        """从配置文件加载"""
        file_path = Path(config_file)
        # print(f"DEBUG: 尝试加载配置文件: {file_path}")
        # print(f"DEBUG: 配置文件是否存在: {file_path.exists()}")

        if not file_path.exists():
            logger.warning(f"配置文件不存在: {config_file}")
            return

        try:
            if file_path.suffix.lower() == '.json':
                self._load_json(file_path)
            elif file_path.suffix.lower() in ['.yaml', '.yml']:
                self._load_yaml(file_path)
            else:
                logger.warning(f"不支持的配置文件格式: {file_path.suffix}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")

    def _load_json(self, file_path: Path):
        """加载JSON配置文件"""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        self._update_config_from_dict(data)

    def _load_yaml(self, file_path: Path):
        """加载YAML配置文件"""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        self._update_config_from_dict(data)

    def _update_config_from_dict(self, data: Dict[str, Any]):
        """从字典更新配置"""
        # 更新数据库配置
        if 'database' in data:
            db_data = data['database']
            self.config.database = DatabaseConfig(
                path=db_data.get('path', self.config.database.path),
                pool_size=db_data.get('pool_size', self.config.database.pool_size),
                timeout=db_data.get('timeout', self.config.database.timeout),
                echo=db_data.get('echo', self.config.database.echo),
            )

        # 更新API配置
        if 'api' in data:
            api_data = data['api']
            self.config.api = APIConfig(
                base_url=api_data.get('base_url', self.config.api.base_url),
                model=api_data.get('model', self.config.api.model),
                api_key=api_data.get('api_key', self.config.api.api_key),
                timeout=api_data.get('timeout', self.config.api.timeout),
                max_retries=api_data.get('max_retries', self.config.api.max_retries),
                request_interval=api_data.get('request_interval', self.config.api.request_interval),
            )

        # 更新查询配置
        if 'query' in data:
            query_data = data['query']
            self.config.query = QueryConfig(
                cache_size=query_data.get('cache_size', self.config.query.cache_size),
                cache_ttl=query_data.get('cache_ttl', self.config.query.cache_ttl),
                max_results_per_page=query_data.get('max_results_per_page', self.config.query.max_results_per_page),
                default_limit=query_data.get('default_limit', self.config.query.default_limit),
                enable_cache=query_data.get('enable_cache', self.config.query.enable_cache),
            )

        # 更新安全配置
        if 'security' in data:
            security_data = data['security']
            self.config.security = SecurityConfig(
                enable_sql_injection_check=security_data.get('enable_sql_injection_check',
                                                             self.config.security.enable_sql_injection_check),
                max_query_complexity=security_data.get('max_query_complexity',
                                                       self.config.security.max_query_complexity),
                enable_data_masking=security_data.get('enable_data_masking',
                                                      self.config.security.enable_data_masking),
            )

        # 更新日志配置
        if 'logging' in data:
            logging_data = data['logging']
            self.config.logging = LoggingConfig(
                level=logging_data.get('level', self.config.logging.level),
                file=logging_data.get('file', self.config.logging.file),
                format=logging_data.get('format', self.config.logging.format),
                max_file_size=logging_data.get('max_file_size', self.config.logging.max_file_size),
                backup_count=logging_data.get('backup_count', self.config.logging.backup_count),
            )

        # 更新用户界面配置
        if 'ui' in data:
            ui_data = data['ui']
            self.config.ui = UIConfig(
                enable_rich_output=ui_data.get('enable_rich_output', self.config.ui.enable_rich_output),
                interactive_timeout=ui_data.get('interactive_timeout', self.config.ui.interactive_timeout),
                history_size=ui_data.get('history_size', self.config.ui.history_size),
                prompt_style=ui_data.get('prompt_style', self.config.ui.prompt_style),
            )

    def _load_from_env(self):
        """从环境变量加载配置"""
        # 数据库配置
        db_path = os.environ.get('DATABASE_PATH')
        if db_path:
            self.config.database.path = db_path

        # API配置
        api_key = os.environ.get('DEEPSEEK_API_KEY')
        if api_key:
            self.config.api.api_key = api_key

        api_model = os.environ.get('DEEPSEEK_API_MODEL')
        if api_model:
            self.config.api.model = api_model

        # 日志配置
        log_level = os.environ.get('LOG_LEVEL')
        if log_level:
            self.config.logging.level = log_level

        # 查询配置
        enable_cache = os.environ.get('ENABLE_QUERY_CACHE')
        if enable_cache:
            self.config.query.enable_cache = enable_cache.lower() in ['true', '1', 'yes']

    def _validate_config(self):
        """验证配置"""
        errors = []

        # 检查API Key（警告但不阻止）
        if not self.config.api.api_key:
            logger.warning("DeepSeek API Key未设置，API功能将不可用")
            # 不将API Key缺失作为错误，只是警告

        # 检查数据库路径
        original_db_path = self.config.database.path
        logger.debug(f"原始数据库路径配置: {original_db_path}")

        # 将相对路径转换为绝对路径
        db_path = Path(original_db_path)

        # 如果是相对路径，尝试基于配置文件位置解析
        if not db_path.is_absolute():
            if self.config_file:
                # 相对于配置文件目录
                config_dir = Path(self.config_file).parent
                db_path = (config_dir / original_db_path).resolve()
                logger.debug(f"相对于配置文件解析的路径: {db_path} (存在: {db_path.exists()})")
            else:
                # 相对于当前工作目录
                db_path = (Path.cwd() / original_db_path).resolve()
                logger.debug(f"相对于工作目录解析的路径: {db_path} (存在: {db_path.exists()})")

        # 调试：打印路径信息
        # print(f"DEBUG: 检查数据库文件: {db_path}")
        # print(f"DEBUG: 文件是否存在: {db_path.exists()}")
        # print(f"DEBUG: 原始路径: {original_db_path}")

        logger.info(f"检查数据库文件: {db_path}")
        logger.info(f"文件是否存在: {db_path.exists()}")

        # 检查数据库文件（宽松检查，只记录警告不阻止启动）
        try:
            if db_path.exists():
                self.config.database.path = str(db_path)
                logger.info(f"找到数据库文件: {self.config.database.path}")
            else:
                # 文件不存在，只记录警告，不阻止启动
                logger.warning(f"数据库文件可能不存在或路径编码问题: {db_path}")
                logger.warning(f"原始配置路径: {original_db_path}")
                # 仍然设置路径，让后续代码处理连接失败
                self.config.database.path = str(db_path)
        except Exception as e:
            logger.warning(f"检查数据库文件时出错（继续启动）: {e}")
            self.config.database.path = str(db_path)

        if errors:
            error_msg = "配置验证失败:\n" + "\n".join(f"  - {error}" for error in errors)
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info("配置验证通过")

    def save(self, config_file: str, format: str = 'json'):
        """保存配置到文件"""
        data = asdict(self.config)

        try:
            if format.lower() == 'json':
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            elif format.lower() in ['yaml', 'yml']:
                with open(config_file, 'w', encoding='utf-8') as f:
                    yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
            else:
                raise ValueError(f"不支持的格式: {format}")

            logger.info(f"配置已保存到: {config_file}")
        except Exception as e:
            logger.error(f"保存配置失败: {e}")
            raise

    def print_summary(self):
        """打印配置摘要"""
        summary = [
            "=" * 50,
            "配置摘要",
            "=" * 50,
            f"数据库路径: {self.config.database.path}",
            f"DeepSeek模型: {self.config.api.model}",
            f"API Key已设置: {'是' if self.config.api.api_key else '否'}",
            f"查询缓存: {'启用' if self.config.query.enable_cache else '禁用'}",
            f"SQL注入检查: {'启用' if self.config.security.enable_sql_injection_check else '禁用'}",
            f"日志级别: {self.config.logging.level}",
            f"美化输出: {'启用' if self.config.ui.enable_rich_output else '禁用'}",
            "=" * 50,
        ]

        print("\n".join(summary))


# 全局配置实例
_config_loader: Optional[ConfigLoader] = None


def get_config_loader(config_file: Optional[str] = None) -> ConfigLoader:
    """获取配置加载器实例"""
    global _config_loader

    # 如果提供了配置文件路径，或者还没有创建加载器，则创建新的加载器
    if _config_loader is None or (config_file is not None and _config_loader.config_file != config_file):
        _config_loader = ConfigLoader(config_file)

    return _config_loader


def load_config(config_file: Optional[str] = None) -> SystemConfig:
    """加载配置"""
    import os
    from pathlib import Path

    # 如果未指定配置文件，自动查找
    if config_file is None:
        # 查找可能的配置文件
        possible_files = [
            "config.yaml",
            "config.yml",
            "config.json",
            "config/config.yaml",
            "config/config.yml",
            "config/config.json"
        ]

        for file in possible_files:
            if Path(file).exists():
                config_file = file
                logger.info(f"自动发现配置文件: {config_file}")
                break

    loader = get_config_loader(config_file)
    return loader.load()


def save_config(config_file: str, format: str = 'json'):
    """保存配置"""
    loader = get_config_loader()
    loader.save(config_file, format)


def print_config_summary():
    """打印配置摘要"""
    loader = get_config_loader()
    loader.print_summary()


# 导出
__all__ = [
    "DatabaseConfig",
    "APIConfig",
    "QueryConfig",
    "SecurityConfig",
    "LoggingConfig",
    "UIConfig",
    "SystemConfig",
    "ConfigLoader",
    "get_config_loader",
    "load_config",
    "save_config",
    "print_config_summary",
]