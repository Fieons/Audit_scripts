"""
安全模块

提供数据脱敏、输入验证等安全功能。
"""

import re
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class SecurityManager:
    """安全管理器"""

    def __init__(self, enable_data_masking: bool = True):
        """
        初始化安全管理器

        Args:
            enable_data_masking: 是否启用数据脱敏
        """
        self.enable_data_masking = enable_data_masking

    def mask_sensitive_data(self, data: Any, data_type: str = "generic") -> Any:
        """
        脱敏敏感数据

        Args:
            data: 原始数据
            data_type: 数据类型（generic, phone, email, id_card, bank_card）

        Returns:
            脱敏后的数据
        """
        if not self.enable_data_masking:
            return data

        if data is None:
            return data

        if isinstance(data, str):
            return self._mask_string(data, data_type)
        elif isinstance(data, dict):
            return self._mask_dict(data)
        elif isinstance(data, list):
            return [self.mask_sensitive_data(item, data_type) for item in data]
        else:
            return data

    def _mask_string(self, text: str, data_type: str) -> str:
        """脱敏字符串"""
        if not text or len(text) < 2:
            return text

        if data_type == "phone":
            # 手机号：保留前3后4
            if len(text) >= 11:
                return f"{text[:3]}****{text[-4:]}"
        elif data_type == "email":
            # 邮箱：保留@前第一个字符和域名
            if "@" in text:
                local_part, domain = text.split("@", 1)
                if len(local_part) > 1:
                    return f"{local_part[0]}***@{domain}"
        elif data_type == "id_card":
            # 身份证：保留前6后4
            if len(text) >= 15:
                return f"{text[:6]}********{text[-4:]}"
        elif data_type == "bank_card":
            # 银行卡：保留前6后4
            if len(text) >= 16:
                return f"{text[:6]}******{text[-4:]}"

        # 通用脱敏：保留首尾字符
        if len(text) <= 4:
            return f"{text[0]}**{text[-1]}" if len(text) > 2 else text
        else:
            mask_length = min(4, len(text) // 2)
            return f"{text[:2]}{'*' * mask_length}{text[-2:]}"

    def _mask_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """脱敏字典数据"""
        masked_data = {}
        sensitive_keys = {
            "phone", "mobile", "tel", "telephone",
            "email", "mail",
            "id_card", "identity", "身份证",
            "bank_card", "card_no", "account_no",
            "password", "pwd", "secret", "token", "key"
        }

        for key, value in data.items():
            key_lower = str(key).lower()
            data_type = "generic"

            # 判断数据类型
            if any(sensitive in key_lower for sensitive in ["phone", "mobile", "tel"]):
                data_type = "phone"
            elif any(sensitive in key_lower for sensitive in ["email", "mail"]):
                data_type = "email"
            elif any(sensitive in key_lower for sensitive in ["id_card", "identity", "身份证"]):
                data_type = "id_card"
            elif any(sensitive in key_lower for sensitive in ["bank_card", "card_no", "account"]):
                data_type = "bank_card"
            elif any(sensitive in key_lower for sensitive in ["password", "pwd", "secret", "token", "key"]):
                if isinstance(value, str):
                    value = "***"  # 完全隐藏密码等敏感信息

            masked_data[key] = self.mask_sensitive_data(value, data_type)

        return masked_data

    def validate_input(self, input_str: str, max_length: int = 1000) -> bool:
        """
        验证输入字符串

        Args:
            input_str: 输入字符串
            max_length: 最大长度限制

        Returns:
            是否有效
        """
        if not input_str or not isinstance(input_str, str):
            return False

        # 长度检查
        if len(input_str) > max_length:
            logger.warning(f"输入长度超过限制: {len(input_str)} > {max_length}")
            return False

        # 特殊字符检查（防止注入攻击）
        dangerous_patterns = [
            r"<script.*?>.*?</script>",  # JavaScript
            r"on\w+\s*=",  # 事件处理器
            r"javascript:",  # JavaScript协议
            r"vbscript:",  # VBScript协议
            r"expression\s*\(",  # CSS表达式
        ]

        for pattern in dangerous_patterns:
            if re.search(pattern, input_str, re.IGNORECASE):
                logger.warning(f"检测到危险输入模式: {pattern}")
                return False

        return True

    def sanitize_sql_identifier(self, identifier: str) -> str:
        """
        清理SQL标识符（表名、列名）

        Args:
            identifier: 标识符

        Returns:
            清理后的标识符
        """
        if not identifier or not isinstance(identifier, str):
            return ""

        # 只允许字母、数字、下划线
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "", identifier)

        # 防止SQL注入
        if sanitized.upper() in {"SELECT", "INSERT", "UPDATE", "DELETE", "DROP",
                                 "CREATE", "ALTER", "TRUNCATE", "UNION", "OR"}:
            logger.warning(f"标识符包含SQL关键字: {sanitized}")
            return ""

        return sanitized

    def check_rate_limit(self, user_id: str, operation: str,
                         limit: int = 100, window_seconds: int = 60) -> bool:
        """
        检查速率限制（简单实现）

        Args:
            user_id: 用户ID
            operation: 操作类型
            limit: 限制次数
            window_seconds: 时间窗口（秒）

        Returns:
            是否允许操作
        """
        # 这里实现简单的速率限制逻辑
        # 实际应用中应该使用Redis等外部存储
        return True  # 暂时总是返回True

    def audit_log(self, user_id: str, operation: str, details: Dict[str, Any]):
        """
        记录审计日志

        Args:
            user_id: 用户ID
            operation: 操作类型
            details: 操作详情
        """
        log_entry = {
            "user_id": user_id,
            "operation": operation,
            "details": self.mask_sensitive_data(details),
            "timestamp": self._get_current_timestamp()
        }

        logger.info(f"审计日志: {log_entry}")

    def _get_current_timestamp(self) -> str:
        """获取当前时间戳"""
        from datetime import datetime
        return datetime.now().isoformat()


# 全局安全管理器实例
_security_manager: Optional[SecurityManager] = None


def get_security_manager(enable_data_masking: bool = True) -> SecurityManager:
    """获取安全管理器实例"""
    global _security_manager
    if _security_manager is None:
        _security_manager = SecurityManager(enable_data_masking)
    return _security_manager


def mask_sensitive_data(data: Any, data_type: str = "generic") -> Any:
    """脱敏敏感数据"""
    manager = get_security_manager()
    return manager.mask_sensitive_data(data, data_type)


def validate_input(input_str: str, max_length: int = 1000) -> bool:
    """验证输入字符串"""
    manager = get_security_manager()
    return manager.validate_input(input_str, max_length)


# 导出
__all__ = [
    "SecurityManager",
    "get_security_manager",
    "mask_sensitive_data",
    "validate_input",
]