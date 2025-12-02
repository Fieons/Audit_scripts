"""
字符串工具
提供安全的字符串处理功能，解决代理对字符等编码问题
"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def safe_string(text: str, max_length: Optional[int] = None) -> str:
    """
    安全的字符串处理，移除代理对字符等无效字符

    Args:
        text: 原始字符串
        max_length: 最大长度限制（可选）

    Returns:
        安全的字符串
    """
    if not text:
        return ""

    # 1. 移除代理对字符
    try:
        # 先尝试正常编码解码
        safe_text = text.encode('utf-8', errors='replace').decode('utf-8')
    except UnicodeError as e:
        logger.warning(f"字符串编码错误: {e}, 使用更安全的方法")
        # 使用更安全的方法
        safe_text = text.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')

    # 2. 移除控制字符（除了换行和制表符）
    safe_text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', safe_text)

    # 3. 长度限制
    if max_length and len(safe_text) > max_length:
        safe_text = safe_text[:max_length]
        logger.debug(f"字符串截断到 {max_length} 字符")

    return safe_text


def safe_encode(text: str) -> bytes:
    """
    安全的字符串编码

    Args:
        text: 原始字符串

    Returns:
        编码后的字节串
    """
    if not text:
        return b""

    try:
        # 先尝试正常编码
        return text.encode('utf-8')
    except UnicodeEncodeError as e:
        logger.warning(f"字符串编码失败: {e}, 使用安全编码")
        # 使用安全编码
        safe_text = safe_string(text)
        return safe_text.encode('utf-8')


def safe_hash(text: str) -> str:
    """
    安全的字符串哈希，用于缓存键等

    Args:
        text: 原始字符串

    Returns:
        哈希字符串
    """
    import hashlib

    safe_text = safe_string(text)
    return hashlib.md5(safe_encode(safe_text)).hexdigest()


def is_surrogate_char(char: str) -> bool:
    """
    检查字符是否是代理对字符

    Args:
        char: 单个字符

    Returns:
        是否是代理对字符
    """
    if len(char) != 1:
        return False

    code = ord(char)
    return 0xD800 <= code <= 0xDFFF


def remove_surrogates(text: str) -> str:
    """
    移除字符串中的代理对字符

    Args:
        text: 原始字符串

    Returns:
        移除代理对后的字符串
    """
    if not text:
        return ""

    result = []
    for char in text:
        if not is_surrogate_char(char):
            result.append(char)
        else:
            logger.debug(f"移除代理对字符: U+{ord(char):04X}")

    return ''.join(result)


def test_string_safety():
    """测试字符串安全性"""
    test_cases = [
        "正常字符串",
        "查询科目总数",
        "银行存款交易",
        "包含\x00控制字符\x01的字符串",
        "正常结尾"
    ]

    print("字符串安全性测试:")
    print("=" * 60)

    for test in test_cases:
        print(f"原始: {repr(test)}")
        safe = safe_string(test)
        print(f"安全: {repr(safe)}")
        print(f"长度: {len(test)} -> {len(safe)}")
        print("-" * 60)


if __name__ == "__main__":
    test_string_safety()