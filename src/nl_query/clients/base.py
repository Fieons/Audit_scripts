"""
基础LLM客户端模块
提供通用的LLM API调用功能，作为SQL生成和聊天客户端的基类
"""

import logging
import time
from typing import Optional, Dict, Any, List
from openai import OpenAI
from openai.types.chat import ChatCompletion

logger = logging.getLogger(__name__)

class LLMError(Exception):
    """LLM相关错误基类"""
    pass

class BaseLLMClient:
    """基础LLM客户端，提供通用的API调用功能"""

    def __init__(self, api_key: str, base_url: str, model: str):
        """
        初始化LLM客户端

        Args:
            api_key: API密钥
            base_url: API基础URL
            model: 模型名称
        """
        self.api_key = api_key
        self.base_url = base_url
        self.model = model

        # 初始化OpenAI客户端
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )

        # 请求统计
        self.request_count = 0
        self.total_tokens = 0
        self.total_time = 0.0

    def call_api(self, messages: List[Dict[str, str]],
                temperature: float = 0.1,
                max_tokens: int = 1000,
                stream: bool = False) -> ChatCompletion:
        """
        调用LLM API

        Args:
            messages: 消息列表，格式为[{"role": "system/user/assistant", "content": "..."}]
            temperature: 生成温度（0-1）
            max_tokens: 最大token数
            stream: 是否流式输出

        Returns:
            API响应
        """
        start_time = time.time()

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                stream=stream
            )

            # 记录统计信息
            elapsed_time = time.time() - start_time
            self.request_count += 1
            self.total_time += elapsed_time

            if response.usage:
                self.total_tokens += response.usage.total_tokens

            logger.debug(f"API调用成功: {elapsed_time:.2f}秒, tokens: {response.usage.total_tokens if response.usage else 'N/A'}")
            return response

        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"API调用失败: {e}, 耗时: {elapsed_time:.2f}秒")
            raise LLMError(f"API调用失败: {e}")

    def test_connection(self) -> bool:
        """
        测试API连接

        Returns:
            连接是否成功
        """
        try:
            # 发送一个简单的测试请求
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": "Hello"}],
                max_tokens=10
            )
            return response.choices[0].message.content is not None
        except Exception as e:
            logger.error(f"API连接测试失败: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        """
        获取客户端统计信息

        Returns:
            统计信息字典
        """
        return {
            "request_count": self.request_count,
            "total_tokens": self.total_tokens,
            "total_time": self.total_time,
            "avg_time_per_request": self.total_time / self.request_count if self.request_count > 0 else 0,
            "avg_tokens_per_request": self.total_tokens / self.request_count if self.request_count > 0 else 0
        }

    def reset_stats(self):
        """重置统计信息"""
        self.request_count = 0
        self.total_tokens = 0
        self.total_time = 0.0

    def _extract_content_from_response(self, response: ChatCompletion) -> str:
        """
        从API响应中提取内容

        Args:
            response: API响应

        Returns:
            提取的内容
        """
        if not response.choices or not response.choices[0].message.content:
            raise LLMError("API响应为空")

        return response.choices[0].message.content.strip()

if __name__ == "__main__":
    # 测试基础客户端
    import logging
    logging.basicConfig(level=logging.INFO)

    from .config import DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL, DEEPSEEK_MODEL

    print("测试基础LLM客户端...")

    client = BaseLLMClient(
        api_key=DEEPSEEK_API_KEY,
        base_url=DEEPSEEK_BASE_URL,
        model=DEEPSEEK_MODEL
    )

    # 测试连接
    if client.test_connection():
        print("✓ API连接测试成功")

        # 测试API调用
        try:
            response = client.call_api(
                messages=[
                    {"role": "system", "content": "你是一个助手"},
                    {"role": "user", "content": "你好"}
                ],
                temperature=0.1,
                max_tokens=50
            )

            content = client._extract_content_from_response(response)
            print(f"✓ API调用测试成功: {content[:50]}...")

            # 显示统计信息
            stats = client.get_stats()
            print(f"\n统计信息:")
            for key, value in stats.items():
                print(f"  {key}: {value}")

        except Exception as e:
            print(f"✗ API调用测试失败: {e}")
    else:
        print("✗ API连接测试失败")