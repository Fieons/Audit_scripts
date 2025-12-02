"""
DeepSeek API客户端

提供与DeepSeek API的交互功能，支持聊天补全、流式响应和错误处理。
"""

import json
import time
import hashlib
from typing import Dict, List, Any, Optional, Generator, Union
from dataclasses import dataclass, field, asdict
import logging
from enum import Enum

import requests
from requests.exceptions import RequestException, Timeout

from ..utils.string_utils import safe_string

logger = logging.getLogger(__name__)


class ModelType(Enum):
    """DeepSeek模型类型"""
    DEEPSEEK_CHAT = "deepseek-chat"
    DEEPSEEK_CODER = "deepseek-coder"
    DEEPSEEK_REASONER = "deepseek-reasoner"


class MessageRole(Enum):
    """消息角色"""
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"


@dataclass
class Message:
    """消息"""
    role: MessageRole
    content: str

    def to_dict(self) -> Dict[str, str]:
        """转换为字典"""
        return {
            "role": self.role.value,
            "content": safe_string(self.content)
        }


@dataclass
class ChatCompletionRequest:
    """聊天补全请求"""
    messages: List[Message]
    model: str = ModelType.DEEPSEEK_CHAT.value
    temperature: float = 0.7
    max_tokens: Optional[int] = None
    top_p: float = 1.0
    frequency_penalty: float = 0.0
    presence_penalty: float = 0.0
    stream: bool = False
    response_format: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = {
            "model": self.model,
            "messages": [msg.to_dict() for msg in self.messages],
            "temperature": self.temperature,
            "top_p": self.top_p,
            "frequency_penalty": self.frequency_penalty,
            "presence_penalty": self.presence_penalty,
            "stream": self.stream
        }

        if self.max_tokens is not None:
            data["max_tokens"] = self.max_tokens

        if self.response_format is not None:
            data["response_format"] = self.response_format

        return data


@dataclass
class ChatCompletionChoice:
    """聊天补全选择"""
    index: int
    message: Message
    finish_reason: str
    logprobs: Optional[Dict[str, Any]] = None


@dataclass
class ChatCompletionUsage:
    """使用情况统计"""
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


@dataclass
class ChatCompletionResponse:
    """聊天补全响应"""
    id: str
    object: str
    created: int
    model: str
    choices: List[ChatCompletionChoice]
    usage: ChatCompletionUsage

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ChatCompletionResponse":
        """从字典创建"""
        choices = []
        for choice_data in data.get("choices", []):
            message_data = choice_data.get("message", {})
            message = Message(
                role=MessageRole(message_data.get("role", "assistant")),
                content=message_data.get("content", "")
            )
            choice = ChatCompletionChoice(
                index=choice_data.get("index", 0),
                message=message,
                finish_reason=choice_data.get("finish_reason", "stop"),
                logprobs=choice_data.get("logprobs")
            )
            choices.append(choice)

        usage_data = data.get("usage", {})
        usage = ChatCompletionUsage(
            prompt_tokens=usage_data.get("prompt_tokens", 0),
            completion_tokens=usage_data.get("completion_tokens", 0),
            total_tokens=usage_data.get("total_tokens", 0)
        )

        return cls(
            id=data.get("id", ""),
            object=data.get("object", "chat.completion"),
            created=data.get("created", int(time.time())),
            model=data.get("model", ModelType.DEEPSEEK_CHAT.value),
            choices=choices,
            usage=usage
        )

    def get_content(self) -> str:
        """获取响应内容"""
        if self.choices:
            return self.choices[0].message.content
        return ""

    def get_first_choice(self) -> Optional[ChatCompletionChoice]:
        """获取第一个选择"""
        return self.choices[0] if self.choices else None


@dataclass
class StreamDelta:
    """流式响应增量"""
    content: Optional[str] = None
    role: Optional[str] = None


@dataclass
class StreamChoice:
    """流式选择"""
    index: int
    delta: StreamDelta
    finish_reason: Optional[str] = None


@dataclass
class StreamResponse:
    """流式响应"""
    id: str
    object: str
    created: int
    model: str
    choices: List[StreamChoice]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StreamResponse":
        """从字典创建"""
        choices = []
        for choice_data in data.get("choices", []):
            delta_data = choice_data.get("delta", {})
            delta = StreamDelta(
                content=delta_data.get("content"),
                role=delta_data.get("role")
            )
            choice = StreamChoice(
                index=choice_data.get("index", 0),
                delta=delta,
                finish_reason=choice_data.get("finish_reason")
            )
            choices.append(choice)

        return cls(
            id=data.get("id", ""),
            object=data.get("object", "chat.completion.chunk"),
            created=data.get("created", int(time.time())),
            model=data.get("model", ModelType.DEEPSEEK_CHAT.value),
            choices=choices
        )


class DeepSeekClient:
    """DeepSeek API客户端"""

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.deepseek.com",
        timeout: int = 30,
        max_retries: int = 3,
        request_interval: float = 1.0
    ):
        """
        初始化DeepSeek客户端

        Args:
            api_key: API密钥
            base_url: API基础URL
            timeout: 请求超时时间（秒）
            max_retries: 最大重试次数
            request_interval: 请求间隔（秒）
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.request_interval = request_interval

        # 会话管理
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

        # 统计信息
        self.total_requests = 0
        self.total_tokens = 0
        self.total_errors = 0

        logger.info(f"DeepSeek客户端初始化完成，基础URL: {self.base_url}")

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        stream: bool = False
    ) -> requests.Response:
        """
        发送HTTP请求

        Args:
            method: HTTP方法
            endpoint: API端点
            data: 请求数据
            stream: 是否流式响应

        Returns:
            requests.Response: 响应对象

        Raises:
            RequestException: 请求失败
        """
        url = f"{self.base_url}{endpoint}"

        for attempt in range(self.max_retries):
            try:
                self.total_requests += 1

                kwargs = {
                    "timeout": self.timeout,
                    "stream": stream
                }

                if data is not None:
                    kwargs["json"] = data

                response = self.session.request(method, url, **kwargs)

                # 检查响应状态
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    # 速率限制
                    retry_after = int(response.headers.get("Retry-After", 5))
                    logger.warning(f"速率限制，等待 {retry_after} 秒后重试...")
                    time.sleep(retry_after)
                    continue
                elif response.status_code >= 500:
                    # 服务器错误
                    logger.warning(f"服务器错误 ({response.status_code})，尝试重试...")
                    time.sleep(self.request_interval)
                    continue
                else:
                    # 其他错误
                    error_msg = self._parse_error_response(response)
                    raise RequestException(f"API请求失败 ({response.status_code}): {error_msg}")

            except Timeout:
                logger.warning(f"请求超时，尝试 {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.request_interval)
                    continue
                raise
            except RequestException as e:
                logger.error(f"请求异常: {e}")
                self.total_errors += 1
                if attempt < self.max_retries - 1:
                    time.sleep(self.request_interval)
                    continue
                raise

        raise RequestException(f"请求失败，已达到最大重试次数 {self.max_retries}")

    def _parse_error_response(self, response: requests.Response) -> str:
        """解析错误响应"""
        try:
            error_data = response.json()
            if "error" in error_data:
                error_info = error_data["error"]
                if isinstance(error_info, dict):
                    return error_info.get("message", str(error_info))
                return str(error_info)
            return response.text
        except:
            return response.text

    def chat_completion(
        self,
        messages: List[Message],
        model: str = ModelType.DEEPSEEK_CHAT.value,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        stream: bool = False,
        **kwargs
    ) -> Union[ChatCompletionResponse, Generator[str, None, None]]:
        """
        聊天补全

        Args:
            messages: 消息列表
            model: 模型名称
            temperature: 温度参数
            max_tokens: 最大token数
            stream: 是否流式响应
            **kwargs: 其他参数

        Returns:
            Union[ChatCompletionResponse, Generator[str, None, None]]: 响应结果或流式生成器

        Raises:
            RequestException: 请求失败
        """
        request = ChatCompletionRequest(
            messages=messages,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=stream,
            **kwargs
        )

        logger.debug(f"发送聊天补全请求，模型: {model}, 消息数: {len(messages)}")

        if stream:
            return self._stream_chat_completion(request)
        else:
            return self._non_stream_chat_completion(request)

    def _non_stream_chat_completion(self, request: ChatCompletionRequest) -> ChatCompletionResponse:
        """非流式聊天补全"""
        response = self._make_request(
            "POST",
            "/chat/completions",
            data=request.to_dict(),
            stream=False
        )

        response_data = response.json()
        completion = ChatCompletionResponse.from_dict(response_data)

        # 更新统计
        self.total_tokens += completion.usage.total_tokens

        logger.info(
            f"聊天补全完成，使用token: {completion.usage.total_tokens} "
            f"(提示: {completion.usage.prompt_tokens}, 补全: {completion.usage.completion_tokens})"
        )

        return completion

    def _stream_chat_completion(self, request: ChatCompletionRequest) -> Generator[str, None, None]:
        """流式聊天补全"""
        response = self._make_request(
            "POST",
            "/chat/completions",
            data=request.to_dict(),
            stream=True
        )

        full_content = ""

        for line in response.iter_lines():
            if line:
                line = line.decode("utf-8").strip()

                # 跳过心跳消息
                if line.startswith("data: "):
                    data = line[6:]

                    # 结束标记
                    if data == "[DONE]":
                        break

                    try:
                        chunk_data = json.loads(data)
                        stream_response = StreamResponse.from_dict(chunk_data)

                        for choice in stream_response.choices:
                            if choice.delta.content:
                                full_content += choice.delta.content
                                yield choice.delta.content
                    except json.JSONDecodeError:
                        logger.warning(f"解析流式响应失败: {data}")

        logger.info(f"流式聊天补全完成，总长度: {len(full_content)} 字符")

    def create_system_message(self, content: str) -> Message:
        """创建系统消息"""
        return Message(role=MessageRole.SYSTEM, content=content)

    def create_user_message(self, content: str) -> Message:
        """创建用户消息"""
        return Message(role=MessageRole.USER, content=content)

    def create_assistant_message(self, content: str) -> Message:
        """创建助手消息"""
        return Message(role=MessageRole.ASSISTANT, content=content)

    def simple_chat(
        self,
        user_input: str,
        system_prompt: Optional[str] = None,
        model: str = ModelType.DEEPSEEK_CHAT.value,
        temperature: float = 0.7
    ) -> str:
        """
        简单聊天接口

        Args:
            user_input: 用户输入
            system_prompt: 系统提示词
            model: 模型名称
            temperature: 温度参数

        Returns:
            str: 助手回复

        Raises:
            RequestException: 请求失败
        """
        messages = []

        if system_prompt:
            messages.append(self.create_system_message(system_prompt))

        messages.append(self.create_user_message(user_input))

        response = self.chat_completion(
            messages=messages,
            model=model,
            temperature=temperature,
            stream=False
        )

        return response.get_content()

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "total_requests": self.total_requests,
            "total_tokens": self.total_tokens,
            "total_errors": self.total_errors,
            "avg_tokens_per_request": self.total_tokens / self.total_requests if self.total_requests > 0 else 0
        }

    def close(self):
        """关闭客户端"""
        self.session.close()
        logger.info("DeepSeek客户端已关闭")

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()


# 全局客户端实例
_deepseek_client: Optional[DeepSeekClient] = None


def get_deepseek_client() -> DeepSeekClient:
    """获取DeepSeek客户端实例"""
    global _deepseek_client

    if _deepseek_client is None:
        from ..utils.config_loader import load_config
        config = load_config()

        if not config.api.api_key:
            raise ValueError("DeepSeek API Key未设置，请在配置文件中设置或通过环境变量设置")

        _deepseek_client = DeepSeekClient(
            api_key=config.api.api_key,
            base_url=config.api.base_url,
            timeout=config.api.timeout,
            max_retries=config.api.max_retries,
            request_interval=config.api.request_interval
        )

    return _deepseek_client


def close_deepseek_client():
    """关闭DeepSeek客户端"""
    global _deepseek_client
    if _deepseek_client is not None:
        _deepseek_client.close()
        _deepseek_client = None


# 导出
__all__ = [
    "ModelType",
    "MessageRole",
    "Message",
    "ChatCompletionRequest",
    "ChatCompletionResponse",
    "StreamResponse",
    "DeepSeekClient",
    "get_deepseek_client",
    "close_deepseek_client",
]