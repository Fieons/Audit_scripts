"""
聊天专用客户端
继承BaseLLMClient，处理多轮对话和上下文管理
"""

import logging
from typing import Dict, Any, List, Optional
import time

from .base import BaseLLMClient, LLMError
from ..config import DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL, DEEPSEEK_MODEL, CHAT_MAX_HISTORY, CHAT_TEMPERATURE

logger = logging.getLogger(__name__)

class ChatError(LLMError):
    """聊天相关错误"""
    pass

class ChatClient(BaseLLMClient):
    """聊天专用客户端，支持多轮对话和上下文管理"""

    def __init__(self, api_key: str = None, base_url: str = None, model: str = None,
                 max_history: int = None, temperature: float = None):
        """
        初始化聊天客户端

        Args:
            api_key: API密钥
            base_url: API基础URL
            model: 模型名称
            max_history: 最大对话历史轮数
            temperature: 聊天温度
        """
        super().__init__(
            api_key=api_key or DEEPSEEK_API_KEY,
            base_url=base_url or DEEPSEEK_BASE_URL,
            model=model or DEEPSEEK_MODEL
        )

        self.max_history = max_history or CHAT_MAX_HISTORY
        self.chat_temperature = temperature or CHAT_TEMPERATURE
        self.conversation_history: List[Dict[str, str]] = []

    def send_message(self, user_message: str, context_info: Optional[str] = None) -> str:
        """
        发送消息并获取AI回复

        Args:
            user_message: 用户消息
            context_info: 上下文信息（如查询需求、SQL、结果等）

        Returns:
            AI回复内容
        """
        start_time = time.time()

        try:
            # 构建系统提示词
            system_prompt = self._build_system_prompt(context_info)

            # 添加用户消息到历史
            self.conversation_history.append({"role": "user", "content": user_message})

            # 准备消息列表（系统提示词 + 最近的历史消息）
            messages = [{"role": "system", "content": system_prompt}]
            messages.extend(self.conversation_history[-self.max_history*2:])  # 保留最近N轮对话

            # 调用API
            response = self.call_api(
                messages=messages,
                temperature=self.chat_temperature,
                max_tokens=1000
            )

            # 提取回复内容
            assistant_message = self._extract_content_from_response(response)

            # 添加AI回复到历史
            self.conversation_history.append({"role": "assistant", "content": assistant_message})

            # 记录日志
            elapsed_time = time.time() - start_time
            logger.info(f"聊天消息处理成功: 耗时{elapsed_time:.2f}秒, 历史长度: {len(self.conversation_history)}")

            return assistant_message

        except Exception as e:
            logger.error(f"聊天消息处理失败: {e}")
            raise ChatError(f"聊天消息处理失败: {e}")

    def _build_system_prompt(self, context_info: Optional[str] = None) -> str:
        """构建聊天系统提示词"""
        base_prompt = """你是一个专业的审计数据分析助手，帮助用户理解和分析查询结果。

重要：你必须基于提供的"当前查询上下文"来回答问题。上下文包含了用户执行的查询的所有相关信息。

你的角色：
1. 基于上下文中的SQL语句，解释查询的逻辑和数据关系
2. 基于上下文中的查询结果，帮助用户理解数据的含义和业务价值
3. 基于上下文提供数据分析建议
4. 回答用户关于上下文中的数据的问题
5. 协助用户思考可能的查询优化方向（基于上下文中的SQL）

关键要求：
1. 必须使用上下文中的信息来回答问题
2. 如果用户的问题涉及SQL，请引用上下文中的SQL语句
3. 如果用户的问题涉及查询结果，请引用上下文中的结果摘要
4. 用清晰易懂的语言解释技术概念
5. 如果用户询问如何修改查询，提供思路但不要直接生成SQL
6. 保持专业、友好的态度
7. 支持Markdown格式，对于SQL代码使用```sql代码块```

请根据用户的讨论内容和提供的查询上下文提供有帮助的回复。"""

        if context_info:
            return f"{base_prompt}\n\n当前查询上下文：\n{context_info}\n\n请基于以上上下文信息回答用户的问题。"
        else:
            return base_prompt + "\n\n注意：当前没有查询上下文信息，请告知用户需要先执行查询并点击'讨论此查询'按钮。"

    def clear_history(self):
        """清空对话历史"""
        self.conversation_history = []
        logger.info("对话历史已清空")

    def get_history(self) -> List[Dict[str, str]]:
        """获取对话历史"""
        return self.conversation_history.copy()

    def get_history_summary(self) -> str:
        """获取对话历史摘要"""
        if not self.conversation_history:
            return "暂无对话历史"

        summary = []
        for i, msg in enumerate(self.conversation_history[-5:], 1):  # 最近5条
            role = "用户" if msg["role"] == "user" else "助手"
            content_preview = msg["content"][:50] + "..." if len(msg["content"]) > 50 else msg["content"]
            summary.append(f"{i}. {role}: {content_preview}")

        return "\n".join(summary)

    def get_context_for_prompt(self) -> str:
        """获取当前对话上下文用于提示词"""
        if not self.conversation_history:
            return "暂无对话历史"

        context_lines = []
        for msg in self.conversation_history[-self.max_history*2:]:  # 保留最近N轮
            role = "用户" if msg["role"] == "user" else "助手"
            context_lines.append(f"{role}: {msg['content']}")

        return "\n".join(context_lines)

if __name__ == "__main__":
    # 测试聊天客户端
    import logging
    logging.basicConfig(level=logging.INFO)

    print("测试聊天客户端...")

    client = ChatClient(max_history=5, temperature=0.7)

    # 测试连接
    if client.test_connection():
        print("✓ API连接测试成功")

        # 测试聊天功能
        test_context = """
        当前查询上下文：
        1. 原始查询需求：查询2024年所有公司的管理费用
        2. 生成的SQL：SELECT company_name, SUM(amount) as 管理费用 FROM expenses WHERE expense_type = '管理费用' AND year = 2024 GROUP BY company_name;
        3. 查询结果摘要：共找到10家公司，管理费用总额为1,234,567元
        """

        try:
            # 第一轮对话
            print("\n测试第一轮对话...")
            response1 = client.send_message("这个查询结果是什么意思？", test_context)
            print(f"✓ AI回复: {response1[:100]}...")

            # 第二轮对话
            print("\n测试第二轮对话...")
            response2 = client.send_message("能解释一下SQL中的GROUP BY是什么意思吗？")
            print(f"✓ AI回复: {response2[:100]}...")

            # 显示历史
            print(f"\n对话历史长度: {len(client.get_history())}")
            print("历史摘要:")
            print(client.get_history_summary())

            # 显示统计信息
            stats = client.get_stats()
            print(f"\n统计信息:")
            for key, value in stats.items():
                print(f"  {key}: {value}")

        except Exception as e:
            print(f"✗ 聊天测试失败: {e}")

        # 测试清空历史
        print("\n测试清空历史...")
        client.clear_history()
        print(f"✓ 历史已清空，当前长度: {len(client.get_history())}")

    else:
        print("✗ API连接测试失败")