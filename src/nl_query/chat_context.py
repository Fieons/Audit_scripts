"""
聊天上下文管理模块
管理聊天会话状态，同步查询状态到聊天上下文
"""

import logging
from typing import Dict, Any, List, Optional
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class ChatContext:
    """聊天上下文管理类"""

    def __init__(self):
        """初始化聊天上下文"""
        # 查询相关上下文
        self.natural_language_query: str = ""
        self.generated_sql: str = ""
        self.query_result_summary: str = ""
        self.query_execution_time: float = 0.0
        self.query_result_shape: Dict[str, int] = {"rows": 0, "columns": 0}

        # 讨论历史
        self.discussion_history: List[Dict[str, Any]] = []

        # 元数据
        self.created_at: datetime = datetime.now()
        self.last_updated: datetime = self.created_at
        self.context_id: str = self._generate_context_id()

    def update_query_context(self, natural_language: str, sql: str,
                           result_summary: str = "", execution_time: float = 0.0,
                           result_shape: Optional[Dict[str, int]] = None):
        """
        更新查询上下文

        Args:
            natural_language: 自然语言查询
            sql: 生成的SQL语句
            result_summary: 查询结果摘要
            execution_time: 查询执行时间
            result_shape: 结果形状（行数、列数）
        """
        self.natural_language_query = natural_language
        self.generated_sql = sql
        self.query_result_summary = result_summary
        self.query_execution_time = execution_time

        if result_shape:
            self.query_result_shape = result_shape
        else:
            self.query_result_shape = {"rows": 0, "columns": 0}

        self.last_updated = datetime.now()
        logger.info(f"查询上下文已更新: {natural_language[:50]}...")

    def add_discussion_message(self, role: str, content: str, metadata: Optional[Dict[str, Any]] = None):
        """
        添加讨论消息

        Args:
            role: 角色（user/assistant）
            content: 消息内容
            metadata: 附加元数据
        """
        message = {
            "role": role,
            "content": content,
            "timestamp": datetime.now().isoformat(),
            "message_id": f"msg_{len(self.discussion_history) + 1:04d}"
        }

        if metadata:
            message.update(metadata)

        self.discussion_history.append(message)
        self.last_updated = datetime.now()

        logger.debug(f"讨论消息已添加: {role} - {content[:50]}...")

    def get_context_summary(self, max_length: int = 1000) -> str:
        """
        获取上下文摘要

        Args:
            max_length: 摘要最大长度

        Returns:
            上下文摘要字符串
        """
        summary_parts = []

        # 查询信息
        if self.natural_language_query:
            summary_parts.append(f"原始查询需求：{self.natural_language_query}")

        if self.generated_sql:
            # 智能SQL显示：完整显示但限制长度
            if len(self.generated_sql) <= 300:
                sql_display = self.generated_sql
            else:
                # 显示前200字符和后100字符
                sql_display = self.generated_sql[:200] + "......" + self.generated_sql[-100:]
            summary_parts.append(f"生成的SQL：\n```sql\n{sql_display}\n```")

        if self.query_result_summary:
            summary_parts.append(f"查询结果摘要：{self.query_result_summary}")

        if self.query_result_shape["rows"] > 0:
            summary_parts.append(f"结果形状：{self.query_result_shape['rows']}行 × {self.query_result_shape['columns']}列")

        if self.query_execution_time > 0:
            summary_parts.append(f"查询耗时：{self.query_execution_time:.2f}秒")

        # 讨论历史摘要
        if self.discussion_history:
            recent_messages = self.discussion_history[-3:]  # 最近3条消息
            discussion_summary = []
            for msg in recent_messages:
                role = "用户" if msg["role"] == "user" else "助手"
                content_preview = msg["content"][:100] + "..." if len(msg["content"]) > 100 else msg["content"]
                discussion_summary.append(f"{role}: {content_preview}")

            if discussion_summary:
                summary_parts.append(f"最近讨论：\n" + "\n".join(discussion_summary))

        summary = "\n".join(summary_parts)

        # 限制长度
        if len(summary) > max_length:
            # 优先保留SQL和查询需求
            if "生成的SQL：" in summary:
                sql_start = summary.find("生成的SQL：")
                sql_end = summary.find("\n```", sql_start + 6) + 4
                if sql_end > sql_start:
                    sql_part = summary[sql_start:sql_end]
                    # 保留SQL部分，截断其他部分
                    other_parts = summary[:sql_start] + summary[sql_end:]
                    if len(other_parts) > max_length - len(sql_part):
                        other_parts = other_parts[:max_length - len(sql_part) - 3] + "..."
                    summary = other_parts[:sql_start] + sql_part + other_parts[sql_start:]
            else:
                summary = summary[:max_length] + "..."

        return summary

    def get_discussion_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        获取讨论历史

        Args:
            limit: 限制返回的消息数量（None表示返回所有）

        Returns:
            讨论历史列表
        """
        if limit is None:
            return self.discussion_history.copy()
        else:
            return self.discussion_history[-limit:]

    def get_recent_discussion(self, last_n: int = 5) -> str:
        """
        获取最近讨论内容

        Args:
            last_n: 获取最近N条消息

        Returns:
            格式化后的讨论内容
        """
        recent_messages = self.discussion_history[-last_n:] if self.discussion_history else []

        if not recent_messages:
            return "暂无讨论"

        formatted = []
        for msg in recent_messages:
            role = "用户" if msg["role"] == "user" else "助手"
            timestamp = msg.get("timestamp", "")
            if timestamp:
                # 简化时间显示
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    time_str = dt.strftime("%H:%M")
                except:
                    time_str = ""
                formatted.append(f"[{time_str}] {role}: {msg['content']}")
            else:
                formatted.append(f"{role}: {msg['content']}")

        return "\n".join(formatted)

    def reset(self):
        """重置上下文（清空讨论历史，保留查询信息）"""
        old_history_len = len(self.discussion_history)
        self.discussion_history = []
        self.last_updated = datetime.now()
        logger.info(f"上下文已重置，清除了{old_history_len}条讨论历史")

    def clear_all(self):
        """清空所有上下文"""
        self.natural_language_query = ""
        self.generated_sql = ""
        self.query_result_summary = ""
        self.query_execution_time = 0.0
        self.query_result_shape = {"rows": 0, "columns": 0}
        self.discussion_history = []
        self.last_updated = datetime.now()
        logger.info("所有上下文已清空")

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "context_id": self.context_id,
            "natural_language_query": self.natural_language_query,
            "generated_sql": self.generated_sql,
            "query_result_summary": self.query_result_summary,
            "query_execution_time": self.query_execution_time,
            "query_result_shape": self.query_result_shape,
            "discussion_history": self.discussion_history.copy(),
            "created_at": self.created_at.isoformat(),
            "last_updated": self.last_updated.isoformat(),
            "discussion_count": len(self.discussion_history)
        }

    def _generate_context_id(self) -> str:
        """生成上下文ID"""
        import hashlib
        import uuid

        unique_str = f"{datetime.now().isoformat()}_{uuid.uuid4()}"
        return hashlib.md5(unique_str.encode()).hexdigest()[:8]

    def __str__(self) -> str:
        """字符串表示"""
        return f"ChatContext(id={self.context_id}, query='{self.natural_language_query[:30]}...', discussions={len(self.discussion_history)})"

if __name__ == "__main__":
    # 测试聊天上下文
    import logging
    logging.basicConfig(level=logging.INFO)

    print("测试聊天上下文...")

    context = ChatContext()
    print(f"✓ 上下文创建成功: {context}")

    # 测试更新查询上下文
    context.update_query_context(
        natural_language="查询2024年所有公司的管理费用",
        sql="SELECT company_name, SUM(amount) as 管理费用 FROM expenses WHERE expense_type = '管理费用' AND year = 2024 GROUP BY company_name;",
        result_summary="共找到10家公司，管理费用总额为1,234,567元",
        execution_time=2.5,
        result_shape={"rows": 10, "columns": 2}
    )

    print(f"✓ 查询上下文更新成功")

    # 测试添加讨论消息
    context.add_discussion_message("user", "这个查询结果是什么意思？")
    context.add_discussion_message("assistant", "这个查询显示了2024年各公司的管理费用汇总情况...")
    context.add_discussion_message("user", "能解释一下SQL中的GROUP BY吗？")

    print(f"✓ 讨论消息添加成功，当前消息数: {len(context.discussion_history)}")

    # 测试获取上下文摘要
    summary = context.get_context_summary()
    print(f"\n上下文摘要:\n{summary}")

    # 测试获取最近讨论
    recent = context.get_recent_discussion(2)
    print(f"\n最近讨论:\n{recent}")

    # 测试转换为字典
    context_dict = context.to_dict()
    print(f"\n字典格式:")
    for key in ["context_id", "natural_language_query", "discussion_count"]:
        print(f"  {key}: {context_dict[key]}")

    # 测试重置
    context.reset()
    print(f"\n✓ 上下文重置成功，剩余消息数: {len(context.discussion_history)}")

    # 测试清空所有
    context.clear_all()
    print(f"✓ 所有上下文清空成功")