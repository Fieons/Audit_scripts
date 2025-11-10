#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LLM分类模块
功能：使用大语言模型对会计分录进行智能分类
作者：Claude
日期：2025-11-10
"""

import requests
from typing import Optional


class AIClassifier:
    """基于LLM的会计分类器"""

    def __init__(self, api_key: Optional[str] = None, provider: str = "deepseek"):
        """
        初始化分类器

        Args:
            api_key: API密钥
            provider: LLM提供商，支持 "deepseek", "openai" 等
        """
        self.api_key = api_key
        self.provider = provider
        self.api_config = self._get_api_config(provider)

    def _get_api_config(self, provider: str) -> dict:
        """获取API配置"""
        configs = {
            "deepseek": {
                "url": "https://api.deepseek.com/v1/chat/completions",
                "model": "deepseek-chat",
                "headers": {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                }
            },
            "openai": {
                "url": "https://api.openai.com/v1/chat/completions",
                "model": "gpt-3.5-turbo",
                "headers": {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                }
            }
        }
        return configs.get(provider, configs["deepseek"])

    def _call_llm(self, prompt: str, max_tokens: int = 50) -> Optional[str]:
        """
        调用LLM API

        Args:
            prompt: 提示词
            max_tokens: 最大令牌数

        Returns:
            Optional[str]: LLM响应结果
        """
        if not self.api_key:
            raise ValueError("LLM API密钥未配置，无法进行智能分类")

        try:
            response = requests.post(
                self.api_config["url"],
                headers=self.api_config["headers"],
                json={
                    "model": self.api_config["model"],
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": max_tokens
                },
                timeout=10
            )

            if response.status_code == 200:
                result = response.json()
                return result["choices"][0]["message"]["content"].strip()
            else:
                raise RuntimeError(f"LLM API调用失败: HTTP {response.status_code} - {response.text}")

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"网络请求失败: {e}")
        except Exception as e:
            raise RuntimeError(f"LLM调用异常: {e}")

    def classify_payment_purpose(self, summary: str, account_name: str, auxiliary_item: str) -> str:
        """
        分类款项用途

        Args:
            summary: 摘要信息
            account_name: 科目名称
            auxiliary_item: 辅助项信息

        Returns:
            str: 分类结果
        """
        prompt = f"""
请根据以下会计信息，对款项用途进行分类。请基于利润表的末级科目进行分类。

**会计信息**：
- 摘要：{summary}
- 科目名称：{account_name}
- 辅助项：{auxiliary_item}


请只返回分类名称，不要解释。如果无法确定，返回"其他"。
"""

        result = self._call_llm(prompt)
        return result if result else "其他"

    def classify_cash_flow_item(self, summary: str, account_name: str, auxiliary_item: str) -> str:
        """
        分类现金流量表项目

        Args:
            summary: 摘要信息
            account_name: 科目名称
            auxiliary_item: 辅助项信息

        Returns:
            str: 分类结果
        """
        prompt = f"""
请根据以下会计信息，对现金流量表项目进行分类。这需要会计专业知识，请根据《企业会计准则第31号——现金流量表》的规范进行分类。

**会计信息**：
- 摘要：{summary}
- 科目名称：{account_name}
- 辅助项：{auxiliary_item}

请从以下标准分类中选择最合适的一项：

**经营活动**：
- 购买商品、接受劳务支付的现金
- 支付给职工以及为职工支付的现金
- 支付的各项税费
- 支付其他与经营活动有关的现金

**投资活动**：
- 购建固定资产、无形资产和其他长期资产支付的现金
- 投资支付的现金
- 取得子公司及其他营业单位支付的现金净额
- 支付其他与投资活动有关的现金

**筹资活动**：
- 偿还债务支付的现金
- 分配股利、利润或偿付利息支付的现金
- 支付其他与筹资活动有关的现金

**其他**：
- 其他活动

请根据会计准则的专业要求，选择最合适的分类。只返回分类名称，不要解释。如果无法确定，返回"其他活动"。
"""

        result = self._call_llm(prompt)
        return result if result else "其他活动"

    