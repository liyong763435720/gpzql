# -*- coding: utf-8 -*-
"""
Webhook 推送通知模块
支持钉钉、飞书、企业微信三种 Webhook
"""

import json
import requests
from typing import Dict, Optional


class WebhookNotifier:
    """Webhook 推送器"""

    TIMEOUT = 8  # 请求超时秒数

    def send(self, webhook_config: Dict, title: str, content: str) -> Dict:
        """
        统一推送入口

        Args:
            webhook_config: {"type": "dingtalk"|"feishu"|"wecom", "url": "https://..."}
            title: 通知标题
            content: 通知正文（纯文本）

        Returns:
            {"success": bool, "message": str}
        """
        if not webhook_config or not webhook_config.get('url'):
            return {'success': False, 'message': 'Webhook URL 未配置'}

        wtype = webhook_config.get('type', 'dingtalk')
        url = webhook_config['url'].strip()

        try:
            if wtype == 'dingtalk':
                return self._send_dingtalk(url, title, content)
            elif wtype == 'feishu':
                return self._send_feishu(url, title, content)
            elif wtype == 'wecom':
                return self._send_wecom(url, title, content)
            else:
                return {'success': False, 'message': f'不支持的 Webhook 类型: {wtype}'}
        except requests.exceptions.Timeout:
            return {'success': False, 'message': 'Webhook 请求超时，请检查 URL 是否正确'}
        except requests.exceptions.ConnectionError:
            return {'success': False, 'message': 'Webhook 连接失败，请检查网络或 URL'}
        except Exception as e:
            return {'success': False, 'message': f'推送失败: {str(e)}'}

    def _send_dingtalk(self, url: str, title: str, content: str) -> Dict:
        """钉钉自定义机器人 - Markdown 消息"""
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": f"## {title}\n\n{content}\n"
            }
        }
        resp = requests.post(url, json=payload, timeout=self.TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get('errcode', -1) == 0:
            return {'success': True, 'message': '钉钉推送成功'}
        return {'success': False, 'message': f"钉钉返回错误: {data.get('errmsg', '未知错误')}"}

    def _send_feishu(self, url: str, title: str, content: str) -> Dict:
        """飞书自定义机器人 - 富文本消息"""
        payload = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": title,
                        "content": [[{"tag": "text", "text": content}]]
                    }
                }
            }
        }
        resp = requests.post(url, json=payload, timeout=self.TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get('StatusCode', -1) == 0 or data.get('code', -1) == 0:
            return {'success': True, 'message': '飞书推送成功'}
        return {'success': False, 'message': f"飞书返回错误: {data.get('msg', data.get('StatusMessage', '未知错误'))}"}

    def _send_wecom(self, url: str, title: str, content: str) -> Dict:
        """企业微信群机器人 - Markdown 消息"""
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": f"**{title}**\n{content}"
            }
        }
        resp = requests.post(url, json=payload, timeout=self.TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get('errcode', -1) == 0:
            return {'success': True, 'message': '企业微信推送成功'}
        return {'success': False, 'message': f"企业微信返回错误: {data.get('errmsg', '未知错误')}"}

    def format_arbitrage_message(self, fund_code: str, fund_name: str,
                                  arbitrage_type: str, profit_rate: float,
                                  annualized_rate: float, price: float,
                                  nav: float, price_diff_pct: float,
                                  holding_days: int, net_profit_10k: float) -> tuple:
        """
        格式化套利机会消息

        Returns:
            (title, content) 元组
        """
        type_cn = '溢价' if arbitrage_type == 'premium' else '折价'

        title = f"[LOF套利提醒] {type_cn} | {fund_name}（{fund_code}）"

        content = (
            f"**基金代码**：{fund_code}\n"
            f"**基金名称**：{fund_name}\n"
            f"**套利方向**：{type_cn}套利\n"
            f"**场内价格**：{price:.4f} 元\n"
            f"**场外净值**：{nav:.4f} 元\n"
            f"**折溢价率**：{price_diff_pct:+.2f}%\n"
            f"**预期收益**：{profit_rate:.2f}%（万元净赚 {net_profit_10k:.1f} 元）\n"
            f"**年化收益**：{annualized_rate:.1f}%\n"
            f"**预计持仓**：约 {holding_days} 个交易日\n"
        )

        if arbitrage_type == 'premium':
            content += "\n> 操作：场外申购 → 转场内 → 场内卖出"
        else:
            content += "\n> 操作：场内买入 → 转场外 → 场外赎回"

        return title, content


# 模块级单例
webhook_notifier = WebhookNotifier()
