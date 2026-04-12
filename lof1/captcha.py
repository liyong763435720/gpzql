# -*- coding: utf-8 -*-
"""
验证码生成和验证模块
防止机器脚本自动注册
"""

import random
import time
from typing import Dict, Tuple


class CaptchaManager:
    """验证码管理器"""
    
    def __init__(self):
        # session_id -> (答案, 过期时间)
        self.captchas: Dict[str, Tuple[int, float]] = {}
        # 验证码有效期（秒）
        self.expiry_time = 300  # 5分钟
        # 清理过期验证码的时间间隔
        self.cleanup_interval = 60  # 1分钟
        self.last_cleanup = time.time()
    
    def _cleanup_expired(self):
        """清理过期的验证码"""
        current_time = time.time()
        if current_time - self.last_cleanup < self.cleanup_interval:
            return
        
        expired_sessions = [
            session_id for session_id, (answer, expiry) in self.captchas.items()
            if current_time > expiry
        ]
        
        for session_id in expired_sessions:
            del self.captchas[session_id]
        
        self.last_cleanup = current_time
    
    def generate_captcha(self, session_id: str) -> Dict[str, any]:
        """
        生成验证码
        
        Args:
            session_id: 会话ID
            
        Returns:
            {'question': '问题文本', 'answer': 答案}
        """
        self._cleanup_expired()
        
        # 生成简单的数学题
        num1 = random.randint(1, 20)
        num2 = random.randint(1, 20)
        operator = random.choice(['+', '-', '*'])
        
        if operator == '+':
            answer = num1 + num2
            question = f"{num1} + {num2} = ?"
        elif operator == '-':
            # 确保结果为正数
            if num1 < num2:
                num1, num2 = num2, num1
            answer = num1 - num2
            question = f"{num1} - {num2} = ?"
        else:  # '*'
            # 使用较小的数字避免结果过大
            num1 = random.randint(1, 10)
            num2 = random.randint(1, 10)
            answer = num1 * num2
            question = f"{num1} × {num2} = ?"
        
        expiry = time.time() + self.expiry_time
        self.captchas[session_id] = (answer, expiry)
        
        return {
            'question': question
        }
    
    def verify_captcha(self, session_id: str, user_answer: str) -> Tuple[bool, str]:
        """
        验证验证码
        
        Args:
            session_id: 会话ID
            user_answer: 用户输入的答案
            
        Returns:
            (是否正确, 错误消息)
        """
        self._cleanup_expired()
        
        if session_id not in self.captchas:
            return False, "验证码已过期，请刷新后重试"
        
        answer, expiry = self.captchas[session_id]
        
        # 检查是否过期
        if time.time() > expiry:
            del self.captchas[session_id]
            return False, "验证码已过期，请刷新后重试"
        
        # 验证答案
        try:
            user_answer_int = int(user_answer.strip())
            if user_answer_int == answer:
                # 验证成功后删除验证码（一次性使用）
                del self.captchas[session_id]
                return True, ""
            else:
                return False, "验证码错误"
        except ValueError:
            return False, "验证码格式错误，请输入数字"
    
    def get_captcha_question(self, session_id: str) -> str:
        """
        获取验证码问题（不返回答案）
        
        Args:
            session_id: 会话ID
            
        Returns:
            问题文本
        """
        if session_id not in self.captchas:
            captcha_data = self.generate_captcha(session_id)
            return captcha_data['question']
        
        # 如果已有验证码，返回问题（需要重新生成以获取问题文本）
        # 为了简化，我们重新生成
        captcha_data = self.generate_captcha(session_id)
        return captcha_data['question']
