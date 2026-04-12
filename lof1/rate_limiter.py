# -*- coding: utf-8 -*-
"""
注册频率限制模块
防止恶意注册和机器脚本自动注册
"""

import time
from collections import defaultdict
from typing import Dict, Tuple


class RateLimiter:
    """频率限制器"""
    
    def __init__(self):
        # IP地址 -> [(时间戳, 用户名), ...]
        self.ip_registrations: Dict[str, list] = defaultdict(list)
        # 用户名 -> 最后注册尝试时间
        self.username_attempts: Dict[str, float] = {}
        # 清理过期记录的时间间隔（秒）
        self.cleanup_interval = 3600  # 1小时
        self.last_cleanup = time.time()
    
    def _cleanup_old_records(self):
        """清理过期的注册记录"""
        current_time = time.time()
        if current_time - self.last_cleanup < self.cleanup_interval:
            return
        
        # 清理超过1小时的IP注册记录
        cutoff_time = current_time - 3600
        for ip in list(self.ip_registrations.keys()):
            self.ip_registrations[ip] = [
                (ts, username) for ts, username in self.ip_registrations[ip]
                if ts > cutoff_time
            ]
            if not self.ip_registrations[ip]:
                del self.ip_registrations[ip]
        
        # 清理超过1小时的用户名尝试记录
        cutoff_time = current_time - 3600
        for username in list(self.username_attempts.keys()):
            if self.username_attempts[username] < cutoff_time:
                del self.username_attempts[username]
        
        self.last_cleanup = current_time
    
    def check_rate_limit(self, ip: str, username: str) -> Tuple[bool, str]:
        """
        检查是否超过频率限制
        
        Args:
            ip: 客户端IP地址
            username: 用户名
            
        Returns:
            (是否允许, 错误消息)
        """
        self._cleanup_old_records()
        
        current_time = time.time()
        
        # 检查同一IP在1小时内的注册次数（最多5次）
        ip_records = self.ip_registrations[ip]
        recent_ip_registrations = [
            (ts, uname) for ts, uname in ip_records
            if current_time - ts < 3600
        ]
        
        if len(recent_ip_registrations) >= 5:
            return False, "注册过于频繁，请1小时后再试"
        
        # 检查同一用户名在1分钟内的尝试次数（最多3次）
        if username in self.username_attempts:
            last_attempt = self.username_attempts[username]
            if current_time - last_attempt < 60:
                return False, "该用户名注册尝试过于频繁，请1分钟后再试"
        
        return True, ""
    
    def record_attempt(self, ip: str, username: str, success: bool = False):
        """
        记录注册尝试
        
        Args:
            ip: 客户端IP地址
            username: 用户名
            success: 是否成功
        """
        current_time = time.time()
        
        # 记录IP注册尝试
        self.ip_registrations[ip].append((current_time, username))
        
        # 记录用户名尝试时间
        self.username_attempts[username] = current_time
        
        # 如果成功，清理该用户名的尝试记录
        if success:
            if username in self.username_attempts:
                del self.username_attempts[username]
