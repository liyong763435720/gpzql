# -*- coding: utf-8 -*-
"""
缓存管理模块
实现多级缓存：内存缓存 + 文件缓存
"""

import json
import os
import time
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from functools import lru_cache
import threading


class CacheManager:
    """缓存管理器 - 多级缓存支持"""
    
    def __init__(self, cache_dir: str = "cache"):
        """
        初始化缓存管理器
        
        Args:
            cache_dir: 缓存文件目录
        """
        self.cache_dir = cache_dir
        self.memory_cache: Dict[str, Dict] = {}  # 内存缓存
        self.cache_lock = threading.Lock()  # 内存缓存锁
        self.file_lock = threading.Lock()   # 文件缓存锁（防止并发写入损坏文件）
        
        # 创建缓存目录
        os.makedirs(cache_dir, exist_ok=True)
        
        # 缓存配置
        self.cache_config = {
            'fund_info': {
                'ttl': 60,  # 60秒（价格和净值数据，提高缓存时间）
                'file_ttl': 600,  # 10分钟（文件缓存）
                'file_name': 'fund_info_cache.json'
            },
            'fund_list': {
                'ttl': 3600,  # 1小时（基金列表）
                'file_ttl': 3600,
                'file_name': 'fund_list_cache.json'
            },
            'fund_name': {
                'ttl': 3600,  # 1小时（基金名称）
                'file_ttl': 86400,  # 24小时
                'file_name': 'fund_name_cache.json'
            },
            'purchase_limit': {
                'ttl': 3600,  # 1小时（限购信息）
                'file_ttl': 3600,
                'file_name': 'purchase_limit_cache.json'
            },
            'funds_batch': {
                'ttl': 60,  # 60秒（批量基金数据，提高缓存时间）
                'file_ttl': 600,  # 10分钟
                'file_name': 'funds_batch_cache.json'
            }
        }
    
    def _get_cache_key(self, cache_type: str, key: str = '') -> str:
        """生成缓存键"""
        if key:
            return f"{cache_type}:{key}"
        return cache_type
    
    def _get_file_path(self, cache_type: str) -> str:
        """获取缓存文件路径"""
        config = self.cache_config.get(cache_type, {})
        filename = config.get('file_name', f'{cache_type}_cache.json')
        return os.path.join(self.cache_dir, filename)
    
    def _is_expired(self, cache_data: Dict, ttl: int) -> bool:
        """检查缓存是否过期"""
        if 'timestamp' not in cache_data:
            return True
        elapsed = time.time() - cache_data['timestamp']
        return elapsed > ttl
    
    def get(self, cache_type: str, key: str = '', use_file: bool = True) -> Optional[Any]:
        """
        获取缓存数据
        
        Args:
            cache_type: 缓存类型
            key: 缓存键（可选）
            use_file: 是否使用文件缓存
            
        Returns:
            缓存数据，如果不存在或过期返回None
        """
        cache_key = self._get_cache_key(cache_type, key)
        config = self.cache_config.get(cache_type, {})
        ttl = config.get('ttl', 60)
        
        # 1. 先检查内存缓存
        with self.cache_lock:
            if cache_key in self.memory_cache:
                cache_data = self.memory_cache[cache_key]
                if not self._is_expired(cache_data, ttl):
                    return cache_data.get('data')
                else:
                    # 过期，删除
                    del self.memory_cache[cache_key]
        
        # 2. 检查文件缓存
        if use_file:
            file_path = self._get_file_path(cache_type)
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        file_cache = json.load(f)
                    
                    # 如果key为空，返回整个文件缓存
                    if not key:
                        if not self._is_expired(file_cache, config.get('file_ttl', 300)):
                            # 将文件缓存加载到内存
                            if 'data' in file_cache:
                                with self.cache_lock:
                                    self.memory_cache[cache_key] = {
                                        'data': file_cache['data'],
                                        'timestamp': file_cache.get('timestamp', time.time())
                                    }
                                return file_cache['data']
                    else:
                        # 查找特定key的数据
                        if 'data' in file_cache and isinstance(file_cache['data'], dict):
                            if key in file_cache['data']:
                                item_data = file_cache['data'][key]
                                if isinstance(item_data, dict) and 'timestamp' in item_data:
                                    if not self._is_expired(item_data, ttl):
                                        return item_data.get('data')
                except Exception as e:
                    print(f"读取文件缓存失败: {e}")
        
        return None
    
    def set(self, cache_type: str, data: Any, key: str = '', use_file: bool = True):
        """
        设置缓存数据
        
        Args:
            cache_type: 缓存类型
            data: 要缓存的数据
            key: 缓存键（可选）
            use_file: 是否保存到文件
        """
        cache_key = self._get_cache_key(cache_type, key)
        config = self.cache_config.get(cache_type, {})
        
        # 1. 保存到内存缓存
        with self.cache_lock:
            self.memory_cache[cache_key] = {
                'data': data,
                'timestamp': time.time()
            }
        
        # 2. 保存到文件缓存（使用独立文件锁保证并发安全）
        if use_file:
            file_path = self._get_file_path(cache_type)
            with self.file_lock:
                try:
                    # 读取现有缓存
                    file_cache = {}
                    if os.path.exists(file_path):
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                file_cache = json.load(f)
                        except Exception:
                            file_cache = {}

                    # 更新缓存
                    if not key:
                        # 整个缓存
                        file_cache = {
                            'data': data,
                            'timestamp': time.time()
                        }
                    else:
                        # 部分缓存
                        if 'data' not in file_cache:
                            file_cache['data'] = {}
                        if not isinstance(file_cache['data'], dict):
                            file_cache['data'] = {}

                        file_cache['data'][key] = {
                            'data': data,
                            'timestamp': time.time()
                        }
                        file_cache['timestamp'] = time.time()  # 更新文件时间戳

                    # 写入文件
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(file_cache, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    print(f"保存文件缓存失败: {e}")
    
    def clear(self, cache_type: str = None, key: str = ''):
        """
        清除缓存
        
        Args:
            cache_type: 缓存类型，如果为None则清除所有
            key: 缓存键（可选）
        """
        with self.cache_lock:
            if cache_type is None:
                # 清除所有内存缓存
                self.memory_cache.clear()
            else:
                cache_key = self._get_cache_key(cache_type, key)
                if cache_key in self.memory_cache:
                    del self.memory_cache[cache_key]
        
        # 清除文件缓存
        if cache_type:
            file_path = self._get_file_path(cache_type)
            if os.path.exists(file_path):
                if not key:
                    # 删除整个文件
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass
                else:
                    # 删除特定key
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            file_cache = json.load(f)
                        if 'data' in file_cache and isinstance(file_cache['data'], dict):
                            if key in file_cache['data']:
                                del file_cache['data'][key]
                                with open(file_path, 'w', encoding='utf-8') as f:
                                    json.dump(file_cache, f, ensure_ascii=False, indent=2)
                    except Exception:
                        pass

    def get_or_set(self, cache_type: str, key: str, fetch_func, *args, **kwargs) -> Any:
        """
        获取缓存，如果不存在则调用函数获取并缓存
        
        Args:
            cache_type: 缓存类型
            key: 缓存键
            fetch_func: 获取数据的函数
            *args, **kwargs: 传递给fetch_func的参数
            
        Returns:
            缓存或新获取的数据
        """
        # 先尝试从缓存获取
        cached_data = self.get(cache_type, key)
        if cached_data is not None:
            return cached_data
        
        # 缓存未命中，调用函数获取
        data = fetch_func(*args, **kwargs)
        
        # 保存到缓存
        if data is not None:
            self.set(cache_type, data, key)
        
        return data


# 全局缓存管理器实例（使用绝对路径，支持从任意目录导入）
cache_manager = CacheManager(os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache"))
