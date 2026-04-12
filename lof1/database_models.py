# -*- coding: utf-8 -*-
"""
数据库模型定义
使用 SQLAlchemy ORM
"""

from sqlalchemy import create_engine, Column, String, Integer, Float, Boolean, DateTime, Text, ForeignKey, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

Base = declarative_base()


class User(Base):
    """用户表"""
    __tablename__ = 'users'
    
    username = Column(String(50), primary_key=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(20), default='user', nullable=False)  # 'user' or 'admin'
    favorites = Column(JSON, default=list)  # 收藏的基金代码列表
    settings = Column(JSON, default=dict)  # 用户设置（交易费用、阈值等）
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    # 关系
    arbitrage_records = relationship("ArbitrageRecord", back_populates="user", cascade="all, delete-orphan")
    notifications = relationship("Notification", back_populates="user", cascade="all, delete-orphan")


class ArbitrageRecord(Base):
    """套利记录表"""
    __tablename__ = 'arbitrage_records'
    
    id = Column(String(100), primary_key=True)
    username = Column(String(50), ForeignKey('users.username', ondelete='CASCADE'), nullable=False, index=True)
    fund_code = Column(String(20), nullable=False, index=True)
    fund_name = Column(String(200), nullable=False)
    arbitrage_type = Column(String(20), nullable=False, index=True)  # 'premium' or 'discount'
    status = Column(String(20), nullable=False, default='pending', index=True)  # 'pending', 'in_progress', 'completed', 'cancelled'
    
    # 初始操作
    initial_operation_type = Column(String(20))  # 'subscribe', 'buy'
    initial_price = Column(Float, nullable=False)
    initial_shares = Column(Float, nullable=False)
    initial_amount = Column(Float, nullable=False)
    initial_date = Column(String(20))  # YYYY-MM-DD
    initial_timestamp = Column(DateTime)
    initial_fees_info = Column(JSON)  # 费用信息字典
    initial_fee_amount = Column(Float, default=0.0)
    
    # 最终操作
    final_operation_type = Column(String(20))  # 'sell', 'redeem'
    final_price = Column(Float)
    final_shares = Column(Float)
    final_amount = Column(Float)
    final_date = Column(String(20))  # YYYY-MM-DD
    final_timestamp = Column(DateTime)
    final_fees_info = Column(JSON)  # 费用信息字典
    final_fee_amount = Column(Float, default=0.0)
    
    # 盈亏信息
    profit = Column(Float)  # 毛利润
    profit_rate = Column(Float)  # 毛利润率（%）
    net_profit = Column(Float)  # 净利润（扣除所有费用）
    net_profit_rate = Column(Float)  # 净利润率（%）
    
    created_at = Column(DateTime, default=datetime.now, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    # 关系
    user = relationship("User", back_populates="arbitrage_records")


class Notification(Base):
    """通知表"""
    __tablename__ = 'notifications'
    
    id = Column(String(100), primary_key=True)
    username = Column(String(50), ForeignKey('users.username', ondelete='CASCADE'), nullable=False, index=True)
    notification_type = Column(String(50), nullable=False)  # 'arbitrage_opportunity', 'arbitrage_completed', etc.
    title = Column(String(200), nullable=False)
    content = Column(Text, nullable=False)
    data = Column(JSON, default=dict)  # 附加数据
    read = Column(Boolean, default=False, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now, nullable=False, index=True)
    read_at = Column(DateTime)
    
    # 关系
    user = relationship("User", back_populates="notifications")


class FundData(Base):
    """基金数据表（缓存基金实时数据）"""
    __tablename__ = 'fund_data'
    
    fund_code = Column(String(20), primary_key=True)
    fund_name = Column(String(200), nullable=False)
    
    # 价格数据
    price = Column(Float)  # 场内价格
    price_date = Column(String(20))  # 价格日期 YYYY-MM-DD
    change_pct = Column(Float)  # 涨跌幅（%）
    
    # 净值数据
    nav = Column(Float)  # 净值
    nav_date = Column(String(20))  # 净值日期 YYYY-MM-DD
    
    # 套利相关数据
    price_diff = Column(Float)  # 价差（价格-净值）
    price_diff_pct = Column(Float)  # 价差率（%）
    arbitrage_type = Column(String(20))  # 'premium' 或 'discount'
    profit_rate = Column(Float)  # 预期收益率（%）
    
    # 限购信息
    purchase_limit = Column(JSON)  # 限购信息字典
    
    # 元数据
    data_source = Column(String(50))  # 数据来源
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now, nullable=False)


class PriceHistory(Base):
    """折溢价率历史记录表（每次后台更新时追加一条）"""
    __tablename__ = 'price_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_code = Column(String(20), nullable=False, index=True)
    price = Column(Float)           # 场内价格
    nav = Column(Float)             # 场外净值
    price_diff_pct = Column(Float)  # 折溢价率 %
    profit_rate = Column(Float)     # 套利预期收益率 %
    recorded_at = Column(DateTime, default=datetime.now, nullable=False, index=True)


class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, db_path: str = "lof_arbitrage.db"):
        """
        初始化数据库管理器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.engine = create_engine(
            f'sqlite:///{db_path}',
            connect_args={'check_same_thread': False},  # SQLite 允许多线程
            echo=False,  # 设置为 True 可以查看 SQL 语句
            pool_size=10,  # 连接池大小
            max_overflow=20,  # 最大溢出连接数
            pool_pre_ping=True,  # 连接前检查连接是否有效
            pool_recycle=3600  # 连接回收时间（秒）
        )
        self.SessionLocal = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
    
    def init_database(self):
        """初始化数据库（创建表）"""
        Base.metadata.create_all(self.engine)
        print(f"数据库已初始化: {self.db_path}")
    
    def get_session(self):
        """获取数据库会话"""
        return self.SessionLocal()
    
    def close(self):
        """关闭数据库连接"""
        self.engine.dispose()


# 全局数据库管理器实例
_db_manager = None


def get_db_manager(db_path: str = "lof_arbitrage.db") -> DatabaseManager:
    """获取全局数据库管理器实例"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager(db_path)
    return _db_manager


def init_database(db_path: str = "lof_arbitrage.db"):
    """初始化数据库"""
    manager = get_db_manager(db_path)
    manager.init_database()
    return manager
