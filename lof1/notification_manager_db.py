# -*- coding: utf-8 -*-
"""
通知管理模块 - SQLite 数据库版本
"""

from datetime import datetime
from typing import Dict, List, Optional
from database_models import get_db_manager, Notification
import uuid


class NotificationType:
    """通知类型"""
    ARBITRAGE_OPPORTUNITY = "arbitrage_opportunity"  # 套利机会
    ARBITRAGE_COMPLETED = "arbitrage_completed"  # 套利完成
    ARBITRAGE_SELL = "arbitrage_sell"  # 套利卖出提醒
    SYSTEM = "system"  # 系统通知
    USER = "user"  # 用户相关通知


class NotificationManagerDB:
    """通知管理器（数据库版本）"""
    
    def __init__(self, db_path: str = "lof_arbitrage.db"):
        """
        初始化通知管理器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_manager = get_db_manager(db_path)
    
    def _get_session(self):
        """获取数据库会话"""
        return self.db_manager.get_session()
    
    def _notification_to_dict(self, notification: Notification) -> Dict:
        """将数据库通知转换为字典"""
        return {
            'id': notification.id,
            'type': notification.notification_type,
            'title': notification.title,
            'content': notification.content,
            'data': notification.data or {},
            'read': notification.read,
            'created_at': notification.created_at.isoformat() if notification.created_at else None,
            'read_at': notification.read_at.isoformat() if notification.read_at else None
        }
    
    def create_notification(self, username: str, notification_type: str, title: str, 
                           content: str, data: Dict = None) -> str:
        """创建通知"""
        notification_id = str(uuid.uuid4())
        
        session = self._get_session()
        try:
            notification = Notification(
                id=notification_id,
                username=username,
                notification_type=notification_type,
                title=title,
                content=content,
                data=data or {},
                read=False,
                created_at=datetime.now()
            )
            session.add(notification)
            session.commit()
            
            # 限制每个用户最多保留500条通知
            self._limit_user_notifications(username)
            
            return notification_id
        except Exception as e:
            session.rollback()
            print(f"创建通知失败: {e}")
            raise
        finally:
            session.close()
    
    def _limit_user_notifications(self, username: str, limit: int = 500):
        """限制用户通知数量"""
        session = self._get_session()
        try:
            notifications = session.query(Notification).filter(
                Notification.username == username
            ).order_by(Notification.created_at.desc()).all()
            
            if len(notifications) > limit:
                # 删除超出限制的通知（保留最新的）
                to_delete = notifications[limit:]
                for notification in to_delete:
                    session.delete(notification)
                session.commit()
        except Exception as e:
            session.rollback()
            print(f"限制通知数量失败: {e}")
        finally:
            session.close()
    
    def get_notifications(self, username: str, unread_only: bool = False, 
                         limit: int = None) -> List[Dict]:
        """获取用户的通知列表（优化：使用索引查询）"""
        session = self._get_session()
        try:
            # 使用索引字段进行查询优化
            query = session.query(Notification).filter(Notification.username == username)
            
            if unread_only:
                # 使用 read 索引
                query = query.filter(Notification.read == False)
            
            # 使用 created_at 索引排序
            query = query.order_by(Notification.created_at.desc())
            
            if limit:
                query = query.limit(limit)
            
            notifications = query.all()
            return [self._notification_to_dict(n) for n in notifications]
        finally:
            session.close()
    
    def get_unread_count(self, username: str) -> int:
        """获取未读通知数量"""
        session = self._get_session()
        try:
            count = session.query(Notification).filter(
                Notification.username == username,
                Notification.read == False
            ).count()
            return count
        finally:
            session.close()
    
    def mark_as_read(self, username: str, notification_id: str) -> bool:
        """标记通知为已读"""
        session = self._get_session()
        try:
            notification = session.query(Notification).filter(
                Notification.id == notification_id,
                Notification.username == username
            ).first()
            
            if not notification:
                return False
            
            notification.read = True
            notification.read_at = datetime.now()
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
    
    def mark_all_as_read(self, username: str) -> bool:
        """标记所有通知为已读"""
        session = self._get_session()
        try:
            notifications = session.query(Notification).filter(
                Notification.username == username,
                Notification.read == False
            ).all()
            
            if not notifications:
                return False
            
            for notification in notifications:
                notification.read = True
                notification.read_at = datetime.now()
            
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
    
    def delete_notification(self, username: str, notification_id: str) -> bool:
        """删除通知"""
        session = self._get_session()
        try:
            notification = session.query(Notification).filter(
                Notification.id == notification_id,
                Notification.username == username
            ).first()
            
            if not notification:
                return False
            
            session.delete(notification)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
    
    def delete_all_read(self, username: str) -> int:
        """删除所有已读通知"""
        session = self._get_session()
        try:
            notifications = session.query(Notification).filter(
                Notification.username == username,
                Notification.read == True
            ).all()
            
            deleted_count = len(notifications)
            
            for notification in notifications:
                session.delete(notification)
            
            session.commit()
            return deleted_count
        except Exception as e:
            session.rollback()
            return 0
        finally:
            session.close()
