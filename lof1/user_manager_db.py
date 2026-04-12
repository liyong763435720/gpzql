# -*- coding: utf-8 -*-
"""
用户管理模块 - SQLite 数据库版本
这是使用 SQLite 数据库的版本，可以替换 user_manager.py
"""

from datetime import datetime
from typing import Dict, Optional, List
from werkzeug.security import generate_password_hash, check_password_hash
from database_models import get_db_manager, User


class UserManagerDB:
    """用户管理器（数据库版本）"""
    
    def __init__(self, db_path: str = "lof_arbitrage.db"):
        """
        初始化用户管理器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_manager = get_db_manager(db_path)
        self._ensure_default_admin()
    
    def _get_session(self):
        """获取数据库会话"""
        return self.db_manager.get_session()
    
    def _ensure_default_admin(self):
        """确保默认管理员账号存在"""
        session = self._get_session()
        try:
            admin = session.query(User).filter(User.username == 'admin').first()
            if not admin:
                admin = User(
                    username='admin',
                    password_hash=generate_password_hash('admin123'),
                    role='admin',
                    favorites=[],
                    settings={},
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                session.add(admin)
                session.commit()
                print("已创建默认管理员账号: admin / admin123")
        except Exception as e:
            session.rollback()
            print(f"创建默认管理员失败: {e}")
        finally:
            session.close()
    
    def register(self, username: str, password: str, email: str = None) -> tuple[bool, str]:
        """注册新用户"""
        # 验证用户名
        if not username or len(username.strip()) < 3:
            return False, "用户名至少需要3个字符"
        
        username = username.strip()
        if len(username) > 20:
            return False, "用户名不能超过20个字符"
        
        if not username.isalnum():
            return False, "用户名只能包含字母和数字"
        
        # 验证邮箱格式（如果提供）
        if email:
            email = email.strip()
            if len(email) > 100:
                return False, "邮箱地址不能超过100个字符"
            
            # 邮箱格式验证
            import re
            email_pattern = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
            if not re.match(email_pattern, email):
                return False, "请输入有效的邮箱地址"
        
        # 验证密码
        if not password or len(password) < 6:
            return False, "密码至少需要6个字符"
        
        if len(password) > 50:
            return False, "密码长度不能超过50个字符"
        
        if len(set(password)) == 1:
            return False, "密码不能全为相同字符"
        
        weak_passwords = ['123456', 'password', '12345678', 'qwerty', 'abc123', 'password123']
        if password.lower() in weak_passwords:
            return False, "密码过于简单，请使用更复杂的密码"
        
        session = self._get_session()
        try:
            # 检查用户是否已存在
            existing = session.query(User).filter(User.username == username).first()
            if existing:
                return False, "用户名已存在"
            
            # 创建新用户
            user = User(
                username=username,
                password_hash=generate_password_hash(password),
                role='user',
                favorites=[],
                settings={},
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            session.add(user)
            session.commit()
            return True, "注册成功"
        except Exception as e:
            session.rollback()
            return False, f"注册失败: {str(e)}"
        finally:
            session.close()
    
    def login(self, username: str, password: str) -> tuple[bool, str, Optional[Dict]]:
        """用户登录"""
        session = self._get_session()
        try:
            user = session.query(User).filter(User.username == username).first()
            if not user:
                return False, "用户名或密码错误", None
            
            if not check_password_hash(user.password_hash, password):
                return False, "用户名或密码错误", None
            
            # 更新最后登录时间
            user.updated_at = datetime.now()
            session.commit()
            
            # 返回用户信息
            user_info = {
                'username': user.username,
                'email': None,  # 数据库模型中暂无 email 字段
                'created_at': user.created_at.isoformat() if user.created_at else None,
                'last_login': user.updated_at.isoformat() if user.updated_at else None,
                'role': user.role
            }
            return True, "登录成功", user_info
        except Exception as e:
            session.rollback()
            return False, f"登录失败: {str(e)}", None
        finally:
            session.close()
    
    def get_user(self, username: str) -> Optional[Dict]:
        """获取用户信息"""
        session = self._get_session()
        try:
            user = session.query(User).filter(User.username == username).first()
            if not user:
                return None
            
            return {
                'username': user.username,
                'email': None,
                'created_at': user.created_at.isoformat() if user.created_at else None,
                'last_login': user.updated_at.isoformat() if user.updated_at else None,
                'role': user.role
            }
        finally:
            session.close()
    
    def user_exists(self, username: str) -> bool:
        """检查用户是否存在"""
        session = self._get_session()
        try:
            user = session.query(User).filter(User.username == username).first()
            return user is not None
        finally:
            session.close()
    
    def get_user_favorites(self, username: str) -> List[str]:
        """获取用户的自选基金列表"""
        session = self._get_session()
        try:
            user = session.query(User).filter(User.username == username).first()
            if not user:
                return []
            return user.favorites or []
        finally:
            session.close()
    
    def set_user_favorites(self, username: str, fund_codes: List[str]) -> bool:
        """设置用户的自选基金列表"""
        session = self._get_session()
        try:
            user = session.query(User).filter(User.username == username).first()
            if not user:
                return False
            user.favorites = fund_codes
            user.updated_at = datetime.now()
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
    
    def add_user_favorite(self, username: str, fund_code: str) -> bool:
        """添加自选基金"""
        session = self._get_session()
        try:
            user = session.query(User).filter(User.username == username).first()
            if not user:
                return False
            if user.favorites is None:
                user.favorites = []
            if fund_code not in user.favorites:
                user.favorites.append(fund_code)
                user.updated_at = datetime.now()
                session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
    
    def remove_user_favorite(self, username: str, fund_code: str) -> bool:
        """移除自选基金"""
        session = self._get_session()
        try:
            user = session.query(User).filter(User.username == username).first()
            if not user:
                return False
            if user.favorites and fund_code in user.favorites:
                user.favorites.remove(fund_code)
                user.updated_at = datetime.now()
                session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
    
    def get_user_settings(self, username: str) -> Dict:
        """获取用户的设置"""
        session = self._get_session()
        try:
            user = session.query(User).filter(User.username == username).first()
            if not user:
                return {}
            return user.settings or {}
        finally:
            session.close()
    
    def set_user_settings(self, username: str, settings: Dict) -> bool:
        """设置用户的设置"""
        session = self._get_session()
        try:
            user = session.query(User).filter(User.username == username).first()
            if not user:
                return False
            
            # 获取现有设置并合并新设置
            if user.settings is None:
                current_settings = {}
            else:
                # 创建新字典，避免直接修改原对象
                current_settings = dict(user.settings) if isinstance(user.settings, dict) else {}
            
            # 合并新设置
            current_settings.update(settings)
            
            # 直接赋值，触发 SQLAlchemy 的变更检测
            user.settings = current_settings
            user.updated_at = datetime.now()
            
            # 标记为已修改
            from sqlalchemy.orm.attributes import flag_modified
            flag_modified(user, 'settings')
            
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            print(f"保存用户设置失败: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            session.close()
    
    def list_all_users(self) -> List[Dict]:
        """获取所有用户列表"""
        session = self._get_session()
        try:
            users = session.query(User).all()
            return [{
                'username': u.username,
                'email': None,
                'role': u.role,
                'created_at': u.created_at.isoformat() if u.created_at else None,
                'last_login': u.updated_at.isoformat() if u.updated_at else None
            } for u in users]
        finally:
            session.close()
    
    def update_user_role(self, username: str, role: str) -> bool:
        """更新用户角色"""
        session = self._get_session()
        try:
            user = session.query(User).filter(User.username == username).first()
            if not user:
                return False
            if role not in ['admin', 'user']:
                return False
            user.role = role
            user.updated_at = datetime.now()
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
    
    def update_user_email(self, username: str, email: str) -> bool:
        """更新用户邮箱（数据库版本暂不支持）"""
        # 数据库模型中暂无 email 字段，可以后续添加
        return False
    
    def reset_user_password(self, username: str, new_password: str) -> bool:
        """重置用户密码"""
        if len(new_password) < 6:
            return False
        
        session = self._get_session()
        try:
            user = session.query(User).filter(User.username == username).first()
            if not user:
                return False
            user.password_hash = generate_password_hash(new_password)
            user.updated_at = datetime.now()
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
    
    def delete_user(self, username: str) -> bool:
        """删除用户"""
        session = self._get_session()
        try:
            user = session.query(User).filter(User.username == username).first()
            if not user:
                return False
            session.delete(user)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
