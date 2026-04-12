"""
认证和权限管理
"""
import os
import uuid
import time
import bcrypt
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from fastapi import HTTPException, Request, Cookie
from app.database import Database
from app.permissions import ALL_PERMISSIONS

# 桌面模式：跳过所有认证，自动以管理员身份运行
DESKTOP_MODE = os.environ.get("YONGJINGE_DESKTOP") == "1"

_DESKTOP_USER = {
    "id": 1,
    "username": "admin",
    "role": "admin",
    "permissions": ALL_PERMISSIONS,
}

# 内存 session 缓存：{ session_id: (user_dict, expire_ts) }
# TTL=30秒，同一 session 在30秒内直接命中缓存，跳过4次数据库查询
_SESSION_CACHE: Dict[str, tuple] = {}
_SESSION_CACHE_TTL = 300  # 秒（5分钟）


def _cache_get(session_id: str) -> Optional[Dict]:
    entry = _SESSION_CACHE.get(session_id)
    if entry and time.monotonic() < entry[1]:
        return entry[0]
    if entry:
        del _SESSION_CACHE[session_id]
    return None


def _cache_set(session_id: str, user: Dict):
    _SESSION_CACHE[session_id] = (user, time.monotonic() + _SESSION_CACHE_TTL)


def _cache_delete(session_id: str):
    _SESSION_CACHE.pop(session_id, None)


class AuthManager:
    def __init__(self, db: Database):
        self.db = db
    
    def verify_password(self, password: str, password_hash: str) -> bool:
        """验证密码"""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
        except (ValueError, TypeError):
            return False
    
    def login(self, username: str, password: str) -> Dict:
        """用户登录"""
        user = self.db.get_user_by_username(username)
        if not user:
            raise HTTPException(status_code=401, detail="用户名或密码错误")
        
        if not user['is_active']:
            raise HTTPException(status_code=403, detail="用户已被禁用")
        
        # 检查用户有效期
        if user['valid_until']:
            valid_until = datetime.strptime(user['valid_until'], '%Y%m%d%H%M%S')
            if valid_until < datetime.now():
                raise HTTPException(
                    status_code=403, 
                    detail="用户账号已过期，请联系管理员重新授权"
                )
        
        if not self.verify_password(password, user['password_hash']):
            raise HTTPException(status_code=401, detail="用户名或密码错误")
        
        # 创建会话
        session_id = str(uuid.uuid4())
        
        # 获取会话时长（小时）
        session_duration_hours = int(self.db.get_system_config('session_duration_hours', '24'))
        expires_at = (datetime.now() + timedelta(hours=session_duration_hours)).strftime('%Y%m%d%H%M%S')
        
        self.db.create_session(user['id'], session_id, expires_at)
        
        # 获取用户权限
        if user['role'] == 'admin':
            permissions = ALL_PERMISSIONS
        else:
            permissions = self.db.get_user_permissions(user['id'])
        
        return {
            'session_id': session_id,
            'user': {
                'id': user['id'],
                'username': user['username'],
                'role': user['role'],
                'permissions': permissions
            }
        }
    
    def logout(self, session_id: str):
        """用户登出"""
        _cache_delete(session_id)
        self.db.delete_session(session_id)
    
    def get_current_user(self, session_id: Optional[str] = None, include_daily_unlocks: bool = False) -> Optional[Dict]:
        """获取当前登录用户（包含权限信息）"""
        if DESKTOP_MODE:
            return _DESKTOP_USER
        if not session_id:
            return None

        # 缓存命中：直接返回，0 次数据库查询
        if not include_daily_unlocks:
            cached = _cache_get(session_id)
            if cached is not None:
                return cached

        # 缓存未命中：一次连接完成 session + user + permissions + trial 所有查询
        data = self.db.get_user_auth_data(session_id)
        if not data:
            _cache_delete(session_id)
            return None

        user_id   = data['user_id']
        role      = data['role']
        valid_until = data['valid_until']

        # 检查账号有效期
        if valid_until:
            if datetime.strptime(valid_until, '%Y%m%d%H%M%S') < datetime.now():
                self.db.delete_session(session_id)
                _cache_delete(session_id)
                return {
                    'id': user_id,
                    'username': data['username'],
                    'role': role,
                    'expired': True,
                    'expired_message': '账号已过期，请联系管理员重新授权'
                }

        # 权限合并
        if role == 'admin':
            permissions  = ALL_PERMISSIONS
            active_trial = None
        else:
            permissions  = data['permissions']
            active_trial = data['trial']
            if active_trial:
                from app.permissions import PLANS
                trial_perms = PLANS.get(active_trial['plan_code'], {}).get('permissions', [])
                permissions = list(set(permissions) | set(trial_perms))
            if include_daily_unlocks:
                today = datetime.now().strftime('%Y%m%d')
                conn = self.db.get_connection()
                try:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT permission_code FROM daily_unlocks WHERE user_id=? AND unlock_date=?",
                        (user_id, today)
                    )
                    daily_perms = [r[0] for r in cursor.fetchall()]
                    if daily_perms:
                        permissions = list(set(permissions) | set(daily_perms))
                finally:
                    conn.close()

        result = {
            'id':          user_id,
            'username':    data['username'],
            'role':        role,
            'permissions': permissions,
            'valid_until': valid_until,
            'trial':       active_trial,
        }
        if not include_daily_unlocks:
            _cache_set(session_id, result)
        return result

    def require_auth(self, session_id: Optional[str] = None) -> Dict:
        """要求用户已登录"""
        if DESKTOP_MODE:
            return _DESKTOP_USER
        user = self.get_current_user(session_id)
        if not user:
            raise HTTPException(status_code=401, detail="请先登录")
        return user

    def require_admin(self, session_id: Optional[str] = None) -> Dict:
        """要求管理员权限"""
        if DESKTOP_MODE:
            return _DESKTOP_USER
        user = self.require_auth(session_id)
        if user['role'] != 'admin':
            raise HTTPException(status_code=403, detail="需要管理员权限")
        return user
    
    def require_permission(self, session_id: Optional[str] = None, permission_code: str = None) -> Dict:
        """要求指定权限（订阅权限 或 当日点数解锁均可通过）"""
        user = self.require_auth(session_id)

        # 管理员始终拥有所有权限
        if user['role'] == 'admin':
            return user

        # 检查订阅权限
        if permission_code in user.get('permissions', []):
            return user

        # 检查今日点数解锁
        today = datetime.now().strftime('%Y%m%d')
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id FROM daily_unlocks WHERE user_id=? AND permission_code=? AND unlock_date=?",
            (user['id'], permission_code, today)
        )
        unlocked = cursor.fetchone()
        conn.close()
        if unlocked:
            return user

        from app.permissions import get_permission_name
        permission_name = get_permission_name(permission_code)
        raise HTTPException(
            status_code=403,
            detail=f"需要权限: {permission_name}"
        )

