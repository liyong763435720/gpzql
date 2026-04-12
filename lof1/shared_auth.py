# -*- coding: utf-8 -*-
"""
共享认证桥接模块
从主项目（涌金阁）的 SQLite 数据库验证用户 session，
通过 Flask before_request 钩子将认证状态同步到 Flask session，
实现 lof1 和主项目的用户系统融合。
"""

import sqlite3
import os
from datetime import datetime

# 桌面模式：YONGJINGE_DESKTOP=1 时免登录，自动以 admin 身份运行
DESKTOP_MODE = os.environ.get("YONGJINGE_DESKTOP") == "1"

# 主项目数据库路径（lof1 目录的上级即主项目根目录）
_LOF1_DIR = os.path.dirname(os.path.abspath(__file__))
_MAIN_DB = os.path.join(os.path.dirname(_LOF1_DIR), "stock_data.db")


def get_user_from_main_db(session_id: str):
    """
    根据 session_id 查询主项目数据库，返回用户信息 dict 或 None。
    返回: {'username': str, 'role': str} 或 None
    """
    if not session_id:
        return None
    if not os.path.exists(_MAIN_DB):
        return None
    try:
        conn = sqlite3.connect(_MAIN_DB, timeout=5)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT s.expires_at, u.username, u.role, u.is_active
            FROM sessions s
            JOIN users u ON s.user_id = u.id
            WHERE s.session_id = ?
        """, (session_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        # 检查会话是否过期
        expires_at = datetime.strptime(row[0], '%Y%m%d%H%M%S')
        if expires_at < datetime.now():
            return None

        # 检查账号是否启用
        if not row[3]:
            return None

        return {'username': row[1], 'role': row[2]}
    except Exception:
        return None


def ensure_user_in_lof1(username: str, role: str, user_manager):
    """
    如果用户在 lof1 DB 中不存在，自动创建一条记录（用于收藏/设置等功能）。
    密码设为随机串（lof1 不负责认证，所以密码无意义）。
    """
    try:
        if not user_manager.user_exists(username):
            import secrets
            user_manager.register(username, secrets.token_hex(16), role=role)
    except Exception:
        pass


def init_shared_auth(app, user_manager):
    """
    在 Flask app 上注册 before_request 钩子。
    每次请求前读取主项目 session，同步到 Flask session。
    桌面模式下自动以 admin 身份登录，无需 session_id cookie。
    """
    from flask import request, session as flask_session

    @app.before_request
    def _sync_main_session():
        # 桌面模式：直接以 admin 身份免登录
        if DESKTOP_MODE:
            flask_session['logged_in'] = True
            flask_session['username'] = 'admin'
            flask_session['role'] = 'admin'
            ensure_user_in_lof1('admin', 'admin', user_manager)
            return

        session_id = request.cookies.get('session_id')
        user = get_user_from_main_db(session_id)

        if user:
            flask_session['logged_in'] = True
            flask_session['username'] = user['username']
            flask_session['role'] = user['role']
            # 确保 lof1 DB 中有该用户记录（懒创建）
            ensure_user_in_lof1(user['username'], user['role'], user_manager)
        else:
            flask_session['logged_in'] = False
            flask_session.pop('username', None)
            flask_session.pop('role', None)
