# -*- coding: utf-8 -*-
"""
LOF1 用户管理器 - 主项目数据库版
直接读写主项目的 stock_data.db，实现用户数据统一存储。
替代原 user_manager_db.py（后者使用独立的 lof_arbitrage.db）。
"""

import sqlite3
import json
import os
from datetime import datetime
from typing import Dict, Optional, List

_LOF1_DIR = os.path.dirname(os.path.abspath(__file__))
_MAIN_DB = os.path.join(os.path.dirname(_LOF1_DIR), "stock_data.db")


def _connect():
    return sqlite3.connect(_MAIN_DB, timeout=10)


class UserManagerMain:
    """用户管理器（主项目数据库版）"""

    def __init__(self, db_path: str = None):
        # db_path 参数保留以兼容原接口，实际忽略，统一使用主 DB
        pass

    # ------------------------------------------------------------------
    # 基础用户操作（读写 users 表）
    # ------------------------------------------------------------------

    def user_exists(self, username: str) -> bool:
        conn = _connect()
        try:
            c = conn.cursor()
            c.execute("SELECT 1 FROM users WHERE username = ?", (username,))
            return c.fetchone() is not None
        finally:
            conn.close()

    def register(self, username: str, password: str, role: str = 'user', email: str = None):
        """
        在主 DB 中创建用户。
        如果用户已存在则直接返回成功（SSO 场景下用户已由主项目创建）。
        返回 (success: bool, message: str)
        """
        if self.user_exists(username):
            return True, "用户已存在"

        import bcrypt
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        conn = _connect()
        try:
            c = conn.cursor()
            c.execute("""
                INSERT INTO users (username, password_hash, role, is_active, created_at)
                VALUES (?, ?, ?, 1, ?)
            """, (username, password_hash, role, datetime.now().strftime('%Y%m%d%H%M%S')))
            conn.commit()
            return True, "注册成功"
        except sqlite3.IntegrityError:
            return False, "用户名已存在"
        except Exception as e:
            return False, f"注册失败: {e}"
        finally:
            conn.close()

    def get_user(self, username: str) -> Optional[Dict]:
        conn = _connect()
        try:
            c = conn.cursor()
            c.execute("""
                SELECT id, username, role, is_active, created_at, updated_at
                FROM users WHERE username = ?
            """, (username,))
            row = c.fetchone()
            if not row:
                return None
            return {
                'username': row[1],
                'role': row[2],
                'email': None,
                'created_at': row[4],
                'last_login': row[5],
            }
        finally:
            conn.close()

    def login(self, username: str, password: str):
        """密码验证（lof1 独立运行时使用；SSO 模式下不调用此方法）"""
        import bcrypt
        conn = _connect()
        try:
            c = conn.cursor()
            c.execute("""
                SELECT password_hash, role, is_active, created_at, updated_at
                FROM users WHERE username = ?
            """, (username,))
            row = c.fetchone()
            if not row:
                return False, "用户名或密码错误", None
            if not row[2]:
                return False, "账号已被禁用", None
            if not bcrypt.checkpw(password.encode('utf-8'), row[0].encode('utf-8')):
                return False, "用户名或密码错误", None
            return True, "登录成功", {
                'username': username,
                'role': row[1],
                'email': None,
                'created_at': row[3],
                'last_login': row[4],
            }
        finally:
            conn.close()

    def list_all_users(self) -> List[Dict]:
        conn = _connect()
        try:
            c = conn.cursor()
            c.execute("""
                SELECT username, role, is_active, created_at, updated_at
                FROM users ORDER BY created_at DESC
            """)
            return [{
                'username': row[0],
                'role': row[1],
                'email': None,
                'is_active': bool(row[2]),
                'created_at': row[3],
                'last_login': row[4],
            } for row in c.fetchall()]
        finally:
            conn.close()

    def update_user_role(self, username: str, role: str) -> bool:
        if role not in ('admin', 'user'):
            return False
        conn = _connect()
        try:
            c = conn.cursor()
            c.execute("""
                UPDATE users SET role = ?, updated_at = ?
                WHERE username = ?
            """, (role, datetime.now().strftime('%Y%m%d%H%M%S'), username))
            conn.commit()
            return c.rowcount > 0
        except Exception:
            return False
        finally:
            conn.close()

    def reset_user_password(self, username: str, new_password: str) -> bool:
        if len(new_password) < 6:
            return False
        import bcrypt
        password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        conn = _connect()
        try:
            c = conn.cursor()
            c.execute("""
                UPDATE users SET password_hash = ?, updated_at = ?
                WHERE username = ?
            """, (password_hash, datetime.now().strftime('%Y%m%d%H%M%S'), username))
            conn.commit()
            return c.rowcount > 0
        except Exception:
            return False
        finally:
            conn.close()

    def delete_user(self, username: str) -> bool:
        conn = _connect()
        try:
            c = conn.cursor()
            c.execute("SELECT id FROM users WHERE username = ?", (username,))
            row = c.fetchone()
            if not row:
                return False
            user_id = row[0]
            c.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
            c.execute("DELETE FROM users WHERE id = ?", (user_id,))
            c.execute("DELETE FROM lof_user_favorites WHERE username = ?", (username,))
            c.execute("DELETE FROM lof_user_settings WHERE username = ?", (username,))
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            return False
        finally:
            conn.close()

    def update_user_email(self, username: str, email: str) -> bool:
        # 主项目 users 表暂无 email 字段，不支持
        return False

    # ------------------------------------------------------------------
    # 自选基金（lof_user_favorites 表）
    # ------------------------------------------------------------------

    def get_user_favorites(self, username: str) -> List[str]:
        conn = _connect()
        try:
            c = conn.cursor()
            c.execute("SELECT fund_codes FROM lof_user_favorites WHERE username = ?", (username,))
            row = c.fetchone()
            if not row:
                return []
            return json.loads(row[0]) if row[0] else []
        except Exception:
            return []
        finally:
            conn.close()

    def set_user_favorites(self, username: str, fund_codes: List[str]) -> bool:
        conn = _connect()
        try:
            c = conn.cursor()
            c.execute("""
                INSERT INTO lof_user_favorites (username, fund_codes, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    fund_codes = excluded.fund_codes,
                    updated_at = excluded.updated_at
            """, (username, json.dumps(fund_codes, ensure_ascii=False),
                  datetime.now().strftime('%Y%m%d%H%M%S')))
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            return False
        finally:
            conn.close()

    def add_user_favorite(self, username: str, fund_code: str) -> bool:
        favorites = self.get_user_favorites(username)
        if fund_code not in favorites:
            favorites.append(fund_code)
        return self.set_user_favorites(username, favorites)

    def remove_user_favorite(self, username: str, fund_code: str) -> bool:
        favorites = self.get_user_favorites(username)
        if fund_code in favorites:
            favorites.remove(fund_code)
        return self.set_user_favorites(username, favorites)

    # ------------------------------------------------------------------
    # 用户设置（lof_user_settings 表）
    # ------------------------------------------------------------------

    def get_user_settings(self, username: str) -> Dict:
        conn = _connect()
        try:
            c = conn.cursor()
            c.execute("SELECT settings FROM lof_user_settings WHERE username = ?", (username,))
            row = c.fetchone()
            if not row:
                return {}
            return json.loads(row[0]) if row[0] else {}
        except Exception:
            return {}
        finally:
            conn.close()

    def set_user_settings(self, username: str, settings: Dict) -> bool:
        current = self.get_user_settings(username)
        current.update(settings)
        conn = _connect()
        try:
            c = conn.cursor()
            c.execute("""
                INSERT INTO lof_user_settings (username, settings, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    settings = excluded.settings,
                    updated_at = excluded.updated_at
            """, (username, json.dumps(current, ensure_ascii=False),
                  datetime.now().strftime('%Y%m%d%H%M%S')))
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            return False
        finally:
            conn.close()
