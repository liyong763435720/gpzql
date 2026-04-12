"""
FastAPI路由和接口
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Body, Cookie, Depends, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from jinja2 import Template
import os
import logging
from typing import Optional, Dict, List, Any
import json
import asyncio
from datetime import datetime, timedelta
from pydantic import BaseModel
import pandas as pd
import io

_logger = logging.getLogger(__name__)

# ===== 注册防护：IP 速率限制 =====
import time
import secrets
import random
from collections import defaultdict

_reg_attempts: dict = defaultdict(list)   # ip -> [timestamp, ...]
_REG_LIMIT  = 5      # 每小时最多注册次数
_REG_WINDOW = 3600   # 时间窗口（秒）

def _check_reg_rate(ip: str) -> tuple:
    """检查 IP 是否超出注册频率限制。返回 (allowed, wait_seconds)"""
    now = time.time()
    _reg_attempts[ip] = [t for t in _reg_attempts[ip] if now - t < _REG_WINDOW]
    if len(_reg_attempts[ip]) >= _REG_LIMIT:
        wait = int(_REG_WINDOW - (now - _reg_attempts[ip][0]))
        return False, wait
    return True, 0

def _record_reg_attempt(ip: str):
    _reg_attempts[ip].append(time.time())

# ===== 注册防护：验证码（内存存储）=====
_captcha_store: dict = {}   # token -> (answer, expiry)
_CAPTCHA_TTL = 600          # 验证码有效期（秒）

# /api/data/status 服务端缓存（数据导入后失效）
_data_status_cache: dict = {}   # {'result': ..., 'ts': float}
_DATA_STATUS_TTL = 120          # 2 分钟

def _invalidate_data_status_cache():
    """数据更新后调用，清除 status 缓存"""
    _data_status_cache.clear()

def _gen_captcha() -> tuple:
    """生成数学验证码，返回 (token, question, answer)"""
    a = random.randint(1, 20)
    b = random.randint(1, 20)
    ops = [('+', a + b), ('-', abs(a - b)), ('×', a * b % 100)]
    op_str, ans = random.choice(ops)
    question = f"{max(a,b) if op_str=='-' else a} {op_str} {b if op_str!='-' else min(a,b)} = ?"
    token = secrets.token_urlsafe(16)
    _captcha_store[token] = (ans, time.time() + _CAPTCHA_TTL)
    return token, question

def _verify_captcha(token: str, answer) -> bool:
    """校验验证码，验证后立即删除（一次性）"""
    entry = _captcha_store.pop(token, None)
    if not entry:
        return False
    expected, expiry = entry
    if time.time() > expiry:
        return False
    try:
        return int(answer) == expected
    except (TypeError, ValueError):
        return False

# ===== 注册防护：用户名黑名单 =====
_USERNAME_BLACKLIST = {
    'admin', 'administrator', 'root', 'superuser', 'su', 'sysadmin',
    'test', 'demo', 'guest', 'user', 'null', 'undefined', 'none',
    'system', 'support', 'help', 'info', 'mod', 'moderator',
    'api', 'bot', 'robot', 'script', 'operator',
}

from app.database import Database
from app.config import Config
from app.statistics import Statistics
from app.data_updater import DataUpdater
from app.data_fetcher import DataFetcher
from app.auth import AuthManager
from app.license import get_license_status, validate_license_text, save_license_text

app = FastAPI(title="涌金阁 - 多市场量化分析平台")


@app.on_event("startup")
async def _startup_warmup():
    """启动后后台预热 data/status 缓存，让第一个登录用户也能秒开"""
    import threading
    def _warmup():
        try:
            import time as _time
            _time.sleep(2)   # 等数据库初始化完成
            stocks_df        = db.get_stocks(exclude_delisted=True)
            total_stocks     = len(stocks_df)
            data_source_stats = db.get_data_source_statistics()
            latest_date      = db.get_latest_trade_date()
            market_stats     = db.get_market_statistics()
            completeness     = db.get_market_completeness()
            mds = config.get('market_data_sources', {})
            market_label = {'A': 'A股', 'HK': '港股', 'US': '美股'}
            source_markets: dict = {}
            for m, src in mds.items():
                source_markets.setdefault(src, []).append(market_label.get(m, m))
            existing_sources = {s['data_source'] for s in data_source_stats}
            configured_sources = set(mds.values()) | {config.get('data_source', '')}
            configured_sources.discard('')
            for src in sorted(configured_sources):
                if src not in existing_sources:
                    data_source_stats.append({'data_source': src, 'data_count': 0,
                                              'latest_date': None, 'stock_count': 0, 'configured_only': True})
            for s in data_source_stats:
                s['markets'] = source_markets.get(s['data_source'], [])
            data_source_stats.sort(key=lambda x: (x.get('data_count', 0) == 0, -x.get('data_count', 0)))
            ref_cfg = config.get('market_reference_counts', {}) or {}
            for c in completeness:
                ref = ref_cfg.get(c['market'], 0)
                c['reference_count'] = ref if ref > 0 else None
                c['list_coverage']   = round(c['total_stocks'] / ref * 100, 1) if ref > 0 else None
            result = {"success": True, "data": {
                "total_stocks": total_stocks, "latest_date": latest_date,
                "data_sources": data_source_stats, "market_stats": market_stats,
                "completeness": completeness,
                "reference_synced_at": config.get('market_reference_synced_at'),
                "reference_source": ref_cfg.get('_source', 'manual'),
            }}
            _data_status_cache['result'] = result
            _data_status_cache['ts'] = _time.monotonic()
            _logger.info("data/status 缓存预热完成")
        except Exception as e:
            _logger.warning("data/status 缓存预热失败: %s", e)
    threading.Thread(target=_warmup, daemon=True).start()


# CORS 配置（仅允许同源，生产环境可按需调整 allow_origins）
app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("CORS_ALLOW_ORIGIN", "")],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# 初始化
db = Database()
config = Config()
statistics = Statistics(db)
updater = DataUpdater(db, config)
auth = AuthManager(db)

# ===== 套餐检测辅助函数 =====
def _get_user_plan_code(permissions: list, role: str) -> str:
    """根据用户权限列表推断套餐代码（pro → basic → free，取第一个匹配）"""
    from app.permissions import PLANS, PLAN_ORDER
    if role == 'admin':
        return 'pro'
    perm_set = set(permissions)
    for plan_code in reversed(PLAN_ORDER):  # pro -> basic -> free
        plan_perms = set(PLANS[plan_code]['permissions'])
        if plan_perms.issubset(perm_set):
            return plan_code
    return 'free'


def _calc_days_left(valid_until: str):
    """计算距有效期剩余天数。valid_until 格式：YYYYMMDDHHMMSS，无有效期返回 None"""
    if not valid_until:
        return None
    try:
        expire_dt = datetime.strptime(valid_until, '%Y%m%d%H%M%S')
        delta = expire_dt - datetime.now()
        days = delta.days
        return days if days >= 0 else 0
    except Exception:
        return None


# 定期清理过期会话
import threading
def cleanup_sessions_periodically():
    while True:
        import time
        time.sleep(3600)  # 每小时清理一次
        try:
            db.cleanup_expired_sessions()
        except Exception as _ce:
            _logger.error("清理过期会话失败: %s", _ce)

cleanup_thread = threading.Thread(target=cleanup_sessions_periodically, daemon=True)
cleanup_thread.start()

# 自动备份调度线程（每分钟检查一次是否到达备份时间）
def _auto_backup_loop():
    import time
    from datetime import datetime, timedelta
    from app.backup_manager import create_backup, cleanup_old_backups
    while True:
        time.sleep(60)
        try:
            if not config.get("auto_backup_enabled", False):
                continue
            interval    = config.get("auto_backup_interval", "daily")   # daily / weekly
            backup_time = config.get("auto_backup_time", "02:00")        # HH:MM
            last_run    = config.get("auto_backup_last_run", "")

            now = datetime.now()
            hh, mm = (int(x) for x in backup_time.split(":"))

            # 只在整点分钟触发
            if now.hour != hh or now.minute != mm:
                continue

            # 防止同一分钟重复触发
            if last_run:
                last_dt = datetime.strptime(last_run, '%Y%m%d%H%M%S')
                if (now - last_dt).total_seconds() < 60:
                    continue

            # 检查间隔
            should_run = True
            if last_run:
                last_dt = datetime.strptime(last_run, '%Y%m%d%H%M%S')
                if interval == "daily"  and (now - last_dt) < timedelta(hours=20):
                    should_run = False
                if interval == "weekly" and (now - last_dt) < timedelta(days=6):
                    should_run = False

            if not should_run:
                continue

            result = create_backup("user_data")
            config.set("auto_backup_last_run", now.strftime('%Y%m%d%H%M%S'))
            retention = int(config.get("backup_retention", 20))
            cleanup_old_backups(retention)
            _logger.info("自动备份完成: %s", result.get("filename", ""))
        except Exception as _ae:
            _logger.error("自动备份失败: %s", _ae)

threading.Thread(target=_auto_backup_loop, daemon=True).start()

# 进度状态（用于实时返回更新进度）
import threading as _threading
_progress_lock = _threading.Lock()
update_progress = {
    'current': 0,
    'total': 100,
    'message': '',
    'is_running': False,
    'paused': False,
}

# 当前运行中的 DataUpdater 实例（用于暂停/停止控制）
_current_updater = None

# 静态文件
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# 挂载 lof1 LOF基金套利工具（Flask WSGI 子应用）
try:
    from starlette.middleware.wsgi import WSGIMiddleware
    from lof1_wrapper import lof1_flask_app

    class Lof1PermissionApp:
        """
        ASGI 包装器：在转发请求给 lof1 Flask 应用之前，
        先检查主app会话是否具备 lof_arbitrage 权限。
        BaseHTTPMiddleware 与 WSGI 子应用存在兼容问题，
        直接包装 WSGI app 是更可靠的方案。
        """
        def __init__(self, wsgi_app):
            self._app = WSGIMiddleware(wsgi_app)

        async def __call__(self, scope, receive, send):
            if scope['type'] != 'http':
                await self._app(scope, receive, send)
                return

            # 从请求头解析 session_id cookie
            cookie_header = b''
            for name, value in scope.get('headers', []):
                if name == b'cookie':
                    cookie_header = value
                    break
            session_id = None
            for part in cookie_header.decode('utf-8', errors='ignore').split(';'):
                part = part.strip()
                if part.startswith('session_id='):
                    session_id = part[len('session_id='):]
                    break

            # 权限检查（include_daily_unlocks=True 确保点数解锁的临时权限也被读取）
            _dbg_msg = f"[lof1-auth] cookie_header={cookie_header!r}, session_id={session_id!r}\n"
            try:
                user = auth.get_current_user(session_id, include_daily_unlocks=True)
            except Exception as _ex:
                user = None
                _dbg_msg += f"[lof1-auth] exception: {_ex}\n"
            _dbg_msg += f"[lof1-auth] user={user}\n"
            with open(os.path.join(os.path.dirname(__file__), '..', 'lof1_debug.log'), 'a', encoding='utf-8') as _f:
                _f.write(_dbg_msg)

            if user is None:
                body = json.dumps(
                    {'success': False, 'message': '请先登录', 'requires_login': True},
                    ensure_ascii=False
                ).encode('utf-8')
                await send({'type': 'http.response.start', 'status': 401,
                            'headers': [[b'content-type', b'application/json; charset=utf-8']]})
                await send({'type': 'http.response.body', 'body': body})
                return

            if user['role'] != 'admin' and 'lof_arbitrage' not in user.get('permissions', []):
                body = json.dumps(
                    {'success': False, 'message': '无权访问LOF基金套利功能，请联系管理员开通权限'},
                    ensure_ascii=False
                ).encode('utf-8')
                await send({'type': 'http.response.start', 'status': 403,
                            'headers': [[b'content-type', b'application/json; charset=utf-8']]})
                await send({'type': 'http.response.body', 'body': body})
                return

            await self._app(scope, receive, send)

    lof1_flask_app.config['APPLICATION_ROOT'] = '/lof1'
    app.mount("/lof1", Lof1PermissionApp(lof1_flask_app))
    print("[lof1] LOF基金套利工具已挂载至 /lof1（含权限检查）")
except Exception as _e:
    print(f"[lof1] 挂载失败: {_e}")


def progress_callback(current: int, total: int, message: str = ""):
    """进度回调函数"""
    with _progress_lock:
        update_progress['current'] = current
        update_progress['total'] = total
        update_progress['message'] = message


def resolve_data_source(requested: str = None, ts_code: str = None, market: str = None) -> str:
    """按优先级解析数据源：前端指定 > 按市场自动匹配 > 默认A股源"""
    if requested:
        return requested
    mds = config.get('market_data_sources', {})
    fallback = mds.get('A', config.get('data_source', 'akshare'))
    if ts_code:
        detected = updater.fetcher.detect_market(ts_code)
        return mds.get(detected, fallback)
    if market:
        return mds.get(market, fallback)
    return fallback


def _validate_month(month: int) -> int:
    if not 1 <= month <= 12:
        raise ValueError(f"月份必须在 1-12 之间，收到: {month}")
    return month

def _validate_year_range(start_year: int, end_year: int):
    MIN_YEAR, MAX_YEAR = 1990, 2100
    if not (MIN_YEAR <= start_year <= MAX_YEAR):
        raise ValueError(f"起始年份必须在 {MIN_YEAR}-{MAX_YEAR} 之间，收到: {start_year}")
    if not (MIN_YEAR <= end_year <= MAX_YEAR):
        raise ValueError(f"结束年份必须在 {MIN_YEAR}-{MAX_YEAR} 之间，收到: {end_year}")
    if start_year > end_year:
        raise ValueError(f"起始年份({start_year})不能大于结束年份({end_year})")


# ========== 授权相关API ==========

@app.get("/api/license/status")
async def license_status():
    """获取授权状态（无需登录）"""
    return {"success": True, "data": get_license_status()}


@app.post("/api/license/activate")
async def license_activate(data: Dict = Body(...)):
    """提交激活码"""
    license_text = (data.get("license_text") or "").strip()
    if not license_text:
        return {"success": False, "message": "激活码不能为空"}
    is_valid, reason, payload = validate_license_text(license_text)
    if not is_valid:
        msg_map = {
            "expired":          "激活码已过期",
            "machine_mismatch": "激活码与本机不匹配，请确认机器码是否正确",
            "invalid_signature":"激活码无效，请检查是否完整复制",
            "malformed":        "激活码格式错误",
        }
        return {"success": False, "message": msg_map.get(reason, "激活码无效")}
    if not save_license_text(license_text):
        return {"success": False, "message": "激活码保存失败，请检查目录权限"}
    return {
        "success": True,
        "message": f"激活成功！欢迎使用涌金阁，{payload.get('customer', '')}",
        "data": get_license_status(),
    }


# ========== 认证相关API ==========

@app.post("/api/auth/login")
async def login(data: Dict = Body(...), response: JSONResponse = None):
    """用户登录"""
    try:
        username = data.get('username', '').strip()
        password = data.get('password', '')
        
        if not username or not password:
            return {"success": False, "message": "用户名和密码不能为空"}
        
        result = auth.login(username, password)

        # 附加套餐信息
        login_user = result['user']
        login_user['plan_code'] = _get_user_plan_code(login_user.get('permissions', []), login_user.get('role', 'user'))
        login_user['days_left'] = _calc_days_left(login_user.get('valid_until'))
        # 直接附加点数余额，前端无需再单独请求
        _acc = _get_or_create_credit_account(login_user['id'])
        login_user['credits'] = {
            'balance': _acc['balance'],
            'gift_balance': _acc['gift_balance'],
            'total': _acc['balance'] + _acc['gift_balance'],
        }

        # 创建响应并设置Cookie
        response = JSONResponse({
            "success": True,
            "message": "登录成功",
            "user": login_user
        })
        session_hours = int(db.get_system_config('session_duration_hours', '24'))
        response.set_cookie(
            key="session_id",
            value=result['session_id'],
            max_age=session_hours * 3600,
            httponly=True,
            secure=False,
            samesite="lax",
            path="/"
        )
        return response
    except HTTPException as e:
        return {"success": False, "message": e.detail}
    except Exception as e:
        return {"success": False, "message": f"登录失败: {str(e)}"}


@app.get("/api/auth/registration-status")
async def registration_status():
    """查询注册是否开放（公开接口）"""
    enabled = db.get_system_config('registration_enabled', '1') == '1'
    return {"registration_enabled": enabled}


@app.get("/api/auth/captcha")
async def get_captcha():
    """生成注册验证码（公开接口）"""
    # 清理已过期的验证码
    now = time.time()
    expired = [t for t, (_, exp) in _captcha_store.items() if now > exp]
    for t in expired:
        _captcha_store.pop(t, None)
    token, question = _gen_captcha()
    return {"token": token, "question": question}


@app.post("/api/auth/register")
async def register(request: Request, data: Dict = Body(...)):
    """用户注册（公开接口）"""
    import re
    try:
        if db.get_system_config('registration_enabled', '1') == '0':
            return {"success": False, "message": "注册功能已关闭，请联系管理员"}

        # 1. IP 速率限制
        client_ip = request.client.host if request.client else "unknown"
        allowed, wait = _check_reg_rate(client_ip)
        if not allowed:
            minutes = (wait + 59) // 60
            return {"success": False, "message": f"注册过于频繁，请 {minutes} 分钟后再试"}

        # 2. 验证码校验
        captcha_token = data.get('captcha_token', '')
        captcha_answer = data.get('captcha_answer', '')
        if not _verify_captcha(captcha_token, captcha_answer):
            return {"success": False, "message": "验证码错误或已过期，请刷新后重试"}

        username = (data.get('username') or '').strip()
        password = data.get('password') or ''

        if not username or not password:
            return {"success": False, "message": "用户名和密码不能为空"}

        if len(username) < 3 or len(username) > 20:
            return {"success": False, "message": "用户名长度须为3-20个字符"}

        if not re.match(r'^[a-zA-Z0-9]+$', username):
            return {"success": False, "message": "用户名只能包含字母和数字"}

        # 3. 用户名黑名单
        if username.lower() in _USERNAME_BLACKLIST:
            return {"success": False, "message": "该用户名不可使用，请换一个"}

        if len(password) < 6:
            return {"success": False, "message": "密码至少需要6个字符"}

        if len(set(password)) == 1:
            return {"success": False, "message": "密码不能全为相同字符"}

        weak = {'123456', 'password', '12345678', 'qwerty', 'abc123', 'password123'}
        if password.lower() in weak:
            return {"success": False, "message": "密码过于简单，请使用更复杂的密码"}

        _record_reg_attempt(client_ip)

        # 查询同 IP 已注册账号数
        with db.get_connection() as _conn:
            _cur = _conn.cursor()
            _cur.execute("SELECT COUNT(*) FROM users WHERE reg_ip=? AND reg_ip!=''", (client_ip,))
            ip_count = _cur.fetchone()[0]

        user_id = db.create_user(username, password, role='user', valid_until=None)
        db.set_user_permissions(user_id, ['stock_analysis_single'])

        # 记录注册 IP
        with db.get_connection() as _conn:
            _conn.execute("UPDATE users SET reg_ip=? WHERE id=?", (client_ip, user_id))
            _conn.commit()

        # 赠送点数三层逻辑
        gift_enabled  = db.get_system_config('gift_credits_enabled', '1') == '1'
        review_limit  = int(db.get_system_config('gift_ip_review', '2'))  # 同IP达到此数量冻结待审

        _get_or_create_credit_account(user_id)
        expires_at = (datetime.now() + timedelta(days=7)).strftime('%Y%m%d%H%M%S')

        if not gift_enabled:
            # 总开关关闭
            with db.get_connection() as _conn:
                _conn.execute("UPDATE users SET gift_status='skipped', gift_amount=0 WHERE id=?", (user_id,))
                _conn.commit()
            return {"success": True, "message": "注册成功", "user_id": user_id}

        if ip_count >= review_limit:
            # 同IP账号过多，冻结点数待审核
            with db.get_connection() as _conn:
                _conn.execute("UPDATE users SET gift_status='pending', gift_amount=20 WHERE id=?", (user_id,))
                _conn.commit()
            return {"success": True, "message": "注册成功", "user_id": user_id, "gift_pending": True}

        # 正常赠送
        _add_credits(user_id, 20, is_gift=True, description='注册赠送（7天有效）', expires_at=expires_at)
        with db.get_connection() as _conn:
            _conn.execute("UPDATE users SET gift_status='given', gift_amount=20 WHERE id=?", (user_id,))
            _conn.commit()
        return {"success": True, "message": "注册成功", "user_id": user_id}
    except ValueError as e:
        return {"success": False, "message": str(e)}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"success": False, "message": f"注册失败: {str(e)}"}


@app.post("/api/auth/logout")
async def logout(session_id: Optional[str] = Cookie(None)):
    """用户登出"""
    if session_id:
        auth.logout(session_id)
    response = JSONResponse({"success": True, "message": "已登出"})
    response.delete_cookie(key="session_id", path="/")
    return response


@app.get("/api/auth/current-user")
async def get_current_user(session_id: Optional[str] = Cookie(None)):
    """获取当前登录用户信息"""
    from app.auth import DESKTOP_MODE
    user = auth.get_current_user(session_id, include_daily_unlocks=True)
    if user:
        permissions = user.get('permissions', [])
        role = user.get('role', 'user')
        valid_until = user.get('valid_until')
        plan_code = _get_user_plan_code(permissions, role)
        days_left = _calc_days_left(valid_until)
        user['plan_code'] = plan_code
        user['days_left'] = days_left
        # 试用信息
        trial = user.get('trial')
        if trial:
            user['trial_plan'] = trial['plan_code']
            user['trial_days_left'] = _calc_days_left(trial['expires_at'])
        else:
            user['trial_plan'] = None
            user['trial_days_left'] = None
        # 附加点数余额，前端无需再单独请求 /api/credits/balance
        _acc = _get_or_create_credit_account(user['id'])
        user['credits'] = {
            'balance': _acc['balance'],
            'gift_balance': _acc['gift_balance'],
            'total': _acc['balance'] + _acc['gift_balance'],
        }
        return {"success": True, "user": user, "desktop_mode": DESKTOP_MODE}
    else:
        return {"success": False, "user": None, "desktop_mode": DESKTOP_MODE}


# ========== 用户管理API（仅管理员） ==========

@app.get("/api/users")
async def get_users(session_id: Optional[str] = Cookie(None)):
    """获取所有用户列表（仅管理员）"""
    auth.require_admin(session_id)
    try:
        users = db.get_all_users()
        return {"success": True, "data": users}
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/users")
async def create_user(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """创建用户（仅管理员）"""
    auth.require_admin(session_id)
    try:
        username = data.get('username', '').strip()
        password = data.get('password', '')
        role = data.get('role', 'user')
        valid_until = data.get('valid_until')  # 格式：YYYYMMDDHHMMSS 或 None
        
        if not username or not password:
            return {"success": False, "message": "用户名和密码不能为空"}
        
        if role not in ['admin', 'user']:
            return {"success": False, "message": "角色必须是 admin 或 user"}
        
        user_id = db.create_user(username, password, role, valid_until)
        # 新建用户默认无权限（已在数据库层面实现，这里不需要额外操作）
        return {"success": True, "message": "用户创建成功", "user_id": user_id}
    except ValueError as e:
        return {"success": False, "message": str(e)}
    except Exception as e:
        return {"success": False, "message": f"创建用户失败: {str(e)}"}


@app.get("/api/users/{user_id}")
async def get_user(user_id: int, session_id: Optional[str] = Cookie(None)):
    """获取单个用户信息（仅管理员）"""
    auth.require_admin(session_id)
    try:
        user = db.get_user_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}
        return {"success": True, "data": user}
    except Exception as e:
        return {"success": False, "message": str(e)}


@app.put("/api/users/{user_id}")
async def update_user(user_id: int, data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """更新用户信息（仅管理员）"""
    auth.require_admin(session_id)
    try:
        username = data.get('username')
        password = data.get('password')
        role = data.get('role')

        if role and role not in ['admin', 'user']:
            return {"success": False, "message": "角色必须是 admin 或 user"}

        # 用 _UNSET 区分"前端未传"与"前端显式传 null（清空）"
        _UNSET = db._UNSET
        is_active = data['is_active'] if 'is_active' in data else _UNSET
        valid_until = data['valid_until'] if 'valid_until' in data else _UNSET

        db.update_user(user_id, username, password, role, is_active, valid_until)
        return {"success": True, "message": "用户信息已更新"}
    except Exception as e:
        return {"success": False, "message": f"更新用户失败: {str(e)}"}


@app.delete("/api/users/{user_id}")
async def delete_user(user_id: int, session_id: Optional[str] = Cookie(None)):
    """删除用户（仅管理员）"""
    auth.require_admin(session_id)
    try:
        # 不能删除自己
        current_user = auth.get_current_user(session_id)
        if current_user and current_user['id'] == user_id:
            return {"success": False, "message": "不能删除自己的账号"}
        
        db.delete_user(user_id)
        return {"success": True, "message": "用户已删除"}
    except Exception as e:
        return {"success": False, "message": f"删除用户失败: {str(e)}"}


# ========== 套餐API（公开） ==========

def _get_trial_config() -> dict:
    """读取试用配置，格式：{plan_code: {enabled, days}}"""
    raw = db.get_system_config('plan_trials', '')
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _get_active_promotions() -> dict:
    """读取并过滤出当前有效的促销（已启用且未过期）"""
    import json
    raw = db.get_system_config('plan_promotions', '')
    if not raw:
        return {}
    try:
        promos = json.loads(raw)
    except Exception:
        return {}
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    active = {}
    for code, promo in promos.items():
        if not promo.get('enabled'):
            continue
        end_at = promo.get('end_at', '')
        if end_at and end_at < now:
            continue
        active[code] = promo
    return active


@app.get("/api/plans")
async def get_plans(session_id: Optional[str] = Cookie(None)):
    """获取所有套餐列表（含试用配置和当前用户试用状态）"""
    from app.permissions import get_all_plans, PERMISSIONS, PLAN_ORDER
    plans = get_all_plans()
    # 将数据库保存的价格覆盖应用到计划列表（解决重启后价格重置问题）
    import json as _json
    _price_raw = db.get_system_config('plan_prices', '')
    _price_overrides = _json.loads(_price_raw) if _price_raw else {}
    if _price_overrides:
        plans = [dict(p) for p in plans]   # 不修改原 PLANS 对象
        for p in plans:
            ov = _price_overrides.get(p['code'], {})
            if ov:
                if 'price_monthly'   in ov: p['price_monthly']   = ov['price_monthly']
                if 'price_quarterly' in ov: p['price_quarterly']  = ov['price_quarterly']
                if 'price_yearly'    in ov: p['price_yearly']    = ov['price_yearly']
    active_promos = _get_active_promotions()
    trial_cfg = _get_trial_config()

    # 当前用户试用状态
    user = auth.get_current_user(session_id)
    result = []
    for plan in plans:
        perms = [
            {'code': code, 'name': PERMISSIONS[code]['name']}
            for code in plan['permissions']
            if code in PERMISSIONS
        ]
        item = {**plan, 'permission_details': perms, 'promo': None, 'trial_cfg': None, 'trial_status': None}
        if plan['code'] in active_promos:
            item['promo'] = active_promos[plan['code']]
        # 试用配置
        cfg = trial_cfg.get(plan['code'])
        if cfg and cfg.get('enabled') and plan['code'] != 'free':
            item['trial_cfg'] = {'days': cfg.get('days', 7)}
        # 当前用户对该套餐的试用状态
        if user and plan['code'] != 'free':
            existing = db.get_trial(user['id'], plan['code'])
            if existing:
                db_status = existing.get('status', 'active')
                if db_status == 'pending':
                    item['trial_status'] = {'state': 'pending'}
                elif db_status == 'rejected':
                    item['trial_status'] = {'state': 'rejected'}
                else:
                    # active 状态：再判断是否已过期
                    active_trial = db.get_active_trial(user['id'])
                    if active_trial and active_trial['plan_code'] == plan['code']:
                        days_left = _calc_days_left(active_trial['expires_at'])
                        item['trial_status'] = {'state': 'active', 'days_left': days_left}
                    else:
                        item['trial_status'] = {'state': 'used'}
            # 用户当前套餐 >= 该套餐时不需要试用
            cur_plan = _get_user_plan_code(user.get('permissions', []), user.get('role', 'user'))
            if PLAN_ORDER.index(cur_plan) >= PLAN_ORDER.index(plan['code']):
                item['trial_status'] = {'state': 'subscribed'}
        result.append(item)
    return {"success": True, "data": result}


@app.get("/api/plans/{plan_code}")
async def get_plan(plan_code: str):
    """获取单个套餐详情（公开接口）"""
    from app.permissions import get_plan, PERMISSIONS
    plan = get_plan(plan_code)
    if not plan:
        raise HTTPException(status_code=404, detail="套餐不存在")
    # 应用数据库保存的价格覆盖
    import json as _json
    _price_raw = db.get_system_config('plan_prices', '')
    _price_overrides = _json.loads(_price_raw) if _price_raw else {}
    ov = _price_overrides.get(plan_code, {})
    if ov:
        plan = dict(plan)
        if 'price_monthly'   in ov: plan['price_monthly']   = ov['price_monthly']
        if 'price_quarterly' in ov: plan['price_quarterly']  = ov['price_quarterly']
        if 'price_yearly'    in ov: plan['price_yearly']    = ov['price_yearly']
    perms = [
        {'code': code, 'name': PERMISSIONS[code]['name']}
        for code in plan['permissions']
        if code in PERMISSIONS
    ]
    return {"success": True, "data": {**plan, 'permission_details': perms}}


# ========== 订单与支付API ==========

def _activate_subscription(user_id: int, plan_code: str, billing: str):
    """支付成功后激活订阅：分配权限 + 设置有效期"""
    from app.permissions import get_plan_permissions

    permissions = get_plan_permissions(plan_code)
    db.set_user_permissions(user_id, permissions)

    # 在原有有效期基础上延续（支持叠加续费），或从现在起算
    user = db.get_user_by_id(user_id)
    now = datetime.now()
    base = now
    if user and user.get('valid_until'):
        try:
            existing = datetime.strptime(user['valid_until'], '%Y%m%d%H%M%S')
            if existing > now:
                base = existing
        except ValueError:
            pass

    if billing == 'yearly':
        delta = timedelta(days=366)
    elif billing == 'quarterly':
        delta = timedelta(days=92)
    else:
        delta = timedelta(days=31)
    valid_until = (base + delta).strftime('%Y%m%d%H%M%S')
    db.update_user(user_id, valid_until=valid_until)


class CreateOrderRequest(BaseModel):
    plan_code: str
    billing: str      # monthly / quarterly / yearly
    pay_method: str   # alipay / wechat


@app.post("/api/orders")
async def create_order(req: CreateOrderRequest, session_id: Optional[str] = Cookie(None)):
    """创建支付订单（需要登录）"""
    user = auth.require_auth(session_id)

    from app.permissions import get_plan, PLANS
    plan = get_plan(req.plan_code)
    if not plan:
        raise HTTPException(status_code=400, detail="套餐不存在")
    if req.plan_code == 'free':
        raise HTTPException(status_code=400, detail="免费版无需支付")
    if req.billing not in ('monthly', 'quarterly', 'yearly'):
        raise HTTPException(status_code=400, detail="付费周期无效")
    if req.pay_method not in ('alipay', 'wechat'):
        raise HTTPException(status_code=400, detail="支付方式无效")

    # 根据付费周期取对应价格字段
    price_key = {'monthly': 'price_monthly', 'quarterly': 'price_quarterly', 'yearly': 'price_yearly'}[req.billing]

    # 优先使用促销价（促销仅支持月付/年付，季付直接取原价）
    active_promos = _get_active_promotions()
    promo = active_promos.get(req.plan_code)
    if promo and req.billing != 'quarterly':
        price_yuan = promo.get(price_key, plan[price_key])
    else:
        price_yuan = plan[price_key]
    amount_fen = int(price_yuan * 100)  # 转为分

    # 生成订单号
    now_str = datetime.now().strftime('%Y%m%d%H%M%S')
    import secrets as _secrets
    order_id = f"YJG{now_str}{_secrets.token_hex(3).upper()}"
    expires_at = (datetime.now() + timedelta(minutes=15)).strftime('%Y%m%d%H%M%S')

    db.create_order(
        order_id=order_id,
        user_id=user['id'],
        plan_code=req.plan_code,
        billing=req.billing,
        amount=amount_fen,
        pay_method=req.pay_method,
        expires_at=expires_at,
    )

    billing_label = {'monthly': '月付', 'quarterly': '季付', 'yearly': '年付'}.get(req.billing, req.billing)
    subject = f"涌金阁 {plan['name']}（{billing_label}）"
    cfg = config.config

    # 支付方式 → JeePay wayCode
    way_code_map = {'alipay': 'ALI_QR', 'wechat': 'WX_NATIVE'}
    way_code = way_code_map.get(req.pay_method)
    if not way_code:
        raise HTTPException(status_code=400, detail="不支持的支付方式")

    try:
        from app.payment import create_jeepay_qr, PaymentError
        qr_content = create_jeepay_qr(order_id, amount_fen, subject, way_code, cfg)
    except Exception as e:
        _logger.error(f"创建支付二维码失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "success": True,
        "data": {
            "order_id": order_id,
            "qr_content": qr_content,
            "amount_yuan": amount_fen / 100,
            "expires_at": expires_at,
        }
    }


@app.get("/api/orders/{order_id}/status")
async def get_order_status(order_id: str, session_id: Optional[str] = Cookie(None)):
    """轮询订单状态（需要登录）"""
    user = auth.require_auth(session_id)
    db.expire_stale_orders()
    order = db.get_order(order_id)
    if not order or order['user_id'] != user['id']:
        raise HTTPException(status_code=404, detail="订单不存在")
    return {"success": True, "data": {"status": order['status']}}


@app.get("/api/my/subscription")
async def get_my_subscription(session_id: Optional[str] = Cookie(None)):
    """获取当前用户的订阅信息"""
    user = auth.require_auth(session_id)
    user_detail = db.get_user_by_id(user['id'])
    valid_until = user_detail.get('valid_until') if user_detail else None

    days_left = None
    if valid_until:
        try:
            exp = datetime.strptime(valid_until, '%Y%m%d%H%M%S')
            days_left = max(0, (exp - datetime.now()).days)
        except ValueError:
            pass

    from app.permissions import PLANS, PLAN_ORDER
    permissions = user.get('permissions', [])
    plan_code = 'free'
    for code in reversed(PLAN_ORDER):
        plan_perms = set(PLANS[code]['permissions'])
        if plan_perms.issubset(set(permissions)):
            plan_code = code
            break

    plan_names = {'free': '免费版', 'basic': '基础版', 'pro': '专业版'}

    account = _get_or_create_credit_account(user['id'])
    return {"success": True, "data": {
        "plan_code": plan_code,
        "plan_name": plan_names.get(plan_code, '免费版'),
        "valid_until": valid_until,
        "days_left": days_left,
        "permissions": permissions,
        "credits": {
            "balance": account['balance'],
            "gift_balance": account['gift_balance'],
            "total": account['balance'] + account['gift_balance'],
        },
    }}


@app.get("/api/my/orders")
async def get_my_orders(session_id: Optional[str] = Cookie(None)):
    """获取当前用户的订单列表"""
    user = auth.require_auth(session_id)
    orders = db.get_user_orders(user['id'])
    return {"success": True, "data": orders}


@app.get("/api/admin/orders")
async def get_all_orders(
    status: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    session_id: Optional[str] = Cookie(None)
):
    """获取所有订单（管理员）"""
    auth.require_admin(session_id)
    orders = db.get_all_orders(status=status, page=page, page_size=page_size)
    total = db.count_orders(status=status)
    return {"success": True, "data": orders, "total": total, "page": page, "page_size": page_size}


@app.post("/api/admin/users/{user_id}/subscription")
async def admin_set_subscription(
    user_id: int,
    data: Dict = Body(...),
    session_id: Optional[str] = Cookie(None)
):
    """管理员手动设置用户订阅（激活/延期）"""
    auth.require_admin(session_id)
    plan_code = data.get('plan_code', 'basic')
    billing = data.get('billing', 'monthly')  # monthly or yearly

    _activate_subscription(user_id, plan_code, billing)

    user = db.get_user_by_id(user_id)
    return {"success": True, "message": f"订阅已激活", "valid_until": user.get('valid_until') if user else None}


@app.post("/api/admin/users/{user_id}/credits")
async def admin_adjust_credits(
    user_id: int,
    data: Dict = Body(...),
    session_id: Optional[str] = Cookie(None)
):
    """管理员手动调整用户点数（增加或扣减）"""
    auth.require_admin(session_id)
    delta = int(data.get('delta', 0))
    note = data.get('note', '管理员手动调整')
    if delta == 0:
        raise HTTPException(status_code=400, detail="调整数量不能为0")
    user = db.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")
    _get_or_create_credit_account(user_id)
    if delta > 0:
        _add_credits(user_id, delta, is_gift=True, description=note)
    else:
        ok = _deduct_credits(user_id, abs(delta), note)
        if not ok:
            raise HTTPException(status_code=400, detail="点数不足，无法扣减")
    account = _get_or_create_credit_account(user_id)
    return {"success": True, "message": f"点数已调整 {delta:+d}",
            "balance": account['balance'] + account['gift_balance']}


@app.get("/api/admin/revenue/stats")
async def get_revenue_stats(session_id: Optional[str] = Cookie(None)):
    """收入统计（管理员）"""
    auth.require_admin(session_id)
    stats = db.get_revenue_stats()
    return {"success": True, "data": stats}


@app.post("/api/payment/jeepay/notify")
async def jeepay_notify(request: Request):
    """JeePay 异步回调（公开，无需登录）"""
    try:
        data = await request.json()
    except Exception:
        form = await request.form()
        data = dict(form)

    from app.payment import verify_jeepay_notify
    jeepay_cfg = config.config.get('jeepay', {})
    secret = jeepay_cfg.get('app_secret', '')

    data_copy = dict(data)
    if not verify_jeepay_notify(data_copy, secret):
        _logger.warning(f"JeePay 回调签名验证失败: {data}")
        return JSONResponse({"code": "fail", "msg": "签名错误"})

    # state=2 表示支付成功
    if str(data.get('state')) == '2':
        order_id = data.get('mchOrderNo', '')
        trade_no = data.get('payOrderId', '')
        order = db.get_order(order_id)
        if order and order['status'] == 'pending':
            db.update_order_paid(order_id, trade_no)
            if order['plan_code'] == 'credits':
                # 点数充值订单
                _handle_credit_order_paid(order_id, order['user_id'], order['billing'])
                _logger.info(f"JeePay 点数充值成功: order={order_id} user={order['user_id']}")
            else:
                # 订阅订单
                _activate_subscription(order['user_id'], order['plan_code'], order['billing'])
                _logger.info(f"JeePay 支付成功: order={order_id} user={order['user_id']}")

    return JSONResponse({"code": "SUCCESS", "msg": ""})


# ========== 权限管理API（仅管理员） ==========

@app.get("/api/permissions")
async def get_all_permissions(session_id: Optional[str] = Cookie(None)):
    """获取所有权限列表（仅管理员）"""
    auth.require_admin(session_id)
    from app.permissions import get_all_permissions
    return {"success": True, "data": get_all_permissions()}


@app.get("/api/users/{user_id}/permissions")
async def get_user_permissions(user_id: int, session_id: Optional[str] = Cookie(None)):
    """获取用户权限列表（仅管理员）。管理员角色返回全部权限。"""
    auth.require_admin(session_id)
    try:
        user = db.get_user_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}
        if user['role'] == 'admin':
            from app.permissions import ALL_PERMISSIONS
            return {"success": True, "data": ALL_PERMISSIONS}
        permissions = db.get_user_permissions(user_id)
        return {"success": True, "data": permissions}
    except Exception as e:
        return {"success": False, "message": str(e)}


@app.put("/api/users/{user_id}/permissions")
async def update_user_permissions(user_id: int, data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """更新用户权限（仅管理员）"""
    auth.require_admin(session_id)
    try:
        permission_codes = data.get('permissions', [])
        db.set_user_permissions(user_id, permission_codes)
        return {"success": True, "message": "权限已更新"}
    except Exception as e:
        return {"success": False, "message": str(e)}


@app.post("/api/auth/change-password")
async def change_password(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """修改密码（当前用户）"""
    user = auth.require_auth(session_id)
    try:
        old_password = data.get('old_password', '')
        new_password = data.get('new_password', '')
        
        if not old_password or not new_password:
            return {"success": False, "message": "旧密码和新密码不能为空"}
        
        # 验证旧密码
        user_info = db.get_user_by_id(user['id'])
        if not auth.verify_password(old_password, user_info['password_hash']):
            return {"success": False, "message": "旧密码错误"}
        
        # 更新密码
        db.update_user(user['id'], password=new_password)
        return {"success": True, "message": "密码修改成功"}
    except Exception as e:
        return {"success": False, "message": f"修改密码失败: {str(e)}"}


@app.get("/api/system/config")
async def get_system_config(session_id: Optional[str] = Cookie(None)):
    """获取系统配置（仅管理员）"""
    auth.require_admin(session_id)
    try:
        session_duration     = db.get_system_config('session_duration_hours', '24')
        registration_enabled = db.get_system_config('registration_enabled', '1')
        gift_enabled         = db.get_system_config('gift_credits_enabled', '1')
        gift_ip_review       = db.get_system_config('gift_ip_review', '2')
        from fastapi.responses import JSONResponse
        return JSONResponse(
            content={"success": True, "data": {
                "session_duration_hours": int(session_duration),
                "registration_enabled":  registration_enabled == '1',
                "gift_credits_enabled":  gift_enabled == '1',
                "gift_ip_review":        int(gift_ip_review),
            }},
            headers={"Cache-Control": "no-store"}
        )
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/system/config")
async def update_system_config(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """更新系统配置（仅管理员）"""
    auth.require_admin(session_id)
    try:
        session_duration = data.get('session_duration_hours')
        if session_duration:
            db.set_system_config('session_duration_hours', str(session_duration))
        if 'registration_enabled' in data:
            db.set_system_config('registration_enabled', '1' if data['registration_enabled'] else '0')
        if 'gift_credits_enabled' in data:
            db.set_system_config('gift_credits_enabled', '1' if data['gift_credits_enabled'] else '0')
        if 'gift_ip_review' in data:
            db.set_system_config('gift_ip_review', str(int(data['gift_ip_review'])))
        return {"success": True, "message": "系统配置已更新"}
    except Exception as e:
        return {"success": False, "message": f"更新配置失败: {str(e)}"}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """首页：桌面模式加载专用模板，无登录页"""
    from app.auth import DESKTOP_MODE
    name = "index_desktop.html" if DESKTOP_MODE else "index.html"
    template_path = os.path.join("templates", name)
    if not os.path.exists(template_path):
        template_path = os.path.join("templates", "index.html")
    with open(template_path, 'r', encoding='utf-8') as f:
        template = Template(f.read())
    reg_enabled = db.get_system_config('registration_enabled', '1') == '1'
    return HTMLResponse(
        content=template.render(registration_enabled=reg_enabled, now=datetime.now()),
        headers={"Cache-Control": "no-store"}
    )


@app.get("/register", response_class=HTMLResponse)
async def register_page():
    """注册页面（独立页面）"""
    template_path = os.path.join("templates", "register.html")
    with open(template_path, 'r', encoding='utf-8') as f:
        template = Template(f.read())
    reg_enabled = db.get_system_config('registration_enabled', '1') == '1'
    return HTMLResponse(
        content=template.render(registration_enabled=reg_enabled),
        headers={"Cache-Control": "no-store"}
    )


@app.get("/pricing", response_class=HTMLResponse)
async def pricing_page():
    """定价页面（公开，无需登录）"""
    template_path = os.path.join("templates", "pricing.html")
    with open(template_path, 'r', encoding='utf-8') as f:
        content = f.read()
    return HTMLResponse(content=content, headers={"Cache-Control": "no-store"})


@app.get("/api/stocks")
async def get_stocks(market: Optional[str] = None, session_id: Optional[str] = Cookie(None)):
    """获取股票列表（支持市场筛选）"""
    auth.require_auth(session_id)
    try:
        stocks_df = db.get_stocks(exclude_delisted=True, market=market)
        stocks = stocks_df.to_dict('records')
        return {"success": True, "data": stocks}
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.get("/api/stocks/search")
async def search_stocks(keyword: str = "", limit: int = 20, market: Optional[str] = None):
    """搜索股票（根据代码或名称，可按市场过滤）"""
    try:
        if not keyword or len(keyword) < 1:
            return {"success": True, "data": []}

        results = db.search_stocks(keyword, limit=limit, market=market if market else None)
        return {"success": True, "data": results}
    except Exception as e:
        _logger.error("Error in search_stocks: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.get("/api/stock/{code}")
async def get_stock_info(code: str):
    """获取股票信息"""
    try:
        stock = db.get_stock_by_code(code)
        if stock:
            return {"success": True, "data": stock}
        else:
            raise HTTPException(status_code=404, detail="股票不存在")
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/stock/statistics")
async def get_stock_statistics(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """单只股票月份统计"""
    auth.require_permission(session_id, 'stock_analysis_single')
    try:
        code = data.get('code', '').strip()
        month = int(data.get('month', 1))
        _validate_month(month)
        start_year = data.get('start_year')
        end_year = data.get('end_year')

        if not code:
            return {"success": False, "message": "股票代码不能为空"}
        
        # 转换年份为整数（如果提供）
        if start_year:
            try:
                start_year = int(start_year)
            except (ValueError, TypeError):
                start_year = None
        
        if end_year:
            try:
                end_year = int(end_year)
            except (ValueError, TypeError):
                end_year = None
        
        stock = db.get_stock_by_code(code)
        if not stock:
            return {"success": False, "message": f"股票代码 {code} 不存在，请先更新数据"}
        
        # 按市场自动匹配数据源
        requested_data_source = data.get('data_source')
        current_data_source = resolve_data_source(requested_data_source, ts_code=stock['ts_code'])
        exclude_relisting = bool(data.get('exclude_relisting', False))

        result = statistics.calculate_stock_month_statistics(
            stock['ts_code'], month, start_year, end_year, data_source=current_data_source,
            exclude_relisting=exclude_relisting
        )
        result['symbol'] = stock.get('symbol', code)
        result['name'] = stock.get('name', '')
        # 优先使用统计函数返回的实际数据源（fallback 时为 'mixed'），反映真实来源
        result['data_source'] = result.pop('actual_data_source', current_data_source)
        result['market'] = stock.get('market', '')
        result['currency'] = stock.get('currency', '')
        
        # 如果数据为空，检查是否有其他数据源的数据
        if result['total_count'] == 0:
            from app.data_fetcher import DataFetcher
            fetcher = DataFetcher(config)
            stock_market = fetcher.detect_market(stock['ts_code'])
            market_data_source = fetcher.get_data_source_for_market(stock_market)
            
            # 检查是否有其他数据源的数据
            conn = db.get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) as count, data_source
                    FROM monthly_kline
                    WHERE ts_code = ?
                    GROUP BY data_source
                """, (stock['ts_code'],))
                data_sources = cursor.fetchall()
            finally:
                conn.close()
            
            if data_sources:
                available_sources = [ds[1] for ds in data_sources if ds[0] > 0]
                result['message'] = f"该股票在指定月份和年份范围内没有数据。数据库中已有数据源: {', '.join(available_sources)}，建议检查年份范围或月份选择。"
            else:
                result['message'] = f"该股票还没有月K线数据，请先在'数据管理'页面更新数据。建议市场: {stock_market}，数据源: {market_data_source}"
        
        return {"success": True, "data": result}
    except ValueError as e:
        return {"success": False, "message": f"参数错误: {str(e)}"}
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        print(f"Error in get_stock_statistics: {error_detail}")
        return {"success": False, "message": f"查询失败: {str(e)}"}


@app.post("/api/stock/multi-month-statistics")
async def get_stock_multi_month_statistics(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """单只股票多月统计"""
    auth.require_permission(session_id, 'stock_analysis_multi')
    try:
        code = data.get('code', '').strip()
        months = data.get('months', [])
        start_year = data.get('start_year')
        end_year = data.get('end_year')
        
        if not code:
            return {"success": False, "message": "股票代码不能为空"}
        
        # 如果没有指定月份或月份列表为空，默认查询所有月份
        if months is None or (isinstance(months, list) and len(months) == 0):
            months = list(range(1, 13))  # 默认查询所有月份
        
        # 转换年份为整数（如果提供）
        if start_year:
            try:
                start_year = int(start_year)
            except (ValueError, TypeError):
                start_year = None
        
        if end_year:
            try:
                end_year = int(end_year)
            except (ValueError, TypeError):
                end_year = None
        
        stock = db.get_stock_by_code(code)
        if not stock:
            return {"success": False, "message": f"股票代码 {code} 不存在，请先更新数据"}
        
        # 按市场自动匹配数据源
        requested_data_source = data.get('data_source')
        current_data_source = resolve_data_source(requested_data_source, ts_code=stock['ts_code'])
        exclude_relisting = bool(data.get('exclude_relisting', False))

        # 计算每个月份的统计 —— 一次批量查询替代 N 次单月查询
        from app.data_cleaner import filter_relisting_months
        all_df = db.get_monthly_kline(ts_code=stock['ts_code'],
                                      start_year=start_year, end_year=end_year,
                                      data_source=current_data_source)
        if all_df.empty and current_data_source:
            all_df = db.get_monthly_kline(ts_code=stock['ts_code'],
                                          start_year=start_year, end_year=end_year)
        all_df = all_df[all_df['pct_chg'].notna()]
        if exclude_relisting and not all_df.empty:
            all_df = filter_relisting_months(all_df)

        results = []
        for month in months:
            mdf = all_df[all_df['month'] == month] if not all_df.empty else all_df
            if mdf.empty:
                continue
            up_df   = mdf[mdf['pct_chg'] > 0]
            down_df = mdf[mdf['pct_chg'] < 0]
            flat_df = mdf[mdf['pct_chg'] == 0]
            total   = len(mdf)
            up_cnt  = len(up_df)
            down_cnt = len(down_df)
            flat_cnt = len(flat_df)
            results.append({
                'month': month,
                'symbol': stock.get('symbol', code),
                'name': stock.get('name', ''),
                'total_count': total,
                'up_count': up_cnt,
                'down_count': down_cnt,
                'flat_count': flat_cnt,
                'avg_up_pct': round(float(up_df['pct_chg'].mean()), 2) if up_cnt > 0 else 0,
                'avg_down_pct': round(float(abs(down_df['pct_chg'].mean())), 2) if down_cnt > 0 else 0,
                'up_probability': round(up_cnt / total * 100, 2),
                'down_probability': round(down_cnt / total * 100, 2),
                'flat_probability': round(flat_cnt / total * 100, 2),
                'data_source': current_data_source,
            })
        
        # 按月份排序
        results.sort(key=lambda x: x['month'])
        
        return {"success": True, "data": results}
    except ValueError as e:
        return {"success": False, "message": f"参数错误: {str(e)}"}
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        print(f"Error in get_stock_multi_month_statistics: {error_detail}")
        return {"success": False, "message": f"查询失败: {str(e)}"}


@app.post("/api/month/filter")
async def get_month_filter_statistics(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """月榜单（前20支）"""
    auth.require_permission(session_id, 'month_filter')
    try:
        month = int(data.get('month', 1))
        start_year = int(data.get('start_year', 2000))
        end_year = int(data.get('end_year', datetime.now().year))
        top_n = int(data.get('top_n', 20))
        _validate_month(month)
        _validate_year_range(start_year, end_year)
        # 最小涨跌次数，0表示不限制
        min_count = data.get('min_count')
        if min_count is not None:
            min_count = int(min_count)
        else:
            min_count = 0

        requested_data_source = data.get('data_source')
        market = data.get('market') or None
        current_data_source = resolve_data_source(requested_data_source, market=market)
        exclude_relisting = bool(data.get('exclude_relisting', False))

        results = statistics.calculate_month_filter_statistics(
            month, start_year, end_year, top_n, data_source=current_data_source, min_count=min_count,
            market=market, exclude_relisting=exclude_relisting
        )

        # 为每个结果添加数据源信息
        for result in results:
            result['data_source'] = current_data_source

        return {"success": True, "data": results, "data_source": current_data_source}
    except ValueError as e:
        return {"success": False, "message": f"参数错误: {str(e)}"}
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/month/enhanced-stats")
async def get_month_enhanced_stats(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """月榜单增强统计：期望收益率、跑赢大盘概率、近5年一致性"""
    auth.require_permission(session_id, 'month_enhanced')
    try:
        month = int(data.get('month', 1))
        start_year = int(data.get('start_year', 2000))
        end_year = int(data.get('end_year', datetime.now().year))
        _validate_month(month)
        _validate_year_range(start_year, end_year)
        top_n = int(data.get('top_n', 50))
        min_years = int(data.get('min_years', 3))
        market = data.get('market') or None
        requested_data_source = data.get('data_source')
        current_data_source = resolve_data_source(requested_data_source, market=market)

        enhanced_data_source = current_data_source if market else None
        exclude_relisting = bool(data.get('exclude_relisting', False))
        results = statistics.calculate_month_filter_enhanced_stats(
            month, start_year, end_year, top_n,
            data_source=enhanced_data_source, min_years=min_years, market=market,
            exclude_relisting=exclude_relisting
        )
        return {"success": True, "data": results, "data_source": current_data_source}
    except ValueError as e:
        return {"success": False, "message": f"参数错误: {str(e)}"}
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/industry/statistics")
async def get_industry_statistics(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """行业统计"""
    auth.require_permission(session_id, 'industry_statistics')
    try:
        month = int(data.get('month', 1))
        start_year = int(data.get('start_year', 2000))
        end_year = int(data.get('end_year', datetime.now().year))
        _validate_month(month)
        _validate_year_range(start_year, end_year)
        industry_type = data.get('industry_type', 'sw')

        requested_data_source = data.get('data_source')
        market = data.get('market') or None
        current_data_source = resolve_data_source(requested_data_source, market=market)

        exclude_relisting = bool(data.get('exclude_relisting', False))
        results = statistics.calculate_industry_statistics(
            month, start_year, end_year, industry_type, data_source=current_data_source,
            market=market, exclude_relisting=exclude_relisting
        )

        # 为每个结果添加数据源信息
        for result in results:
            result['data_source'] = current_data_source

        return {"success": True, "data": results, "data_source": current_data_source}
    except ValueError as e:
        return {"success": False, "message": f"参数错误: {str(e)}"}
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/industry/enhanced-stats")
async def get_industry_enhanced_stats(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """行业增强统计：期望收益率、跑赢大盘概率、近5年一致性"""
    auth.require_permission(session_id, 'industry_enhanced')
    try:
        month = int(data.get('month', 1))
        start_year = int(data.get('start_year', 2000))
        end_year = int(data.get('end_year', datetime.now().year))
        _validate_month(month)
        _validate_year_range(start_year, end_year)
        industry_type = data.get('industry_type', 'sw')
        market = data.get('market') or None
        requested_data_source = data.get('data_source')
        current_data_source = resolve_data_source(requested_data_source, market=market)

        # 增强分析需要全量K线，market 指定时用对应数据源，全部市场时不限数据源
        enhanced_data_source = current_data_source if market else None
        exclude_relisting = bool(data.get('exclude_relisting', False))
        results = statistics.calculate_industry_enhanced_stats(
            month, start_year, end_year, industry_type,
            data_source=enhanced_data_source, market=market, exclude_relisting=exclude_relisting
        )
        return {"success": True, "data": results, "data_source": current_data_source}
    except ValueError as e:
        return {"success": False, "message": f"参数错误: {str(e)}"}
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/industry/top-stocks")
async def get_industry_top_stocks(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """行业中上涨概率最高的前20支股票"""
    auth.require_permission(session_id, 'industry_top_stocks')
    try:
        industry_name = data.get('industry_name')
        month = int(data.get('month', 1))
        start_year = int(data.get('start_year', 2000))
        end_year = int(data.get('end_year', datetime.now().year))
        _validate_month(month)
        _validate_year_range(start_year, end_year)
        industry_type = data.get('industry_type', 'sw')
        top_n = int(data.get('top_n', 20))

        if not industry_name:
            raise HTTPException(status_code=400, detail="行业名称不能为空")

        requested_data_source = data.get('data_source')
        market = data.get('market') or None
        current_data_source = resolve_data_source(requested_data_source, market=market)

        exclude_relisting = bool(data.get('exclude_relisting', False))
        results = statistics.calculate_industry_top_stocks(
            industry_name, month, start_year, end_year, industry_type, top_n,
            data_source=current_data_source, market=market, exclude_relisting=exclude_relisting
        )

        # 为每个结果添加数据源信息
        for result in results:
            result['data_source'] = current_data_source

        return {"success": True, "data": results, "data_source": current_data_source}
    except ValueError as e:
        return {"success": False, "message": f"参数错误: {str(e)}"}
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.get("/api/industries")
async def get_industries(industry_type: str = 'sw', market: str = None):
    """获取行业列表，可按市场过滤"""
    try:
        industries = db.get_all_industries(industry_type, market=market or None)
        return {"success": True, "data": industries}
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/data/update")
async def update_data(background_tasks: BackgroundTasks, data: Dict = Body(default={}), 
                     session_id: Optional[str] = Cookie(None)):
    """更新数据（需要数据管理权限或管理员权限）"""
    # 允许管理员或拥有data_management权限的用户
    user = auth.get_current_user(session_id)
    if not user:
        raise HTTPException(status_code=401, detail="请先登录")
    
    # 检查是否是管理员
    is_admin = user.get('role') == 'admin'
    # 检查是否有data_management权限
    has_permission = 'data_management' in user.get('permissions', [])
    
    if not (is_admin or has_permission):
        raise HTTPException(status_code=403, detail="需要数据管理权限或管理员权限")
    
    try:
        update_mode = data.get('update_mode', 'incremental')
        is_test = data.get('is_test', False)  # 是否为测试模式
        test_limit = data.get('test_limit', 10)  # 测试模式更新的股票数量
        
        with _progress_lock:
            if update_progress['is_running']:
                # 自动检测僵尸状态：is_running=True 但没有实际运行的 updater
                if _current_updater is None:
                    _logger.warning("Detected zombie is_running state, auto-resetting")
                    update_progress['is_running'] = False
                    update_progress['paused'] = False
                else:
                    return {
                        "success": True,
                        "message": "数据更新正在进行中，请查看进度",
                        "already_running": True
                    }
            update_progress['is_running'] = True
            update_progress['current'] = 0
            update_progress['total'] = 100
            update_progress['message'] = '准备更新...'
            update_progress['paused'] = False
        
        resume_checkpoint = data.get('resume_checkpoint', False)

        def update_task():
            global _current_updater
            try:
                current_updater = DataUpdater(db, config)
                _current_updater = current_updater
                current_updater.set_progress_callback(progress_callback)

                market = data.get('market')
                if is_test:
                    current_updater.test_update_data(market=market, limit=test_limit)
                else:
                    rebuild = (update_mode == 'rebuild')
                    current_updater.update_all_data(
                        rebuild=rebuild, market=market,
                        resume_checkpoint=resume_checkpoint)
            except Exception as e:
                error_msg = str(e)
                _logger.error("update_task failed: %s", error_msg, exc_info=True)
                with _progress_lock:
                    update_progress['message'] = f"更新失败: {error_msg[:100]}"
            finally:
                _current_updater = None
                with _progress_lock:
                    update_progress['is_running'] = False
                    update_progress['paused'] = False
        
        background_tasks.add_task(update_task)
        _invalidate_data_status_cache()   # 数据更新开始，清除 status 缓存
        mode_text = "测试更新" if is_test else "数据更新"
        return {"success": True, "message": f"{mode_text}已开始"}
    except Exception as e:
        with _progress_lock:
            update_progress['is_running'] = False
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/data/update-industry")
async def update_industry(background_tasks: BackgroundTasks, data: Dict = Body(default={}),
                          session_id: Optional[str] = Cookie(None)):
    """单独更新行业分类"""
    user = auth.get_current_user(session_id)
    if not user:
        raise HTTPException(status_code=401, detail="请先登录")
    is_admin = user.get('role') == 'admin'
    has_permission = 'data_management' in user.get('permissions', [])
    if not (is_admin or has_permission):
        raise HTTPException(status_code=403, detail="需要数据管理权限或管理员权限")

    with _progress_lock:
        if update_progress['is_running']:
            return {"success": True, "message": "已有任务正在运行，请查看进度", "already_running": True}
        update_progress['is_running'] = True
        update_progress['paused'] = False
        update_progress['current'] = 0
        update_progress['total'] = 100
        update_progress['message'] = '准备更新行业分类...'

    market = data.get('market') or None

    def task():
        global _current_updater
        try:
            current_updater = DataUpdater(db, config)
            _current_updater = current_updater
            current_updater.set_progress_callback(progress_callback)
            current_updater.update_industry_only(market=market)
        finally:
            _current_updater = None
            with _progress_lock:
                update_progress['is_running'] = False
                update_progress['paused'] = False

    background_tasks.add_task(task)
    market_text = {'A': 'A股', 'HK': '港股', 'US': '美股'}.get(market, '全部市场')
    return {"success": True, "message": f"{market_text}行业分类更新已开始"}


@app.get("/api/data/progress")
async def get_update_progress(session_id: Optional[str] = Cookie(None)):
    """获取更新进度（SSE流，需要数据管理权限）"""
    auth.require_permission(session_id, 'data_management')

    async def event_stream():
        while True:
            with _progress_lock:
                snapshot = dict(update_progress)
            payload = json.dumps({
                "current": snapshot['current'],
                "total": snapshot['total'],
                "message": snapshot['message'],
                "is_running": snapshot['is_running'],
                "paused": snapshot.get('paused', False),
            }, ensure_ascii=False)
            yield f"data: {payload}\n\n"
            if not snapshot['is_running']:
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(event_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.post("/api/data/update/pause")
async def pause_update(session_id: Optional[str] = Cookie(None)):
    """暂停更新（处理完当前股票后暂停）"""
    auth.require_permission(session_id, 'data_management')
    with _progress_lock:
        if not update_progress['is_running'] or update_progress['paused']:
            return {"success": False, "message": "没有正在运行的更新任务"}
        update_progress['paused'] = True
        update_progress['message'] = "⏸ 正在暂停，等待当前股票处理完成..."
    if _current_updater:
        _current_updater.pause()
    return {"success": True, "message": "暂停请求已发送"}


@app.post("/api/data/update/resume")
async def resume_update(session_id: Optional[str] = Cookie(None)):
    """恢复暂停的更新"""
    auth.require_permission(session_id, 'data_management')
    with _progress_lock:
        if not update_progress['is_running'] or not update_progress['paused']:
            return {"success": False, "message": "更新任务未处于暂停状态"}
        update_progress['paused'] = False
    if _current_updater:
        _current_updater.resume()
    return {"success": True, "message": "已恢复更新"}


@app.post("/api/data/update/stop")
async def stop_update(session_id: Optional[str] = Cookie(None)):
    """停止更新并保存断点"""
    auth.require_permission(session_id, 'data_management')
    with _progress_lock:
        if not update_progress['is_running']:
            return {"success": False, "message": "没有正在运行的更新任务"}
        update_progress['message'] = "⏹ 正在停止，等待当前股票处理完成..."
    if _current_updater:
        _current_updater.stop()
    return {"success": True, "message": "停止请求已发送"}


@app.get("/api/data/checkpoint")
async def get_checkpoint(session_id: Optional[str] = Cookie(None)):
    """查询断点信息"""
    auth.require_permission(session_id, 'data_management')
    from app.data_updater import DataUpdater as _DU
    cp = _DU.load_checkpoint()
    return {"success": True, "checkpoint": cp}


@app.delete("/api/data/checkpoint")
async def delete_checkpoint(session_id: Optional[str] = Cookie(None)):
    """清除断点"""
    auth.require_permission(session_id, 'data_management')
    from app.data_updater import DataUpdater as _DU
    _DU.clear_checkpoint()
    return {"success": True, "message": "断点已清除"}


@app.post("/api/data/update/reset")
async def reset_update_state(session_id: Optional[str] = Cookie(None)):
    """强制重置更新状态（用于服务重启后卡住的情况）"""
    global _current_updater
    auth.require_permission(session_id, 'data_management')
    if _current_updater:
        try:
            _current_updater.stop()
        except Exception:
            pass
    _current_updater = None
    with _progress_lock:
        update_progress['is_running'] = False
        update_progress['paused'] = False
        update_progress['message'] = '状态已重置'
        update_progress['current'] = 0
    return {"success": True, "message": "更新状态已强制重置"}


@app.get("/api/config")
async def get_config(session_id: Optional[str] = Cookie(None)):
    """获取配置（仅管理员）"""
    auth.require_admin(session_id)
    try:
        return {"success": True, "data": config.config}
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/config")
async def update_config(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """更新配置（仅管理员）"""
    auth.require_admin(session_id)
    try:
        for key, value in data.items():
            config.set(key, value)
        
        # 重新初始化数据源（同时更新fetcher和data_source）
        updater.fetcher = DataFetcher(config)
        updater.data_source = config.get('data_source', 'akshare')
        
        return {"success": True, "message": "配置已更新"}
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.get("/api/admin/plan-prices")
async def get_plan_prices(session_id: Optional[str] = Cookie(None)):
    """获取套餐价格配置（仅管理员）"""
    auth.require_admin(session_id)
    from app.permissions import PLANS
    import json
    raw = db.get_system_config('plan_prices', '')
    overrides = json.loads(raw) if raw else {}
    result = {}
    for code, plan in PLANS.items():
        result[code] = {
            'name': plan['name'],
            'price_monthly':   overrides.get(code, {}).get('price_monthly',   plan['price_monthly']),
            'price_quarterly': overrides.get(code, {}).get('price_quarterly', plan.get('price_quarterly', 0)),
            'price_yearly':    overrides.get(code, {}).get('price_yearly',    plan['price_yearly']),
        }
    return {"success": True, "data": result}


@app.post("/api/admin/plan-prices")
async def save_plan_prices(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """保存套餐价格（仅管理员），同时同步更新 permissions.py 中 PLANS 的运行时价格"""
    auth.require_admin(session_id)
    import json
    from app.permissions import PLANS
    # 只保存 basic / pro 两个付费套餐的价格
    overrides = {}
    for code in ('basic', 'pro'):
        if code in data:
            overrides[code] = {
                'price_monthly':   float(data[code].get('price_monthly',   PLANS[code]['price_monthly'])),
                'price_quarterly': float(data[code].get('price_quarterly', PLANS[code].get('price_quarterly', 0))),
                'price_yearly':    float(data[code].get('price_yearly',    PLANS[code]['price_yearly'])),
            }
            # 同步更新运行时 PLANS
            PLANS[code]['price_monthly']   = overrides[code]['price_monthly']
            PLANS[code]['price_quarterly'] = overrides[code]['price_quarterly']
            PLANS[code]['price_yearly']    = overrides[code]['price_yearly']
    db.set_system_config('plan_prices', json.dumps(overrides))
    return {"success": True, "message": "套餐价格已保存"}


@app.get("/api/admin/plan-promotions")
async def get_plan_promotions(session_id: Optional[str] = Cookie(None)):
    """获取所有套餐的促销配置（仅管理员）"""
    auth.require_admin(session_id)
    import json
    from app.permissions import PLANS
    raw = db.get_system_config('plan_promotions', '')
    saved = json.loads(raw) if raw else {}
    result = {}
    for code in ('basic', 'pro'):
        default = {
            'enabled': False, 'label': '限时特惠',
            'price_monthly':   PLANS[code]['price_monthly'],
            'price_quarterly': PLANS[code].get('price_quarterly', 0),
            'price_yearly':    PLANS[code]['price_yearly'],
            'end_at': '',
        }
        result[code] = {**default, **saved.get(code, {})}
    return {"success": True, "data": result}


@app.post("/api/admin/plan-promotions")
async def save_plan_promotions(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """保存套餐促销配置（仅管理员）"""
    auth.require_admin(session_id)
    import json
    db.set_system_config('plan_promotions', json.dumps(data))
    return {"success": True, "message": "促销配置已保存"}


@app.get("/api/admin/plan-trials")
async def get_plan_trials(session_id: Optional[str] = Cookie(None)):
    """获取套餐试用配置（仅管理员）"""
    auth.require_admin(session_id)
    cfg = _get_trial_config()
    result = {}
    for code in ('basic', 'pro'):
        result[code] = cfg.get(code, {'enabled': False, 'days': 7})
    return {"success": True, "data": result}


@app.post("/api/admin/plan-trials")
async def save_plan_trials(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """保存套餐试用配置（仅管理员）"""
    auth.require_admin(session_id)
    cfg = {}
    for code in ('basic', 'pro'):
        if code in data:
            days = int(data[code].get('days', 7))
            days = max(1, min(days, 365))
            cfg[code] = {'enabled': bool(data[code].get('enabled', False)), 'days': days}
    db.set_system_config('plan_trials', json.dumps(cfg))
    return {"success": True, "message": "试用配置已保存"}


@app.post("/api/trial/apply")
async def apply_trial(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """用户申请免费试用（提交后进入待审核状态）"""
    user = auth.get_current_user(session_id)
    if not user:
        raise HTTPException(status_code=401, detail="请先登录")

    plan_code = data.get('plan_code', '').strip()
    if plan_code not in ('basic', 'pro'):
        return {"success": False, "message": "无效的套餐"}

    # 检查试用配置是否开启
    trial_cfg = _get_trial_config()
    cfg = trial_cfg.get(plan_code, {})
    if not cfg.get('enabled'):
        return {"success": False, "message": "该套餐暂未开放试用"}

    # 当前套餐已 >= 目标套餐
    from app.permissions import PLAN_ORDER
    cur_plan = _get_user_plan_code(user.get('permissions', []), user.get('role', 'user'))
    if PLAN_ORDER.index(cur_plan) >= PLAN_ORDER.index(plan_code):
        return {"success": False, "message": "您当前套餐已包含该功能，无需试用"}

    # 只能试用高一级的套餐，不能跨级（免费版只能申请基础版试用）
    if PLAN_ORDER.index(plan_code) != PLAN_ORDER.index(cur_plan) + 1:
        plan_names = {'free': '免费版', 'basic': '基础版', 'pro': '专业版'}
        next_plan = PLAN_ORDER[PLAN_ORDER.index(cur_plan) + 1]
        return {"success": False, "message": f"请先体验「{plan_names[next_plan]}」，再升级申请更高套餐的试用"}

    # 验证实名信息
    import re
    real_name = data.get('real_name', '').strip()
    phone     = data.get('phone', '').strip()
    id_card   = data.get('id_card', '').strip()
    if not real_name:
        return {"success": False, "message": "请填写真实姓名"}
    if not re.match(r'^1[3-9]\d{9}$', phone):
        return {"success": False, "message": "请填写正确的手机号（11位）"}
    if not re.match(r'^\d{17}[\dXx]$', id_card):
        return {"success": False, "message": "请填写正确的身份证号（18位）"}

    # 已申请过该套餐（任何状态）
    existing = db.get_trial(user['id'], plan_code)
    if existing:
        status = existing.get('status', 'active')
        if status == 'pending':
            return {"success": False, "message": "您已提交过该套餐的试用申请，请耐心等待审核"}
        if status != 'rejected':
            return {"success": False, "message": "您已申请过该套餐试用，每个套餐仅限一次"}
        # rejected 状态由 create_trial 内部处理冷却逻辑

    # 当前有进行中或待审核的其他试用
    blocking = db.has_active_or_pending_trial(user['id'])
    if blocking:
        plan_names = {'basic': '基础版', 'pro': '专业版'}
        blocking_name = plan_names.get(blocking['plan_code'], blocking['plan_code'])
        blocking_status = blocking.get('status', '')
        if blocking_status == 'pending':
            return {"success": False, "message": f"您有「{blocking_name}」的试用申请正在审核中，请等待审核结束后再申请其他套餐"}
        return {"success": False, "message": f"您当前「{blocking_name}」试用尚未到期，到期后再申请其他套餐"}

    try:
        db.create_trial(user['id'], plan_code, real_name, phone, id_card)
    except ValueError as e:
        return {"success": False, "message": str(e)}

    from app.permissions import PLANS
    plan_name = PLANS[plan_code]['name']
    return {"success": True, "message": f"「{plan_name}」试用申请已提交，等待管理员审核，审核通过后即可使用"}


@app.get("/api/admin/trials")
async def get_admin_trials(session_id: Optional[str] = Cookie(None)):
    """获取所有用户试用记录（仅管理员）"""
    auth.require_admin(session_id)
    rows = db.get_all_trials()
    now_str = datetime.now().strftime('%Y%m%d%H%M%S')
    plan_names = {'basic': '基础版', 'pro': '专业版'}
    trial_cfg = _get_trial_config()
    result = []
    for r in rows:
        db_status = r.get('status', 'active')
        # 计算实际展示状态
        if db_status == 'pending':
            display_status = 'pending'
        elif db_status == 'rejected':
            display_status = 'rejected'
        elif db_status == 'active' and r.get('expires_at') and r['expires_at'] > now_str:
            display_status = 'active'
        else:
            display_status = 'expired'
        days_left = _calc_days_left(r.get('expires_at')) if r.get('expires_at') else None
        cfg = trial_cfg.get(r['plan_code'], {})
        # 身份证脱敏：保留前4后4，中间用*替代
        raw_id = r.get('id_card') or ''
        masked_id = (raw_id[:4] + '*' * (len(raw_id) - 8) + raw_id[-4:]) if len(raw_id) >= 8 else raw_id
        result.append({
            'id': r['id'],
            'username': r['username'] or f"uid:{r['user_id']}",
            'plan_code': r['plan_code'],
            'plan_name': plan_names.get(r['plan_code'], r['plan_code']),
            'applied_at': r.get('applied_at') or r.get('started_at', ''),
            'started_at': r.get('started_at', ''),
            'expires_at': r.get('expires_at', ''),
            'status': display_status,
            'review_note': r.get('review_note', ''),
            'days_left': days_left,
            'trial_days': cfg.get('days', 7),
            'real_name': r.get('real_name') or '',
            'phone': r.get('phone') or '',
            'id_card': masked_id,
        })
    return {"success": True, "data": result}


@app.post("/api/admin/trials/{trial_id}/approve")
async def approve_trial(trial_id: int, session_id: Optional[str] = Cookie(None)):
    """审批通过试用申请（仅管理员）"""
    auth.require_admin(session_id)
    trial_cfg = _get_trial_config()
    # 先查记录，获取 plan_code 以拿到配置天数
    rows = db.get_all_trials()
    trial = next((r for r in rows if r['id'] == trial_id), None)
    if not trial:
        return {"success": False, "message": "记录不存在"}
    if trial.get('status') != 'pending':
        return {"success": False, "message": "该申请不在待审核状态"}
    cfg = trial_cfg.get(trial['plan_code'], {})
    days = cfg.get('days', 7)
    ok = db.approve_trial(trial_id, days)
    if not ok:
        return {"success": False, "message": "审批失败"}
    plan_names = {'basic': '基础版', 'pro': '专业版'}
    return {"success": True, "message": f"已批准，试用期 {days} 天"}


@app.post("/api/admin/trials/{trial_id}/reject")
async def reject_trial(trial_id: int, data: Dict = Body(default={}), session_id: Optional[str] = Cookie(None)):
    """拒绝试用申请（仅管理员）"""
    auth.require_admin(session_id)
    note = data.get('note', '')
    rows = db.get_all_trials()
    trial = next((r for r in rows if r['id'] == trial_id), None)
    if not trial:
        return {"success": False, "message": "记录不存在"}
    if trial.get('status') != 'pending':
        return {"success": False, "message": "该申请不在待审核状态"}
    ok = db.reject_trial(trial_id, note)
    if not ok:
        return {"success": False, "message": "操作失败"}
    return {"success": True, "message": "已拒绝该试用申请"}


# ─── 公告 ────────────────────────────────────────────────────────────────────

@app.get("/api/announcements")
async def get_announcements(session_id: Optional[str] = Cookie(None)):
    """获取当前有效公告（所有人可访问）"""
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    plan_code = '__guest__'  # 默认未登录
    try:
        user = auth.get_current_user(session_id)
        if user:
            plan_code = user.get('plan', 'free') or 'free'
    except Exception:
        pass
    rows = db.get_active_announcements(now_str, plan_code)
    return {"success": True, "data": rows}


@app.get("/api/admin/announcements")
async def admin_get_announcements(session_id: Optional[str] = Cookie(None)):
    """获取所有公告（管理员）"""
    auth.require_admin(session_id)
    rows = db.get_all_announcements()
    return {"success": True, "data": rows}


@app.post("/api/admin/announcements")
async def admin_create_announcement(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """创建公告（管理员）"""
    auth.require_admin(session_id)
    title = (data.get('title') or '').strip()
    if not title:
        return {"success": False, "message": "标题不能为空"}
    ann_id = db.create_announcement(
        title=title,
        content=data.get('content', ''),
        style=data.get('style', 'info'),
        target=data.get('target', 'all'),
        start_at=data.get('start_at') or None,
        end_at=data.get('end_at') or None,
        enabled=int(data.get('enabled', 1)),
        sort_order=int(data.get('sort_order', 0)),
    )
    return {"success": True, "message": "公告已创建", "id": ann_id}


@app.put("/api/admin/announcements/{ann_id}")
async def admin_update_announcement(ann_id: int, data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """更新公告（管理员）"""
    auth.require_admin(session_id)
    title = (data.get('title') or '').strip()
    if not title:
        return {"success": False, "message": "标题不能为空"}
    ok = db.update_announcement(
        ann_id=ann_id,
        title=title,
        content=data.get('content', ''),
        style=data.get('style', 'info'),
        target=data.get('target', 'all'),
        start_at=data.get('start_at') or None,
        end_at=data.get('end_at') or None,
        enabled=int(data.get('enabled', 1)),
        sort_order=int(data.get('sort_order', 0)),
    )
    if not ok:
        return {"success": False, "message": "公告不存在"}
    return {"success": True, "message": "公告已更新"}


@app.delete("/api/admin/announcements/{ann_id}")
async def admin_delete_announcement(ann_id: int, session_id: Optional[str] = Cookie(None)):
    """删除公告（管理员）"""
    auth.require_admin(session_id)
    ok = db.delete_announcement(ann_id)
    if not ok:
        return {"success": False, "message": "公告不存在"}
    return {"success": True, "message": "公告已删除"}


# ─── 宕机补偿 ─────────────────────────────────────────────────────────────────

@app.post("/api/admin/outages")
async def admin_create_outage(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """创建服务中断记录（管理员）"""
    admin = auth.require_admin(session_id)
    title = (data.get('title') or '').strip()
    if not title:
        return {"success": False, "message": "事件标题不能为空"}
    started_at = (data.get('started_at') or '').strip()
    if not started_at:
        started_at = datetime.now().strftime('%Y%m%d%H%M%S')
    ratio = float(data.get('compensation_ratio', 1.5))
    itype = data.get('interruption_type', 'unplanned')
    if itype not in ('unplanned', 'planned', 'degraded'):
        itype = 'unplanned'
    outage_id = db.create_outage(
        title=title,
        description=data.get('description', ''),
        started_at=started_at,
        created_by=admin['id'],
        compensation_ratio=ratio,
        interruption_type=itype,
    )
    return {"success": True, "message": "服务中断记录已创建", "id": outage_id}


@app.get("/api/admin/outages")
async def admin_get_outages(session_id: Optional[str] = Cookie(None)):
    """获取所有宕机记录（管理员）"""
    auth.require_admin(session_id)
    rows = db.get_all_outages()
    return {"success": True, "data": rows}


@app.put("/api/admin/outages/{outage_id}/resolve")
async def admin_resolve_outage(outage_id: int, data: Dict = Body(...),
                               session_id: Optional[str] = Cookie(None)):
    """标记宕机结束（管理员）"""
    auth.require_admin(session_id)
    ended_at = (data.get('ended_at') or '').strip()
    if not ended_at:
        ended_at = datetime.now().strftime('%Y%m%d%H%M%S')
    ok = db.resolve_outage(outage_id, ended_at)
    if not ok:
        return {"success": False, "message": "记录不存在或状态不是 ongoing"}
    outage = db.get_outage_by_id(outage_id)
    return {"success": True, "message": "已标记结束", "duration_minutes": outage['duration_minutes']}


@app.post("/api/admin/outages/{outage_id}/compensate")
async def admin_compensate_outage(outage_id: int, session_id: Optional[str] = Cookie(None)):
    """触发补偿发放（管理员）"""
    auth.require_admin(session_id)
    result = db.apply_outage_compensation(outage_id)
    return result


@app.get("/api/admin/outages/{outage_id}/records")
async def admin_get_compensation_records(outage_id: int, session_id: Optional[str] = Cookie(None)):
    """查看某次宕机的补偿明细（管理员）"""
    auth.require_admin(session_id)
    records = db.get_compensation_records(outage_id)
    return {"success": True, "data": records}


@app.get("/api/my/compensations")
async def my_compensations(session_id: Optional[str] = Cookie(None)):
    """获取当前用户的补偿记录"""
    user = auth.require_auth(session_id)
    records = db.get_user_compensation_records(user['id'])
    return {"success": True, "data": records}


@app.get("/api/admin/payment-config")
async def get_payment_config(session_id: Optional[str] = Cookie(None)):
    """获取支付配置（仅管理员）"""
    auth.require_admin(session_id)
    j = config.get('jeepay') or {}
    return {"success": True, "data": {
        "jeepay": {
            "gateway":    j.get('gateway', ''),
            "mch_no":     j.get('mch_no', ''),
            "app_id":     j.get('app_id', ''),
            "app_secret": j.get('app_secret', ''),
            "notify_url": j.get('notify_url', ''),
        },
    }}


@app.post("/api/admin/payment-config")
async def save_payment_config(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """保存支付配置到 config.json（仅管理员）"""
    auth.require_admin(session_id)
    if 'jeepay' in data:
        config.set('jeepay', data['jeepay'])
    return {"success": True, "message": "支付配置已保存"}


@app.get("/api/data/sources")
async def get_available_data_sources(ts_code: Optional[str] = None):
    """获取可用的数据源列表"""
    try:
        sources = db.get_available_data_sources(ts_code=ts_code)
        return {"success": True, "data": sources}
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/data/compare-sources")
async def compare_data_sources(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """对比不同数据源的数据"""
    auth.require_permission(session_id, 'source_compare')
    try:
        ts_code = data.get('ts_code')
        trade_date = data.get('trade_date')
        month = data.get('month')
        year = data.get('year')
        
        if not ts_code:
            return {"success": False, "message": "股票代码不能为空"}
        
        compare_df = db.compare_data_sources(
            ts_code=ts_code,
            trade_date=trade_date,
            month=month,
            year=year
        )
        
        if compare_df.empty:
            return {"success": True, "data": [], "message": "没有找到对比数据，请先使用不同数据源更新数据"}
        
        # 转换为字典列表
        result = compare_df.to_dict('records')
        
        return {"success": True, "data": result}
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        print(f"Error in compare_data_sources: {error_detail}")
        return {"success": False, "message": f"对比失败: {str(e)}"}


@app.get("/api/data/status")
async def get_data_status(session_id: Optional[str] = Cookie(None)):
    """获取数据状态（所有已登录用户可访问，操作按钮由前端按角色控制）"""
    auth.require_auth(session_id)
    # 命中缓存直接返回，避免每次重跑 5-10 秒的大表聚合查询
    if _data_status_cache and time.monotonic() - _data_status_cache.get('ts', 0) < _DATA_STATUS_TTL:
        return _data_status_cache['result']
    try:
        stocks_df = db.get_stocks(exclude_delisted=True)
        total_stocks = len(stocks_df)

        # 获取所有数据源的统计信息
        data_source_stats = db.get_data_source_statistics()

        # 构建数据源→市场的映射（用于前端展示"配置市场"列）
        mds = config.get('market_data_sources', {})
        market_label = {'A': 'A股', 'HK': '港股', 'US': '美股'}
        source_markets: dict = {}
        for m, src in mds.items():
            source_markets.setdefault(src, []).append(market_label.get(m, m))

        # 将已配置但无数据的数据源也补充进去（显示 0 记录）
        existing_sources = {s['data_source'] for s in data_source_stats}
        configured_sources = set(mds.values()) | {config.get('data_source', '')}
        configured_sources.discard('')
        for src in sorted(configured_sources):
            if src not in existing_sources:
                data_source_stats.append({
                    'data_source': src,
                    'data_count': 0,
                    'latest_date': None,
                    'stock_count': 0,
                    'configured_only': True
                })

        # 给每条记录附上配置的市场信息
        for s in data_source_stats:
            s['markets'] = source_markets.get(s['data_source'], [])

        # 有数据的排前面，同类按数据量降序
        data_source_stats.sort(key=lambda x: (x.get('data_count', 0) == 0, -x.get('data_count', 0)))

        # 获取总体最新日期（所有数据源中的最新日期）
        latest_date = db.get_latest_trade_date()

        market_stats = db.get_market_statistics()

        # 数据完整度：第一层（库存 vs 数据源参考总数）+ 第二层（库存 vs 有K线）
        completeness = db.get_market_completeness()
        ref_cfg = config.get('market_reference_counts', {}) or {}
        ref_synced_at = config.get('market_reference_synced_at') or None
        for c in completeness:
            ref = ref_cfg.get(c['market'], 0)
            # 未同步时用 stocks 表总数作兜底参考（第一层显示 N/A）
            c['reference_count']  = ref if ref > 0 else None
            c['list_coverage']    = round(c['total_stocks'] / ref * 100, 1) if ref > 0 else None
        ref_source = ref_cfg.get('_source', 'manual')

        result = {
            "success": True,
            "data": {
                "total_stocks": total_stocks,
                "latest_date": latest_date,
                "data_sources": data_source_stats,
                "market_stats": market_stats,
                "completeness": completeness,
                "reference_synced_at": ref_synced_at,
                "reference_source": ref_source,
            }
        }
        _data_status_cache['result'] = result
        _data_status_cache['ts'] = time.monotonic()
        return result
    except Exception as e:
        _logger.error("Internal server error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部错误")


@app.post("/api/data/sync-reference-counts")
async def sync_reference_counts(session_id: Optional[str] = Cookie(None)):
    """从数据源获取各市场股票总数，更新参考值"""
    auth.require_permission(session_id, 'data_management')
    fetcher = DataFetcher(config)
    results = {}
    errors  = {}
    for market in ('A', 'HK', 'US'):
        try:
            df = fetcher.get_stock_list(market)
            results[market] = int(len(df)) if df is not None and not df.empty else 0
        except Exception as e:
            _logger.warning("sync_reference_counts market=%s error: %s", market, e)
            errors[market] = str(e)

    if results:
        synced_at = datetime.now().strftime('%Y-%m-%d %H:%M')
        new_ref = {k: v for k, v in results.items()}
        new_ref['_source'] = 'datasource'
        config.set('market_reference_counts', new_ref)
        config.set('market_reference_synced_at', synced_at)

    return {
        "success": bool(results),
        "data": results,
        "errors": errors,
        "synced_at": synced_at if results else None,
    }


# Excel导出工具函数
def export_to_excel(data: List[Dict], filename: str, sheet_name: str = "Sheet1") -> StreamingResponse:
    """将数据导出为Excel文件"""
    try:
        # 创建DataFrame
        df = pd.DataFrame(data)
        
        # 创建Excel文件在内存中
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        output.seek(0)
        
        # 返回文件流
        # 处理中文文件名编码
        from urllib.parse import quote
        encoded_filename = quote(filename.encode('utf-8'))
        
        return StreamingResponse(
            io.BytesIO(output.read()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出Excel失败: {str(e)}")


@app.post("/api/export/stock-statistics")
async def export_stock_statistics(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """导出单只股票统计为Excel"""
    auth.require_permission(session_id, 'export_excel')
    try:
        code = data.get('code', '').strip()
        month = int(data.get('month', 1))
        start_year = data.get('start_year')
        end_year = data.get('end_year')
        requested_data_source = data.get('data_source')
        
        if not code:
            raise HTTPException(status_code=400, detail="股票代码不能为空")
        
        # 转换年份为整数
        if start_year:
            try:
                start_year = int(start_year)
            except (ValueError, TypeError):
                start_year = None
        if end_year:
            try:
                end_year = int(end_year)
            except (ValueError, TypeError):
                end_year = None
        
        stock = db.get_stock_by_code(code)
        if not stock:
            raise HTTPException(status_code=404, detail=f"股票代码 {code} 不存在")

        current_data_source = resolve_data_source(requested_data_source, ts_code=stock['ts_code'])
        exclude_relisting = bool(data.get('exclude_relisting', False))
        result = statistics.calculate_stock_month_statistics(
            stock['ts_code'], month, start_year, end_year, data_source=current_data_source,
            exclude_relisting=exclude_relisting
        )

        # 准备导出数据
        export_data = [{
            '股票代码': stock.get('symbol', code),
            '股票名称': stock.get('name', ''),
            '月份': f"{month}月",
            '起始年份': start_year or '全部',
            '结束年份': end_year or '全部',
            '总次数': result.get('total_count', 0),
            '上涨次数': result.get('up_count', 0),
            '下跌次数': result.get('down_count', 0),
            '上涨概率(%)': result.get('up_probability', 0),
            '下跌概率(%)': result.get('down_probability', 0),
            '平均涨幅(%)': result.get('avg_up_pct', 0),
            '平均跌幅(%)': result.get('avg_down_pct', 0),
            '数据源': current_data_source
        }]
        
        filename = f"{stock.get('symbol', code)}_{stock.get('name', '')}_{month}月统计.xlsx"
        return export_to_excel(export_data, filename, f"{month}月统计")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")


@app.post("/api/export/multi-month-statistics")
async def export_multi_month_statistics(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """导出单只股票多月统计为Excel"""
    auth.require_permission(session_id, 'export_excel')
    try:
        code = data.get('code', '').strip()
        months = data.get('months', [])
        start_year = data.get('start_year')
        end_year = data.get('end_year')
        requested_data_source = data.get('data_source')
        
        if not code:
            raise HTTPException(status_code=400, detail="股票代码不能为空")
        
        if months is None or (isinstance(months, list) and len(months) == 0):
            months = list(range(1, 13))
        
        # 转换年份为整数
        if start_year:
            try:
                start_year = int(start_year)
            except (ValueError, TypeError):
                start_year = None
        if end_year:
            try:
                end_year = int(end_year)
            except (ValueError, TypeError):
                end_year = None
        
        stock = db.get_stock_by_code(code)
        if not stock:
            raise HTTPException(status_code=404, detail=f"股票代码 {code} 不存在")

        current_data_source = resolve_data_source(requested_data_source, ts_code=stock['ts_code'])
        exclude_relisting = bool(data.get('exclude_relisting', False))

        # 计算每个月份的统计
        export_data = []
        for month in months:
            stat = statistics.calculate_stock_month_statistics(
                stock['ts_code'], month, start_year, end_year, data_source=current_data_source,
                exclude_relisting=exclude_relisting
            )
            if stat['total_count'] > 0:
                export_data.append({
                    '月份': f"{month}月",
                    '总次数': stat.get('total_count', 0),
                    '上涨次数': stat.get('up_count', 0),
                    '下跌次数': stat.get('down_count', 0),
                    '上涨概率(%)': stat.get('up_probability', 0),
                    '下跌概率(%)': stat.get('down_probability', 0),
                    '平均涨幅(%)': stat.get('avg_up_pct', 0),
                    '平均跌幅(%)': stat.get('avg_down_pct', 0),
                    '数据源': current_data_source
                })
        
        filename = f"{stock.get('symbol', code)}_{stock.get('name', '')}_多月统计.xlsx"
        return export_to_excel(export_data, filename, "多月统计")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")


@app.post("/api/export/month-filter")
async def export_month_filter(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """导出月榜单为Excel"""
    auth.require_permission(session_id, 'export_excel')
    try:
        month = int(data.get('month', 1))
        start_year = int(data.get('start_year', 2000))
        end_year = int(data.get('end_year', datetime.now().year))
        top_n = int(data.get('top_n', 20))
        min_count = data.get('min_count')
        if min_count is not None:
            min_count = int(min_count)
        else:
            min_count = 0
        requested_data_source = data.get('data_source')
        market = data.get('market') or None
        current_data_source = resolve_data_source(requested_data_source, market=market)
        exclude_relisting = bool(data.get('exclude_relisting', False))
        results = statistics.calculate_month_filter_statistics(
            month, start_year, end_year, top_n, data_source=current_data_source, min_count=min_count,
            market=market, exclude_relisting=exclude_relisting
        )

        # 准备导出数据
        export_data = []
        for idx, item in enumerate(results, 1):
            export_data.append({
                '排名': idx,
                '股票代码': item.get('symbol', ''),
                '股票名称': item.get('name', ''),
                '上涨概率(%)': item.get('up_probability', 0),
                '上涨次数': item.get('up_count', 0),
                '下跌次数': item.get('down_count', 0),
                '平均涨幅(%)': item.get('avg_up_pct', 0),
                '平均跌幅(%)': item.get('avg_down_pct', 0),
                '总次数': item.get('total_count', 0),
                '数据源': current_data_source
            })

        min_count_text = f"_最小涨跌次数{min_count}" if min_count > 0 else ""
        filename = f"{month}月上涨概率前{top_n}支股票{min_count_text}.xlsx"
        return export_to_excel(export_data, filename, f"{month}月统计")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")


@app.post("/api/export/industry-statistics")
async def export_industry_statistics(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """导出行业统计为Excel"""
    auth.require_permission(session_id, 'export_excel')
    try:
        month = int(data.get('month', 1))
        start_year = int(data.get('start_year', 2000))
        end_year = int(data.get('end_year', datetime.now().year))
        industry_type = data.get('industry_type', 'sw')
        requested_data_source = data.get('data_source')
        market = data.get('market') or None
        current_data_source = resolve_data_source(requested_data_source, market=market)
        exclude_relisting = bool(data.get('exclude_relisting', False))
        results = statistics.calculate_industry_statistics(
            month, start_year, end_year, industry_type, data_source=current_data_source,
            market=market, exclude_relisting=exclude_relisting
        )

        # 准备导出数据
        export_data = []
        for idx, item in enumerate(results, 1):
            export_data.append({
                '排名': idx,
                '行业名称': item.get('industry_name', ''),
                '股票数量': item.get('stock_count', 0),
                '上涨概率(%)': item.get('up_probability', 0),
                '上涨次数': item.get('up_count', 0),
                '下跌次数': item.get('down_count', 0),
                '平均涨幅(%)': item.get('avg_up_pct', 0),
                '平均跌幅(%)': item.get('avg_down_pct', 0),
                '总次数': item.get('total_count', 0),
                '数据源': current_data_source
            })
        
        industry_type_name = '申万' if industry_type == 'sw' else '中信'
        filename = f"{industry_type_name}行业_{month}月统计.xlsx"
        return export_to_excel(export_data, filename, f"{industry_type_name}行业统计")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")


@app.post("/api/export/industry-top-stocks")
async def export_industry_top_stocks(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """导出行业前20支股票为Excel"""
    auth.require_permission(session_id, 'export_excel')
    try:
        industry_name = data.get('industry_name')
        month = int(data.get('month', 1))
        start_year = int(data.get('start_year', 2000))
        end_year = int(data.get('end_year', datetime.now().year))
        industry_type = data.get('industry_type', 'sw')
        top_n = int(data.get('top_n', 20))
        requested_data_source = data.get('data_source')
        market = data.get('market') or None

        if not industry_name:
            raise HTTPException(status_code=400, detail="行业名称不能为空")

        current_data_source = resolve_data_source(requested_data_source, market=market)
        exclude_relisting = bool(data.get('exclude_relisting', False))
        results = statistics.calculate_industry_top_stocks(
            industry_name, month, start_year, end_year, industry_type, top_n,
            data_source=current_data_source, market=market, exclude_relisting=exclude_relisting
        )

        # 准备导出数据
        export_data = []
        for idx, item in enumerate(results, 1):
            export_data.append({
                '排名': idx,
                '股票代码': item.get('symbol', ''),
                '股票名称': item.get('name', ''),
                '上涨概率(%)': item.get('up_probability', 0),
                '上涨次数': item.get('up_count', 0),
                '下跌次数': item.get('down_count', 0),
                '平均涨幅(%)': item.get('avg_up_pct', 0),
                '平均跌幅(%)': item.get('avg_down_pct', 0),
                '总次数': item.get('total_count', 0),
                '数据源': current_data_source
            })
        
        filename = f"{industry_name}_{month}月前{top_n}支股票.xlsx"
        return export_to_excel(export_data, filename, f"{industry_name}前{top_n}支")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")


@app.post("/api/export/compare-sources")
async def export_compare_sources(data: Dict = Body(...), session_id: Optional[str] = Cookie(None)):
    """导出数据校对为Excel"""
    auth.require_permission(session_id, 'export_excel')
    try:
        ts_code = data.get('ts_code')
        trade_date = data.get('trade_date')
        month = data.get('month')
        year = data.get('year')
        
        if not ts_code:
            raise HTTPException(status_code=400, detail="股票代码不能为空")
        
        compare_df = db.compare_data_sources(
            ts_code=ts_code,
            trade_date=trade_date,
            month=month,
            year=year
        )
        
        if compare_df.empty:
            raise HTTPException(status_code=404, detail="未找到可对比的数据")
        
        # 转换为字典列表
        export_data = compare_df.to_dict('records')
        
        # 重命名列名为中文
        column_mapping = {
            'ts_code': '股票代码',
            'trade_date': '交易日期',
            'year': '年份',
            'month': '月份',
            'open': '开盘价',
            'close': '收盘价',
            'high': '最高价',
            'low': '最低价',
            'vol': '成交量',
            'amount': '成交额',
            'pct_chg': '涨跌幅(%)',
            'data_source': '数据源'
        }
        
        export_df = pd.DataFrame(export_data)
        export_df = export_df.rename(columns=column_mapping)
        export_data = export_df.to_dict('records')
        
        filename = f"{ts_code}_数据校对.xlsx"
        return export_to_excel(export_data, filename, "数据校对")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")


# ─── 数据管理（备份 / 还原） ───────────────────────────────────────────────────

from app.backup_manager import (
    create_backup, list_backups, delete_backup,
    restore_backup, cleanup_old_backups, BACKUP_DIR, BACKUP_TYPES,
    export_kline_backup, import_kline_backup,
)
import io as _io

@app.get("/api/admin/backups")
async def api_list_backups(session_id: Optional[str] = Cookie(None)):
    """列出所有备份文件（管理员）"""
    auth.require_admin(session_id)
    return {"success": True, "data": list_backups()}


@app.post("/api/admin/backups")
async def api_create_backup(
    data: Dict = Body(default={}),
    session_id: Optional[str] = Cookie(None)
):
    """创建备份（管理员）"""
    auth.require_admin(session_id)
    backup_type = data.get("backup_type", "user_data")
    result = create_backup(backup_type)
    if result["success"]:
        # 创建后自动清理，保留最新20份
        keep = int(config.get("backup_retention", 20))
        cleanup_old_backups(keep)
    return result


@app.get("/api/admin/backups/{filename}/download")
async def api_download_backup(
    filename: str,
    session_id: Optional[str] = Cookie(None)
):
    """下载备份文件（管理员）"""
    auth.require_admin(session_id)
    if not filename.startswith("backup_") or not filename.endswith(".zip") \
            or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="非法文件名")
    path = BACKUP_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="文件不存在")
    data = path.read_bytes()
    return StreamingResponse(
        _io.BytesIO(data),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@app.delete("/api/admin/backups/{filename}")
async def api_delete_backup(
    filename: str,
    session_id: Optional[str] = Cookie(None)
):
    """删除备份文件（管理员）"""
    auth.require_admin(session_id)
    return delete_backup(filename)


@app.post("/api/admin/restore")
async def api_restore_backup(
    file: UploadFile = File(...),
    session_id: Optional[str] = Cookie(None)
):
    """上传备份文件并还原（管理员）"""
    auth.require_admin(session_id)
    if not file.filename.endswith(".zip"):
        return {"success": False, "message": "请上传 .zip 格式的备份文件"}
    zip_bytes = await file.read()
    result = restore_backup(zip_bytes)
    return result


@app.get("/api/admin/kline-export")
async def api_kline_export(
    market: Optional[str] = None,
    session_id: Optional[str] = Cookie(None)
):
    """导出股票数据（stocks + monthly_kline）为 .db.gz 文件供下载"""
    from fastapi.responses import Response
    auth.require_admin(session_id)
    try:
        gz_bytes = export_kline_backup(market=market or None)
        from datetime import datetime
        suffix = f"_{market}" if market else "_all"
        filename = f"klinedata{suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db.gz"
        return Response(
            content=gz_bytes,
            media_type="application/gzip",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        _logger.error("kline export failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/kline-import")
async def api_kline_import(
    file: UploadFile = File(...),
    mode: str = "merge",
    market: Optional[str] = None,
    session_id: Optional[str] = Cookie(None)
):
    """上传 .db.gz 文件还原股票数据（管理员）"""
    auth.require_admin(session_id)
    if not file.filename.endswith(".db.gz"):
        return {"success": False, "message": "请上传 .db.gz 格式的股票数据备份文件"}
    if mode not in ("merge", "replace"):
        return {"success": False, "message": "mode 参数只能是 merge 或 replace"}
    if market and market not in ("A", "HK", "US"):
        return {"success": False, "message": "market 只能是 A、HK 或 US"}
    gz_bytes = await file.read()
    result = import_kline_backup(gz_bytes, mode=mode, market=market or None)
    return result


@app.get("/api/admin/backup-config")
async def api_get_backup_config(session_id: Optional[str] = Cookie(None)):
    """获取自动备份配置（管理员）"""
    auth.require_admin(session_id)
    last_run = config.get("auto_backup_last_run", "")
    last_run_fmt = ""
    if last_run:
        try:
            from datetime import datetime
            dt = datetime.strptime(last_run, '%Y%m%d%H%M%S')
            last_run_fmt = dt.strftime('%Y-%m-%d %H:%M')
        except Exception:
            pass
    return {"success": True, "data": {
        "auto_backup_enabled":  config.get("auto_backup_enabled", False),
        "auto_backup_interval": config.get("auto_backup_interval", "daily"),
        "auto_backup_time":     config.get("auto_backup_time", "02:00"),
        "backup_retention":     config.get("backup_retention", 20),
        "last_run":             last_run_fmt,
    }}


@app.post("/api/admin/backup-config")
async def api_save_backup_config(
    data: Dict = Body(...),
    session_id: Optional[str] = Cookie(None)
):
    """保存自动备份配置（管理员）"""
    auth.require_admin(session_id)
    config.set("auto_backup_enabled",  bool(data.get("auto_backup_enabled", False)))
    config.set("auto_backup_interval", data.get("auto_backup_interval", "daily"))
    config.set("auto_backup_time",     data.get("auto_backup_time", "02:00"))
    config.set("backup_retention",     int(data.get("backup_retention", 20)))
    return {"success": True, "message": "自动备份配置已保存"}


# ========== 点数体系 API ==========

def _get_or_create_credit_account(user_id: int) -> Dict:
    """获取或初始化用户点数账户"""
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM credit_accounts WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if not row:
        now = datetime.now().strftime('%Y%m%d%H%M%S')
        cursor.execute("""
            INSERT INTO credit_accounts (user_id, balance, gift_balance, total_recharged, updated_at)
            VALUES (?, 0, 0, 0, ?)
        """, (user_id, now))
        conn.commit()
        cursor.execute("SELECT * FROM credit_accounts WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
    conn.close()
    return {k: row[k] for k in row.keys()}


def _deduct_credits(user_id: int, credits: int, description: str, order_id: str = None) -> bool:
    """扣减点数（优先消耗赠送点数），返回是否成功"""
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT balance, gift_balance FROM credit_accounts WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return False
    balance, gift_balance = row['balance'], row['gift_balance']
    total = balance + gift_balance
    if total < credits:
        conn.close()
        return False
    # 优先扣赠送点数
    if gift_balance >= credits:
        new_gift = gift_balance - credits
        new_balance = balance
    else:
        new_gift = 0
        new_balance = balance - (credits - gift_balance)
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    cursor.execute("""
        UPDATE credit_accounts SET balance=?, gift_balance=?, updated_at=? WHERE user_id=?
    """, (new_balance, new_gift, now, user_id))
    cursor.execute("""
        INSERT INTO credit_transactions (user_id, type, credits, balance_after, description, order_id, created_at)
        VALUES (?, 'deduct', ?, ?, ?, ?, ?)
    """, (user_id, -credits, new_balance + new_gift, description, order_id, now))
    conn.commit()
    conn.close()
    return True


def _add_credits(user_id: int, credits: int, is_gift: bool, description: str,
                 order_id: str = None, expires_at: str = None) -> None:
    """增加点数"""
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT balance, gift_balance FROM credit_accounts WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    if row:
        balance, gift_balance = row['balance'], row['gift_balance']
        if is_gift:
            new_gift = gift_balance + credits
            new_balance = balance
        else:
            new_gift = gift_balance
            new_balance = balance + credits
        cursor.execute("""
            UPDATE credit_accounts SET balance=?, gift_balance=?, total_recharged=total_recharged+?,
            updated_at=? WHERE user_id=?
        """, (new_balance, new_gift, 0 if is_gift else credits, now, user_id))
    else:
        new_balance = 0 if is_gift else credits
        new_gift = credits if is_gift else 0
        cursor.execute("""
            INSERT INTO credit_accounts (user_id, balance, gift_balance, total_recharged, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, new_balance, new_gift, 0 if is_gift else credits, now))
    cursor.execute("""
        INSERT INTO credit_transactions (user_id, type, credits, balance_after, description, order_id, expires_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (user_id, 'gift' if is_gift else 'recharge', credits,
          new_balance + new_gift, description, order_id, expires_at, now))
    conn.commit()
    conn.close()


@app.get("/api/credits/packages")
async def get_credit_packages():
    """获取充值包列表"""
    from app.permissions import CREDIT_PACKAGES
    return {"success": True, "data": CREDIT_PACKAGES}


@app.get("/api/credits/balance")
async def get_credit_balance(session_id: Optional[str] = Cookie(None)):
    """查询当前用户点数余额"""
    user = auth.require_auth(session_id)
    account = _get_or_create_credit_account(user['id'])
    return {"success": True, "data": {
        "balance": account['balance'],
        "gift_balance": account['gift_balance'],
        "total": account['balance'] + account['gift_balance'],
    }}


@app.get("/api/credits/transactions")
async def get_credit_transactions(
    page: int = 1,
    session_id: Optional[str] = Cookie(None)
):
    """查询点数流水"""
    user = auth.require_auth(session_id)
    page_size = 20
    offset = (page - 1) * page_size
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, type, credits, balance_after, description, created_at
        FROM credit_transactions WHERE user_id = ?
        ORDER BY created_at DESC LIMIT ? OFFSET ?
    """, (user['id'], page_size, offset))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    type_names = {'recharge': '充值', 'gift': '赠送', 'deduct': '消耗', 'expire': '过期'}
    for r in rows:
        r['type_name'] = type_names.get(r['type'], r['type'])
    return {"success": True, "data": rows}


class CreateCreditOrderRequest(BaseModel):
    package_id: str
    pay_method: str  # alipay / wechat


@app.post("/api/credits/order")
async def create_credit_order(req: CreateCreditOrderRequest, session_id: Optional[str] = Cookie(None)):
    """创建点数充值订单"""
    user = auth.require_auth(session_id)
    from app.permissions import CREDIT_PACKAGES
    pkg = next((p for p in CREDIT_PACKAGES if p['id'] == req.package_id), None)
    if not pkg:
        raise HTTPException(status_code=400, detail="充值包不存在")
    if req.pay_method not in ('alipay', 'wechat'):
        raise HTTPException(status_code=400, detail="支付方式无效")

    now_str = datetime.now().strftime('%Y%m%d%H%M%S')
    import secrets as _secrets
    order_id = f"YJC{now_str}{_secrets.token_hex(3).upper()}"
    expires_at = (datetime.now() + timedelta(minutes=15)).strftime('%Y%m%d%H%M%S')
    amount_fen = pkg['amount']

    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO orders (id, user_id, plan_code, billing, amount, pay_method, status, expires_at, created_at)
        VALUES (?, ?, 'credits', ?, ?, ?, 'pending', ?, ?)
    """, (order_id, user['id'], f"credits_{pkg['id']}", amount_fen,
          req.pay_method, expires_at, now_str))
    conn.commit()
    conn.close()

    way_code_map = {'alipay': 'ALI_QR', 'wechat': 'WX_NATIVE'}
    way_code = way_code_map[req.pay_method]
    subject = f"涌金阁 点数充值 {pkg['name']}（{pkg['credits']}点）"
    cfg = config.config
    try:
        from app.payment import create_jeepay_qr
        qr_content = create_jeepay_qr(order_id, amount_fen, subject, way_code, cfg)
    except Exception as e:
        _logger.error(f"创建点数充值二维码失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return {"success": True, "data": {
        "order_id": order_id,
        "qr_content": qr_content,
        "amount_yuan": amount_fen / 100,
        "credits": pkg['credits'],
        "expires_at": expires_at,
    }}


@app.get("/api/credits/order/{order_id}/status")
async def get_credit_order_status(order_id: str, session_id: Optional[str] = Cookie(None)):
    """轮询点数充值订单状态"""
    user = auth.require_auth(session_id)
    db.expire_stale_orders()
    order = db.get_order(order_id)
    if not order or order['user_id'] != user['id']:
        raise HTTPException(status_code=404, detail="订单不存在")
    return {"success": True, "data": {"status": order['status']}}


@app.get("/api/credits/unlocks/today")
async def get_today_unlocks(session_id: Optional[str] = Cookie(None)):
    """查询今日已解锁的功能列表"""
    user = auth.require_auth(session_id)
    today = datetime.now().strftime('%Y%m%d')
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT permission_code, credits_cost FROM daily_unlocks
        WHERE user_id = ? AND unlock_date = ?
    """, (user['id'], today))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return {"success": True, "data": rows}


class UnlockRequest(BaseModel):
    permission_code: str


@app.post("/api/credits/unlock")
async def unlock_feature(req: UnlockRequest, session_id: Optional[str] = Cookie(None)):
    """点数解锁当日功能"""
    user = auth.require_auth(session_id)
    from app.permissions import CREDIT_UNLOCK_COSTS, PERMISSIONS

    cost = CREDIT_UNLOCK_COSTS.get(req.permission_code)
    if cost is None:
        raise HTTPException(status_code=400, detail="该功能不支持点数解锁")
    if req.permission_code not in PERMISSIONS:
        raise HTTPException(status_code=400, detail="功能不存在")

    today = datetime.now().strftime('%Y%m%d')
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    conn = db.get_connection()
    cursor = conn.cursor()

    # 检查今日是否已解锁
    cursor.execute("""
        SELECT id FROM daily_unlocks WHERE user_id=? AND permission_code=? AND unlock_date=?
    """, (user['id'], req.permission_code, today))
    if cursor.fetchone():
        conn.close()
        return {"success": True, "message": "今日已解锁，无需重复操作", "already_unlocked": True}

    conn.close()

    # 扣减点数
    perm_name = PERMISSIONS[req.permission_code]['name']
    ok = _deduct_credits(user['id'], cost, f"日解锁：{perm_name}")
    if not ok:
        raise HTTPException(status_code=402, detail="点数不足，请先充值")

    # 写入解锁记录
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO daily_unlocks (user_id, permission_code, unlock_date, credits_cost, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (user['id'], req.permission_code, today, cost, now))
    conn.commit()
    conn.close()

    # 解锁成功后清除 session 缓存，下次请求会重新读取含日解锁的权限
    from app.auth import _cache_delete
    _cache_delete(session_id)

    account = _get_or_create_credit_account(user['id'])
    return {"success": True, "message": f"解锁成功，今日可无限使用{perm_name}", "balance_after": account['balance'] + account['gift_balance']}


def _handle_credit_order_paid(order_id: str, user_id: int, billing: str) -> None:
    """点数充值订单支付成功后发放点数"""
    from app.permissions import CREDIT_PACKAGES
    pkg_id = billing.replace('credits_', '')
    pkg = next((p for p in CREDIT_PACKAGES if p['id'] == pkg_id), None)
    if not pkg:
        return
    _get_or_create_credit_account(user_id)
    _add_credits(user_id, pkg['credits'], is_gift=False,
                 description=f"充值 {pkg['name']}（{pkg['credits']}点）",
                 order_id=order_id)


# ========== 注册赠送点数审核 ==========

@app.get("/api/user/gift-status")
async def get_gift_status(session_id: Optional[str] = Cookie(None)):
    """用户查询自己的赠送点数状态"""
    user = auth.get_current_user(session_id)
    if not user:
        raise HTTPException(status_code=401, detail="请先登录")
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT gift_status, gift_amount FROM users WHERE id=?", (user['id'],))
        row = cursor.fetchone()
    if not row:
        return {"success": True, "gift_status": "given", "gift_amount": 0}
    return {"success": True, "gift_status": row['gift_status'], "gift_amount": row['gift_amount']}

@app.get("/api/admin/pending-gifts")
async def admin_get_pending_gifts(session_id: Optional[str] = Cookie(None)):
    """获取待审核赠送点数列表，含同IP账号详情"""
    auth.require_admin(session_id)
    with db.get_connection() as conn:
        cursor = conn.cursor()
        # 待审核用户
        cursor.execute("""
            SELECT id, username, reg_ip, gift_status, gift_amount, created_at
            FROM users WHERE gift_status='pending' ORDER BY created_at DESC
        """)
        pending = [dict(r) for r in cursor.fetchall()]

        # 补充：同IP所有账号 + 各账号最后登录时间
        for u in pending:
            ip = u['reg_ip']
            cursor.execute("""
                SELECT u.id, u.username, u.created_at,
                       MAX(s.created_at) AS last_login
                FROM users u
                LEFT JOIN sessions s ON s.user_id = u.id
                WHERE u.reg_ip = ? AND u.reg_ip != ''
                GROUP BY u.id
                ORDER BY u.created_at ASC
            """, (ip,))
            ip_accounts = [dict(r) for r in cursor.fetchall()]
            u['ip_accounts'] = ip_accounts
            u['ip_total'] = len(ip_accounts)

    return {"success": True, "list": pending}

@app.post("/api/admin/pending-gifts/{user_id}/approve")
async def admin_approve_gift(user_id: int, session_id: Optional[str] = Cookie(None)):
    """审核通过：实际发放冻结的赠送点数"""
    admin = auth.require_admin(session_id)
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT gift_status, gift_amount FROM users WHERE id=?", (user_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="用户不存在")
        if row['gift_status'] != 'pending':
            raise HTTPException(status_code=400, detail="该用户无待审核的赠送点数")
        amount = row['gift_amount']
    _get_or_create_credit_account(user_id)
    expires_at = (datetime.now() + timedelta(days=7)).strftime('%Y%m%d%H%M%S')
    _add_credits(user_id, amount, is_gift=True, description=f'注册赠送（审核通过，7天有效）', expires_at=expires_at)
    with db.get_connection() as conn:
        conn.execute("UPDATE users SET gift_status='approved' WHERE id=?", (user_id,))
        conn.commit()
    return {"success": True, "message": f"已发放 {amount} 点"}

@app.post("/api/admin/pending-gifts/{user_id}/reject")
async def admin_reject_gift(user_id: int, session_id: Optional[str] = Cookie(None)):
    """审核拒绝：不发放点数"""
    auth.require_admin(session_id)
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT gift_status FROM users WHERE id=?", (user_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="用户不存在")
        if row['gift_status'] != 'pending':
            raise HTTPException(status_code=400, detail="该用户无待审核的赠送点数")
        conn.execute("UPDATE users SET gift_status='rejected' WHERE id=?", (user_id,))
        conn.commit()
    return {"success": True, "message": "已拒绝"}


# ========== 工单系统 ==========

TICKET_TYPES = {'bug': '功能异常', 'data': '数据问题', 'payment': '订阅支付', 'other': '其他问题'}
TICKET_STATUS = {'open': '待处理', 'processing': '处理中', 'resolved': '已解决'}

class TicketRequest(BaseModel):
    type: str
    description: str
    page: str = ''

class TicketReplyRequest(BaseModel):
    reply: str
    status: str = 'resolved'

@app.post("/api/tickets")
async def create_ticket(req: TicketRequest, session_id: Optional[str] = Cookie(None)):
    user = auth.get_current_user(session_id)
    if not user:
        raise HTTPException(status_code=401, detail="请先登录")
    if req.type not in TICKET_TYPES:
        raise HTTPException(status_code=400, detail="无效的工单类型")
    if not req.description or len(req.description.strip()) < 5:
        raise HTTPException(status_code=400, detail="描述内容太短，请详细说明问题")
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO tickets (user_id, username, type, description, page, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'open', ?, ?)
        """, (user['id'], user['username'], req.type, req.description.strip(), req.page, now, now))
        ticket_id = cursor.lastrowid
        conn.commit()
    return {"success": True, "message": "工单已提交，我们会尽快处理", "ticket_id": ticket_id}

@app.get("/api/tickets/my")
async def get_my_tickets(session_id: Optional[str] = Cookie(None)):
    user = auth.get_current_user(session_id)
    if not user:
        raise HTTPException(status_code=401, detail="请先登录")
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, type, description, page, status, reply, replied_at, created_at
            FROM tickets WHERE user_id = ? ORDER BY created_at DESC LIMIT 50
        """, (user['id'],))
        rows = [dict(r) for r in cursor.fetchall()]
    for r in rows:
        r['type_name'] = TICKET_TYPES.get(r['type'], r['type'])
        r['status_name'] = TICKET_STATUS.get(r['status'], r['status'])
    return {"success": True, "tickets": rows}

@app.get("/api/admin/tickets")
async def admin_get_tickets(status: Optional[str] = None, session_id: Optional[str] = Cookie(None)):
    auth.require_admin(session_id)
    with db.get_connection() as conn:
        cursor = conn.cursor()
        if status:
            cursor.execute("""
                SELECT id, user_id, username, type, description, page, status, reply, replied_by, replied_at, created_at
                FROM tickets WHERE status = ? ORDER BY created_at DESC
            """, (status,))
        else:
            cursor.execute("""
                SELECT id, user_id, username, type, description, page, status, reply, replied_by, replied_at, created_at
                FROM tickets ORDER BY created_at DESC LIMIT 200
            """)
        rows = [dict(r) for r in cursor.fetchall()]
    for r in rows:
        r['type_name'] = TICKET_TYPES.get(r['type'], r['type'])
        r['status_name'] = TICKET_STATUS.get(r['status'], r['status'])
    return {"success": True, "tickets": rows}

@app.put("/api/admin/tickets/{ticket_id}")
async def admin_reply_ticket(ticket_id: int, req: TicketReplyRequest, session_id: Optional[str] = Cookie(None)):
    admin = auth.require_admin(session_id)
    if req.status not in TICKET_STATUS:
        raise HTTPException(status_code=400, detail="无效的状态")
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM tickets WHERE id = ?", (ticket_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="工单不存在")
        cursor.execute("""
            UPDATE tickets SET reply=?, status=?, replied_by=?, replied_at=?, updated_at=?
            WHERE id=?
        """, (req.reply.strip(), req.status, admin['username'], now, now, ticket_id))
        conn.commit()
    return {"success": True, "message": "回复成功"}
