# -*- coding: utf-8 -*-
"""
LOF基金套利工具 - Web界面
"""

from flask import Flask, render_template, jsonify, request, session, send_file
from functools import wraps
from data_fetcher import LOFDataFetcher
from arbitrage_calculator import ArbitrageCalculator
from arbitrage_recorder_db import ArbitrageRecorderDB
from user_manager_main import UserManagerMain as UserManagerDB
from notification_manager_db import NotificationManagerDB, NotificationType
from fund_data_manager_db import FundDataManagerDB
from background_updater import BackgroundFundUpdater
from cache_manager import cache_manager
from config import LOF_FUNDS, DATA_SOURCE, TRADE_FEES, ARBITRAGE_THRESHOLD
from concurrent.futures import ThreadPoolExecutor, as_completed
from sse_downloader import schedule_daily_download
from database_models import init_database
from webhook_notifier import webhook_notifier
import threading
import time
import secrets
import json
import os
import zipfile
import io
from datetime import datetime, timedelta

_LOF1_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, root_path=_LOF1_DIR)
app.config['JSON_AS_ASCII'] = False
app.jinja_env.globals['STATIC_VER'] = '2'

# SECRET_KEY 持久化：首次生成后保存到文件，重启后复用，避免 session 失效
_secret_key_file = os.path.join(_LOF1_DIR, '.secret_key')
if os.path.exists(_secret_key_file):
    with open(_secret_key_file, 'r') as _f:
        app.config['SECRET_KEY'] = _f.read().strip()
else:
    _generated_key = secrets.token_hex(32)
    with open(_secret_key_file, 'w') as _f:
        _f.write(_generated_key)
    try:
        os.chmod(_secret_key_file, 0o600)  # 仅所有者可读写
    except OSError:
        pass
    app.config['SECRET_KEY'] = _generated_key
app.config['COMPRESS_RESPONSE'] = True  # 启用响应压缩
app.config['TEMPLATES_AUTO_RELOAD'] = True  # 每次请求都重新读取模板
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'   # iframe 场景下允许跨站传递 cookie

# 数据库配置（使用绝对路径，支持从任意目录导入）
DB_PATH = os.path.join(_LOF1_DIR, "lof_arbitrage.db")

# 初始化数据库（首次运行）
if not os.path.exists(DB_PATH):
    print("初始化数据库...")
    init_database(DB_PATH)
    print(f"数据库已初始化: {DB_PATH}")
else:
    # 确保数据库表已创建（包括新添加的 FundData 表）
    try:
        init_database(DB_PATH)
    except Exception as e:
        print(f"数据库初始化警告: {e}")

# 全局变量
_lof_funds_lock = threading.RLock()  # 保护 LOF_FUNDS 全局字典的并发访问
data_fetcher = None
calculator = None
arbitrage_recorder = ArbitrageRecorderDB(DB_PATH)
user_manager = UserManagerDB()  # 统一使用主项目 stock_data.db
notification_manager = NotificationManagerDB(DB_PATH)
fund_data_manager = FundDataManagerDB(DB_PATH)

# 后台基金数据更新器（稍后启动）
background_updater = None

# 后台基金数据更新器
background_updater = BackgroundFundUpdater(DB_PATH, update_interval=DATA_SOURCE.get('update_interval', 60))

# 挂载共享认证钩子：从主项目 session_id Cookie 同步登录状态
from shared_auth import init_shared_auth
init_shared_auth(app, user_manager)

# 登录验证装饰器
def login_required(f):
    """需要登录的装饰器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session or not session.get('logged_in'):
            return jsonify({
                'success': False,
                'message': '请先登录',
                'requires_login': True
            }), 401
        return f(*args, **kwargs)
    return decorated_function

# 管理员权限装饰器
def admin_required(f):
    """需要管理员权限的装饰器（role 由 shared_auth 同步自主项目 session）"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session or not session.get('logged_in'):
            return jsonify({
                'success': False,
                'message': '请先登录',
                'requires_login': True
            }), 401

        if session.get('role') != 'admin':
            return jsonify({
                'success': False,
                'message': '需要管理员权限',
                'requires_admin': True
            }), 403

        return f(*args, **kwargs)
    return decorated_function

def init_fetcher():
    """初始化数据获取器（仅使用SSE数据源）"""
    global data_fetcher, calculator
    # 仅使用SSE数据源，不再需要Tushare token
    
    data_fetcher = LOFDataFetcher()
    calculator = ArbitrageCalculator()

# 初始化
init_fetcher()

# 启动时自动发现所有LOF基金
def auto_discover_funds():
    """启动时自动发现所有LOF基金"""
    global LOF_FUNDS
    
    
    try:
        funds_list = data_fetcher.get_lof_funds_list()
        if funds_list and len(funds_list) > 0:
            funds_dict = {fund['code']: fund['name'] for fund in funds_list}
            # 清空并重新填充，确保使用最新数据
            with _lof_funds_lock:
                LOF_FUNDS.clear()
                LOF_FUNDS.update(funds_dict)
            # 更新缓存
            cache_manager.set('fund_list', funds_dict, 'all')
            print(f"自动发现 {len(funds_dict)} 只LOF基金，总计 {len(LOF_FUNDS)} 只")
        else:
            print(f"警告: 获取到的LOF基金列表为空，将使用默认基金列表（{len(LOF_FUNDS)} 只）")
    except Exception as e:
            print(f"自动发现LOF基金失败: {e}")
            print(f"将使用默认基金列表（{len(LOF_FUNDS)} 只）")
            import traceback
            traceback.print_exc()

# 启动时在后台线程异步发现，不阻塞服务启动
threading.Thread(target=auto_discover_funds, daemon=True, name='lof-discover').start()

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')

@app.route('/api/funds')
def get_funds():
    """获取基金列表（优先从数据库读取，快速响应）"""
    global LOF_FUNDS
    
    # 优先从数据库获取基金列表（基金代码和名称）
    try:
        # 从数据库获取所有基金数据，提取代码和名称
        db_funds_data = fund_data_manager.get_all_funds_data()
        if db_funds_data and len(db_funds_data) > 0:
            # 构建基金字典 {code: name}
            funds_dict = {f['fund_code']: f['fund_name'] for f in db_funds_data}
            # 更新全局变量
            with _lof_funds_lock:
                LOF_FUNDS.update(funds_dict)
            # 更新内存缓存
            cache_manager.set('fund_list', funds_dict, 'all')
            
            return jsonify({
                'funds': funds_dict,
                'success': True,
                'from_database': True,
                'count': len(funds_dict)
            })
    except Exception as e:
        print(f"从数据库获取基金列表失败: {e}")
    
    # 数据库未命中，尝试从内存缓存获取
    cached_funds = cache_manager.get('fund_list', 'all')
    if cached_funds is not None and len(cached_funds) > 0:
        return jsonify({
            'funds': cached_funds,
            'success': True,
            'from_cache': True
        })
    
    # 缓存未命中，尝试从SSE数据源获取
    funds_to_return = LOF_FUNDS.copy() if LOF_FUNDS else {}
    
    # 如果LOF_FUNDS为空，尝试重新获取
    if not funds_to_return or len(funds_to_return) == 0:
        try:
            funds_list = data_fetcher.get_lof_funds_list()
            if funds_list:
                funds_dict = {fund['code']: fund['name'] for fund in funds_list}
                funds_to_return = funds_dict
                # 更新全局变量
                with _lof_funds_lock:
                    LOF_FUNDS.update(funds_dict)
        except Exception as e:
            print(f"重新获取基金列表失败: {e}")
    
    # 缓存结果
    if funds_to_return and len(funds_to_return) > 0:
        cache_manager.set('fund_list', funds_to_return, 'all')
    
    return jsonify({
        'funds': funds_to_return,
        'success': True,
        'from_cache': False
    })

@app.route('/api/funds/all', methods=['GET'])
def get_all_funds_fast():
    """快速获取所有基金数据（直接从数据库，秒级响应）"""
    try:
        # 获取用户设置的阈值（如果已登录）
        user_threshold = ARBITRAGE_THRESHOLD
        user_fees = TRADE_FEES
        if 'logged_in' in session and session.get('logged_in'):
            username = session.get('username')
            user_settings = user_manager.get_user_settings(username)
            user_threshold = user_settings.get('arbitrage_threshold', ARBITRAGE_THRESHOLD)
            user_fees = user_settings.get('trade_fees', TRADE_FEES)
        
        # 创建使用用户设置的计算器
        user_calculator = ArbitrageCalculator(threshold=user_threshold, fees=user_fees)
        
        # 直接从数据库获取所有基金数据（一次性查询，非常快）
        db_funds_data = fund_data_manager.get_all_funds_data()
        
        if not db_funds_data or len(db_funds_data) == 0:
            return jsonify({
                'success': False,
                'message': '数据库中没有基金数据，请等待后台更新',
                'data': []
            })
        
        # 批量查询「价格冻结」基金：price_history 近30天内跨5个不同日期价格完全相同
        frozen_price_funds = set()
        try:
            import sqlite3 as _sqlite3
            _conn = _sqlite3.connect(DB_PATH)
            _cur = _conn.cursor()
            _cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            _cur.execute("""
                SELECT fund_code
                FROM price_history
                WHERE recorded_at >= ?
                GROUP BY fund_code
                HAVING COUNT(DISTINCT DATE(recorded_at)) >= 5 AND COUNT(DISTINCT price) = 1
            """, (_cutoff,))
            frozen_price_funds = {row[0] for row in _cur.fetchall()}
            _conn.close()
        except Exception:
            pass

        # 批量转换为前端需要的格式（使用用户设置重新计算套利）
        results = []
        funds_to_refresh = []

        for db_fund in db_funds_data:
            try:
                price_date = db_fund.get('price_date', '') or ''
                nav_date   = db_fund.get('nav_date', '') or ''
                # 判断场内是否退市：净值近期仍更新 + (价格历史冻结 或 场内价格为0)
                is_exchange_delisted = False
                try:
                    from datetime import date as _date
                    today = _date.today()
                    if nav_date:
                        nd = _date.fromisoformat(nav_date[:10])
                        if (today - nd).days <= 60:   # 净值60天内有更新
                            fund_code_chk = db_fund['fund_code']
                            # 情形1：price_history有记录但价格冻结不变
                            if fund_code_chk in frozen_price_funds:
                                is_exchange_delisted = True
                            # 情形2：场内价格为0，说明完全没有交易所报价（真实退市）
                            elif not db_fund.get('price'):
                                is_exchange_delisted = True
                except Exception:
                    pass

                fund_info = {
                    'code': db_fund['fund_code'],
                    'price': db_fund.get('price', 0),
                    'nav': db_fund.get('nav', 0),
                    'nav_date': nav_date,
                    'change_pct': (db_fund.get('change_pct', 0) / 100) if db_fund.get('change_pct') else 0,
                    'update_time': db_fund.get('updated_at', '')
                }

                result = user_calculator.calculate_arbitrage(fund_info)
                if not result:
                    continue

                fund_code = db_fund['fund_code']
                fund_name = db_fund.get('fund_name', fund_code)
                if fund_name == fund_code:
                    fund_name = LOF_FUNDS.get(fund_code, fund_code)
                result['fund_name'] = fund_name
                result['price_date'] = price_date
                result['is_exchange_delisted'] = is_exchange_delisted

                purchase_limit = db_fund.get('purchase_limit') or {}
                # 只有在 purchase_status 字段真正缺失时才用默认值兜底，
                # 避免把空 dict（价格更新覆盖后的残留值）误判为"开放申购"
                if not purchase_limit.get('purchase_status'):
                    result['purchase_limit'] = {
                        'is_limited': False, 'limit_amount': None,
                        'limit_desc': '开放申购', 'purchase_status': '开放申购'
                    }
                    funds_to_refresh.append(fund_code)
                else:
                    result['purchase_limit'] = purchase_limit

                results.append(result)
            except Exception as e:
                print(f"处理基金 {db_fund.get('fund_code', 'unknown')} 失败: {e}")
                continue
        
        # 如果有需要刷新的基金，全部异步更新（不阻塞响应，后台更新器已定期处理）
        if funds_to_refresh:
            def refresh_purchase_limits_async(codes):
                """异步刷新过期申购状态"""
                for fund_code in codes:
                    try:
                        new_purchase_limit = data_fetcher.get_fund_purchase_limit(fund_code)
                        if new_purchase_limit:
                            fund_data = fund_data_manager.get_fund_data(fund_code)
                            if fund_data:
                                fund_data['purchase_limit'] = new_purchase_limit
                            else:
                                fund_data = {
                                    'fund_code': fund_code,
                                    'fund_name': LOF_FUNDS.get(fund_code, fund_code),
                                    'purchase_limit': new_purchase_limit
                                }
                            fund_data_manager.update_fund_data(fund_code, fund_data)
                    except Exception as e:
                        print(f"异步更新基金 {fund_code} 申购状态失败: {e}")
            threading.Thread(target=refresh_purchase_limits_async, args=(funds_to_refresh,), daemon=True).start()
        
        return jsonify({
            'success': True,
            'data': results,
            'count': len(results),
            'from_database': True
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'data': []
        }), 500

@app.route('/api/funds/opportunities', methods=['GET'])
def get_fund_opportunities():
    """获取满足筛选条件的套利机会（轻量接口，供仪表盘使用）

    查询参数:
      min_pct   float  溢价/折价率绝对值下限（%），默认 0.5
      type      str    'premium'=溢价, 'discount'=折价, ''=全部
      min_net   float  万元净收益下限（元），默认 0
      limit     int    最多返回条数，默认 20
    """
    try:
        min_pct = float(request.args.get('min_pct', 0.5))
        arb_type = request.args.get('type', '')        # 'premium' / 'discount' / ''
        min_net = float(request.args.get('min_net', 0))
        limit = min(int(request.args.get('limit', 20)), 100)

        db_funds_data = fund_data_manager.get_all_funds_data()
        if not db_funds_data:
            return jsonify({'success': True, 'data': [], 'total': 0})

        # 获取退市基金集合（30天内跨5个不同日期价格冻结）
        opp_frozen = set()
        try:
            import sqlite3 as _sq3
            _c = _sq3.connect(DB_PATH)
            _cu = _c.cursor()
            _cutoff_opp = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            _cu.execute("""
                SELECT fund_code FROM price_history
                WHERE recorded_at >= ?
                GROUP BY fund_code
                HAVING COUNT(DISTINCT DATE(recorded_at)) >= 5 AND COUNT(DISTINCT price) = 1
            """, (_cutoff_opp,))
            opp_frozen = {r[0] for r in _cu.fetchall()}
            _c.close()
        except Exception:
            pass

        calc = ArbitrageCalculator()
        results = []
        for db_fund in db_funds_data:
            # 跳过场内退市基金
            nav_date = db_fund.get('nav_date', '') or ''
            if nav_date:
                try:
                    from datetime import date as _d
                    if (_d.today() - _d.fromisoformat(nav_date[:10])).days <= 60:
                        fund_code_chk = db_fund['fund_code']
                        # 价格冻结 或 场内价格为0 均视为场内退市，跳过
                        if fund_code_chk in opp_frozen or not db_fund.get('price'):
                            continue
                except Exception:
                    pass

            fund_info = {
                'code': db_fund['fund_code'],
                'price': db_fund.get('price', 0),
                'nav': db_fund.get('nav', 0),
                'nav_date': nav_date,
                'change_pct': (db_fund.get('change_pct', 0) / 100) if db_fund.get('change_pct') else 0,
                'update_time': db_fund.get('updated_at', ''),
            }
            result = calc.calculate_arbitrage(fund_info)
            if not result or not result.get('has_opportunity'):
                continue

            diff_pct = result.get('price_diff_pct', 0)   # 正=溢价, 负=折价
            abs_pct = abs(diff_pct)
            net_profit = result.get('net_profit_10k', 0)

            # 按溢价率阈值过滤
            if abs_pct < min_pct:
                continue
            # 按类型过滤
            if arb_type == 'premium' and diff_pct <= 0:
                continue
            if arb_type == 'discount' and diff_pct >= 0:
                continue
            # 按万元净收益过滤
            if net_profit < min_net:
                continue

            fund_name = db_fund.get('fund_name', db_fund['fund_code'])
            if fund_name == db_fund['fund_code']:
                fund_name = LOF_FUNDS.get(db_fund['fund_code'], db_fund['fund_code'])
            result['fund_name'] = fund_name
            results.append(result)

        results.sort(key=lambda x: abs(x.get('price_diff_pct', 0)), reverse=True)
        total = len(results)
        return jsonify({'success': True, 'data': results[:limit], 'total': total})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/funds/discover', methods=['GET'])
def discover_funds():
    """动态发现LOF基金列表"""
    try:
        funds_list = data_fetcher.get_lof_funds_list()
        if funds_list:
            funds_dict = {fund['code']: fund['name'] for fund in funds_list}
            # 更新全局基金列表（清空后重新填充，确保使用最新数据）
            with _lof_funds_lock:
                LOF_FUNDS.clear()
                LOF_FUNDS.update(funds_dict)
            # 更新缓存
            cache_manager.set('fund_list', funds_dict, 'all')
            
            return jsonify({
                'success': True,
                'funds': funds_dict,
                'count': len(funds_dict),
                'total_count': len(LOF_FUNDS)
            })
        else:
            return jsonify({
                'success': False,
                'message': '未发现LOF基金，请检查SSE数据源或手动下载Excel文件',
                'funds': {},
                'count': 0
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'发现基金失败: {str(e)}'
        }), 500

@app.route('/api/sse/download', methods=['POST'])
def download_sse_data():
    """手动触发下载SSE数据"""
    try:
        from sse_downloader import SSEDownloader
        downloader = SSEDownloader()
        
        print("手动触发下载SSE数据...")
        result = downloader.download_all()
        
        if result.get('fund_list') or result.get('nav_list'):
            # 下载成功后，重新发现基金
            funds_list = data_fetcher.get_lof_funds_list()
            if funds_list:
                funds_dict = {fund['code']: fund['name'] for fund in funds_list}
                with _lof_funds_lock:
                    LOF_FUNDS.clear()
                    LOF_FUNDS.update(funds_dict)
                cache_manager.set('fund_list', funds_dict, 'all')
            
            return jsonify({
                'success': True,
                'message': '下载完成',
                'result': result,
                'funds_count': len(LOF_FUNDS)
            })
        else:
            return jsonify({
                'success': False,
                'message': '下载失败，请检查Selenium是否安装或网络连接',
                'result': result
            })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': f'下载失败: {str(e)}'
        }), 500

@app.route('/api/fund/<fund_code>')
def get_fund_info(fund_code):
    """获取单个基金信息（带缓存）"""
    try:
        # 获取用户设置的阈值（如果已登录）
        user_threshold = ARBITRAGE_THRESHOLD
        user_fees = TRADE_FEES
        if 'logged_in' in session and session.get('logged_in'):
            username = session.get('username')
            user_settings = user_manager.get_user_settings(username)
            user_threshold = user_settings.get('arbitrage_threshold', ARBITRAGE_THRESHOLD)
            user_fees = user_settings.get('trade_fees', TRADE_FEES)
        
        # 创建使用用户设置的计算器
        user_calculator = ArbitrageCalculator(threshold=user_threshold, fees=user_fees)
        
        # 暂时不使用缓存，因为不同用户的阈值不同
        # cached_result = cache_manager.get('funds_batch', fund_code)
        # if cached_result is not None:
        #     return jsonify({
        #         'success': True,
        #         'data': cached_result,
        #         'from_cache': True
        #     })
        
        fund_info = data_fetcher.get_fund_info(fund_code)
        if not fund_info:
            return jsonify({
                'success': False,
                'message': f'无法获取基金 {fund_code} 的数据'
            }), 404
        
        result = user_calculator.calculate_arbitrage(fund_info)
        if result:
            result['fund_name'] = LOF_FUNDS.get(fund_code, '')
            
            # 保存到缓存
            cache_manager.set('funds_batch', result, fund_code)
            
            return jsonify({
                'success': True,
                'data': result,
                'from_cache': False
            })
        else:
            return jsonify({
                'success': False,
                'message': '无法计算套利机会'
            }), 400
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/funds/batch', methods=['POST'])
def get_funds_batch():
    """批量获取基金信息（带缓存优化）"""
    
    try:
        data = request.get_json()
        requested_codes = data.get('codes', list(LOF_FUNDS.keys()))
        
        # 数据源标准：只处理LOF_FUNDS列表中的基金（包括SSE和akShare补充的50开头基金）
        fund_codes = [code for code in requested_codes if code in LOF_FUNDS]
        
        if len(fund_codes) < len(requested_codes):
            skipped = len(requested_codes) - len(fund_codes)
            print(f"警告: {skipped} 只基金不在SSE列表中，已跳过")
        
        
        # 优化方案2：并行请求处理（使用多线程，显著提升速度）
        results = []
        processed = 0
        errors = 0
        
        # 获取用户设置的阈值（如果已登录）
        user_threshold = ARBITRAGE_THRESHOLD
        user_fees = TRADE_FEES
        if 'logged_in' in session and session.get('logged_in'):
            username = session.get('username')
            user_settings = user_manager.get_user_settings(username)
            user_threshold = user_settings.get('arbitrage_threshold', ARBITRAGE_THRESHOLD)
            user_fees = user_settings.get('trade_fees', TRADE_FEES)
        
        # 创建使用用户设置的计算器
        user_calculator = ArbitrageCalculator(threshold=user_threshold, fees=user_fees)
        
        def process_single_fund(fund_code: str):
            """处理单个基金的函数（优先从数据库读取）"""
            try:
                # 1. 优先从数据库读取
                db_fund_data = fund_data_manager.get_fund_data(fund_code)
                if db_fund_data:
                    # 转换为计算器需要的格式
                    fund_info = {
                        'code': db_fund_data['fund_code'],
                        'price': db_fund_data.get('price', 0),
                        'nav': db_fund_data.get('nav', 0),
                        'nav_date': db_fund_data.get('nav_date', ''),
                        'change_pct': (db_fund_data.get('change_pct', 0) / 100) if db_fund_data.get('change_pct') else 0,
                        'update_time': db_fund_data.get('updated_at', '')
                    }
                    
                    # 使用用户设置重新计算套利（因为阈值可能不同）
                    result = user_calculator.calculate_arbitrage(fund_info)
                    if result:
                        # 优先使用数据库中的基金名称，如果名称是代码，尝试从 LOF_FUNDS 获取
                        fund_name = db_fund_data.get('fund_name', fund_code)
                        if fund_name == fund_code:
                            # 如果名称就是代码，尝试从全局变量获取
                            fund_name = LOF_FUNDS.get(fund_code, fund_code)
                        result['fund_name'] = fund_name
                        result['purchase_limit'] = db_fund_data.get('purchase_limit') or {}
                        return {'success': True, 'result': result, 'fund_code': fund_code, 'from_database': True}
                
                # 2. 数据库未命中，尝试从内存缓存获取
                cache_key = f"{fund_code}_{user_threshold.get('min_profit_rate', 0.005)}"
                cached_result = cache_manager.get('funds_batch', cache_key)
                if cached_result is not None:
                    return {'success': True, 'result': cached_result, 'fund_code': fund_code, 'from_cache': True}
                
                # 3. 缓存未命中，从API获取（这种情况应该很少，因为后台会更新数据库）
                fund_info = data_fetcher.get_fund_info(fund_code)
                if fund_info:
                    result = user_calculator.calculate_arbitrage(fund_info)
                    if result:
                        result['purchase_limit'] = fund_info.get('purchase_limit') or {}
                        fund_name = LOF_FUNDS.get(fund_code, '')
                        if not fund_name:
                            try:
                                fund_name = data_fetcher.get_fund_chinese_name(fund_code) or fund_code
                            except:
                                fund_name = fund_code
                        result['fund_name'] = fund_name
                        
                        # 保存到缓存
                        cache_manager.set('funds_batch', result, cache_key)
                        
                        return {'success': True, 'result': result, 'fund_code': fund_code, 'from_cache': False}
                return {'success': False, 'fund_code': fund_code, 'reason': 'no_data'}
            except Exception as e:
                return {'success': False, 'fund_code': fund_code, 'reason': str(e)}
        
        # 使用线程池并行处理（并发数：30，平衡速度和API限制）
        max_workers = 30
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_fund = {executor.submit(process_single_fund, fund_code): fund_code 
                            for fund_code in fund_codes}
            
            # 收集结果
            for future in as_completed(future_to_fund):
                fund_code = future_to_fund[future]
                try:
                    result_data = future.result()
                    if result_data['success']:
                        result = result_data['result']
                        results.append(result)
                        processed += 1
                        
                        # 检测套利机会并发送通知（仅对已登录用户）
                        # has_opportunity 已经根据用户设置的阈值判断过了（在 ArbitrageCalculator 中）
                        if result.get('has_opportunity') and 'logged_in' in session and session.get('logged_in'):
                            username = session.get('username')
                            if username:
                                try:
                                    fund_code = result.get('fund_code', '')
                                    fund_name = result.get('fund_name', fund_code)
                                    arbitrage_type = result.get('arbitrage_type', '')
                                    profit_rate = result.get('profit_rate', 0)
                                    
                                    # 检查是否已经通知过（避免重复通知）
                                    # 可以通过检查最近的通知来判断
                                    recent_notifications = notification_manager.get_notifications(
                                        username, unread_only=True, limit=10
                                    )
                                    already_notified = any(
                                        n.get('type') == NotificationType.ARBITRAGE_OPPORTUNITY and
                                        n.get('data', {}).get('fund_code') == fund_code and
                                        # 检查是否是最近5分钟内的通知
                                        (datetime.now() - datetime.fromisoformat(n.get('created_at', ''))).total_seconds() < 300
                                        for n in recent_notifications
                                    )
                                    
                                    # has_opportunity 已经根据用户设置的阈值判断过了，这里直接使用
                                    # 只需要检查是否已经通知过，避免重复通知
                                    if not already_notified:
                                        notification_manager.create_notification(
                                            username=username,
                                            notification_type=NotificationType.ARBITRAGE_OPPORTUNITY,
                                            title=f'发现套利机会：{fund_name} ({fund_code})',
                                            content=f'{arbitrage_type}，预期收益率 {profit_rate:.2f}%',
                                            data={
                                                'fund_code': fund_code,
                                                'fund_name': fund_name,
                                                'arbitrage_type': arbitrage_type,
                                                'profit_rate': profit_rate,
                                                'price': result.get('price'),
                                                'nav': result.get('nav'),
                                                'price_diff_pct': result.get('price_diff_pct')
                                            }
                                        )
                                except Exception as e:
                                    print(f"发送套利机会通知失败: {e}")
                    else:
                        errors += 1
                except Exception as e:
                    errors += 1
                    print(f"获取基金 {fund_code} 失败: {e}")
        
        
        # 按收益率排序
        sorted_results = calculator.sort_by_profit(results)
        
        # 分类基金：指数型和股票型
        index_funds = [r for r in sorted_results if LOF_FUNDS.get(r['fund_code'], '').endswith('指数') or '指数' in r.get('fund_name', '')]
        stock_funds = [r for r in sorted_results if r not in index_funds]
        
        # 缓存整个批次结果（用于快速加载）
        # 注意：由于不同用户的阈值不同，不应该缓存整个批次结果
        # 每个基金的结果已经在 process_single_fund 中按用户阈值缓存了
        # 这里不再缓存整个批次，避免不同用户共享缓存
        
        return jsonify({
            'success': True,
            'data': sorted_results,
            'count': len(sorted_results),
            'index_funds': index_funds,
            'stock_funds': stock_funds,
            'index_count': len(index_funds),
            'stock_count': len(stock_funds)
        })
    except Exception as e:
        
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/cache/clear', methods=['POST'])
@admin_required
def clear_cache():
    """清除缓存（仅管理员）"""
    try:
        data = request.get_json() or {}
        cache_type = data.get('cache_type')  # 如果为None，清除所有缓存
        
        cache_manager.clear(cache_type)
        
        return jsonify({
            'success': True,
            'message': f'缓存已清除' + (f'（类型: {cache_type}）' if cache_type else '（全部）')
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/funds/<fund_code>/refresh-purchase-limit', methods=['POST'])
def refresh_fund_purchase_limit(fund_code):
    """强制刷新单个基金的申购状态（实时获取）"""
    try:
        # 实时获取申购状态
        purchase_limit = data_fetcher.get_fund_purchase_limit(fund_code)
        
        if purchase_limit:
            # 更新数据库
            fund_data = fund_data_manager.get_fund_data(fund_code)
            if fund_data:
                fund_data['purchase_limit'] = purchase_limit
            else:
                # 如果基金数据不存在，创建一条记录
                from config import LOF_FUNDS
                fund_name = LOF_FUNDS.get(fund_code, fund_code)
                fund_data = {
                    'fund_code': fund_code,
                    'fund_name': fund_name,
                    'purchase_limit': purchase_limit
                }
            fund_data_manager.update_fund_data(fund_code, fund_data)
            
            return jsonify({
                'success': True,
                'purchase_limit': purchase_limit,
                'message': '申购状态已刷新'
            })
        else:
            return jsonify({
                'success': False,
                'message': '无法获取申购状态'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'刷新失败: {str(e)}'
        }), 500

@app.route('/api/funds/purchase-limits', methods=['POST'])
def get_purchase_limits():
    """批量获取基金限购信息（异步调用，不阻塞主流程）"""
    
    try:
        data = request.get_json()
        fund_codes = data.get('codes', [])
        
        limits = {}
        for fund_code in fund_codes:
            try:
                purchase_limit = data_fetcher.get_fund_purchase_limit(fund_code)
                limits[fund_code] = purchase_limit
            except Exception as e:
                limits[fund_code] = {'is_limited': False, 'limit_amount': None, 'limit_desc': '不限购'}
        
        return jsonify({
            'success': True,
            'limits': limits,
            'count': len(limits)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/arbitrage/check-purchase-limit', methods=['POST'])
@login_required
def check_purchase_limit():
    """检查申购金额是否超过限购（用于实时验证）"""
    
    try:
        data = request.get_json()
        fund_code = data.get('fund_code')
        amount = float(data.get('amount', 0))
        date = data.get('date')
        
        
        if not fund_code or amount <= 0:
            return jsonify({
                'success': True,
                'is_valid': True,
                'message': ''
            })
        
        username = session.get('username')
        
        # 获取基金限购信息
        try:
            purchase_limit = data_fetcher.get_fund_purchase_limit(fund_code)
        except Exception as e:
            purchase_limit = {'is_limited': False, 'limit_amount': None, 'limit_desc': '开放申购', 'purchase_status': '开放申购'}
        
        # 检查是否暂停申购
        purchase_status = purchase_limit.get('purchase_status', '')
        if purchase_status == '暂停申购':
            return jsonify({
                'success': True,
                'is_valid': False,
                'message': '该基金已暂停申购，无法申购',
                'purchase_limit': purchase_limit
            })
        
        # 如果基金不限购（开放申购），直接返回有效
        if not purchase_limit or not purchase_limit.get('is_limited') or not purchase_limit.get('limit_amount'):
            return jsonify({
                'success': True,
                'is_valid': True,
                'message': '',
                'purchase_limit': purchase_limit
            })
        
        limit_amount = float(purchase_limit.get('limit_amount', 0))
        
        # 检查单次申购金额
        if amount > limit_amount:
            limit_display = limit_amount / 10000 if limit_amount >= 10000 else limit_amount
            limit_unit = '万元' if limit_amount >= 10000 else '元'
            return jsonify({
                'success': True,
                'is_valid': False,
                'message': f'单次申购金额超过限购 {limit_display:.2f} {limit_unit}',
                'purchase_limit': purchase_limit,
                'limit_amount': limit_amount
            })
        
        # 获取当天累计申购金额
        if date is None:
            from datetime import datetime
            date = datetime.now().strftime('%Y-%m-%d')
        
        daily_amount = arbitrage_recorder.get_daily_purchase_amount(
            username=username,
            fund_code=fund_code,
            date=date
        )
        
        
        # 检查累计申购金额
        total_amount = daily_amount + amount
        if total_amount > limit_amount:
            remaining = limit_amount - daily_amount
            remaining_display = remaining / 10000 if remaining >= 10000 else remaining
            remaining_unit = '万元' if remaining >= 10000 else '元'
            limit_display = limit_amount / 10000 if limit_amount >= 10000 else limit_amount
            limit_unit = '万元' if limit_amount >= 10000 else '元'
            return jsonify({
                'success': True,
                'is_valid': False,
                'message': f'当天累计申购金额将超过限购 {limit_display:.2f} {limit_unit}，剩余可申购 {remaining_display:.2f} {remaining_unit}',
                'purchase_limit': purchase_limit,
                'limit_amount': limit_amount,
                'daily_amount': daily_amount,
                'remaining': remaining
            })
        
        # 金额有效
        remaining = limit_amount - total_amount
        remaining_display = remaining / 10000 if remaining >= 10000 else remaining
        remaining_unit = '万元' if remaining >= 10000 else '元'
        return jsonify({
            'success': True,
            'is_valid': True,
            'message': f'剩余可申购额度：{remaining_display:.2f} {remaining_unit}',
            'purchase_limit': purchase_limit,
            'limit_amount': limit_amount,
            'daily_amount': daily_amount,
            'remaining': remaining
        })
        
    except Exception as e:
        
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/config')
def get_config():
    """获取配置信息"""
    # 如果用户已登录，返回用户设置，否则返回默认配置
    if 'logged_in' in session and session.get('logged_in'):
        username = session.get('username')
        user_settings = user_manager.get_user_settings(username)
        
        # 合并用户设置和默认配置（数据源配置始终使用全局配置）
        config = {
            'trade_fees': user_settings.get('trade_fees', TRADE_FEES),
            'arbitrage_threshold': user_settings.get('arbitrage_threshold', ARBITRAGE_THRESHOLD),
            'update_interval': user_settings.get('update_interval', DATA_SOURCE['update_interval']),
            'data_sources': DATA_SOURCE  # 数据源配置始终使用全局配置
        }
    else:
        # 未登录用户返回默认配置
        config = {
            'trade_fees': TRADE_FEES,
            'arbitrage_threshold': ARBITRAGE_THRESHOLD,
            'update_interval': DATA_SOURCE['update_interval'],
            'data_sources': DATA_SOURCE
        }
    
    return jsonify({
        'success': True,
        'data': config
    })

@app.route('/api/config', methods=['POST'])
@login_required
def update_config():
    """更新配置（保存到用户设置）"""
    try:
        username = session.get('username')
        data = request.get_json()
        
        # 准备用户设置
        user_settings = {}
        
        # 保存交易费用
        if 'trade_fees' in data:
            user_settings['trade_fees'] = data['trade_fees']
        
        # 保存套利阈值
        if 'arbitrage_threshold' in data:
            user_settings['arbitrage_threshold'] = data['arbitrage_threshold']
        
        # 保存到用户设置
        user_manager.set_user_settings(username, user_settings)
        
        # 清除相关缓存，确保新设置立即生效
        # 清除 funds_batch 相关的所有缓存（因为不同用户的阈值不同）
        cache_manager.clear('funds_batch')
        
        # 更新计算器（全局默认计算器，用于未登录用户）
        global calculator
        calculator = ArbitrageCalculator()
        
        return jsonify({
            'success': True,
            'message': '配置更新成功，缓存已清除，新设置将立即生效'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

# 套利记录相关API
@app.route('/api/arbitrage/records', methods=['POST'])
@login_required
def create_arbitrage_record():
    """创建套利记录"""
    
    try:
        data = request.get_json()
        fund_code = data.get('fund_code')
        fund_name = data.get('fund_name', '')
        arbitrage_type = data.get('arbitrage_type')  # 'premium' or 'discount'
        initial_price = float(data.get('initial_price', 0))
        initial_shares = float(data.get('initial_shares', 0))
        initial_amount = float(data.get('initial_amount', 0))
        initial_date = data.get('initial_date')
        
        
        if not fund_code or not arbitrage_type or initial_price <= 0 or initial_amount <= 0:
            return jsonify({
                'success': False,
                'message': '参数不完整'
            }), 400
        
        username = session.get('username')
        
        # 只对溢价套利（申购）进行限购验证
        if arbitrage_type == 'premium':
            
            # 获取基金限购信息
            try:
                purchase_limit = data_fetcher.get_fund_purchase_limit(fund_code)
            except Exception as e:
                # 如果获取限购信息失败，默认不限购
                purchase_limit = {'is_limited': False, 'limit_amount': None, 'limit_desc': '不限购'}
            
            # 如果基金有限购，进行验证
            if purchase_limit and purchase_limit.get('is_limited') and purchase_limit.get('limit_amount'):
                limit_amount = float(purchase_limit.get('limit_amount', 0))
                
                
                # 检查单次申购金额是否超过限购
                if initial_amount > limit_amount:
                    limit_display = limit_amount / 10000 if limit_amount >= 10000 else limit_amount
                    limit_unit = '万元' if limit_amount >= 10000 else '元'
                    return jsonify({
                        'success': False,
                        'message': f'单次申购金额 {initial_amount:.2f} 元超过限购金额 {limit_display:.2f} {limit_unit}'
                    }), 400
                
                # 获取当天累计申购金额
                if initial_date is None:
                    from datetime import datetime
                    initial_date = datetime.now().strftime('%Y-%m-%d')
                
                daily_amount = arbitrage_recorder.get_daily_purchase_amount(
                    username=username,
                    fund_code=fund_code,
                    date=initial_date
                )
                
                
                # 检查累计申购金额是否超过限购
                total_amount = daily_amount + initial_amount
                if total_amount > limit_amount:
                    remaining = limit_amount - daily_amount
                    remaining_display = remaining / 10000 if remaining >= 10000 else remaining
                    remaining_unit = '万元' if remaining >= 10000 else '元'
                    limit_display = limit_amount / 10000 if limit_amount >= 10000 else limit_amount
                    limit_unit = '万元' if limit_amount >= 10000 else '元'
                    return jsonify({
                        'success': False,
                        'message': f'当天累计申购金额 {total_amount:.2f} 元超过限购金额 {limit_display:.2f} {limit_unit}，剩余可申购 {remaining_display:.2f} {remaining_unit}'
                    }), 400
        
        # 获取用户的交易费用设置
        user_fees = TRADE_FEES
        if username:
            user_settings = user_manager.get_user_settings(username)
            user_fees = user_settings.get('trade_fees', TRADE_FEES)
        
        # 获取初始操作类型（用于溢价套利，区分场内/场外申购）
        initial_operation_type = data.get('initial_operation_type')  # 'on_exchange' 或 'off_exchange'
        
        record_id = arbitrage_recorder.create_record(
            fund_code=fund_code,
            fund_name=fund_name,
            arbitrage_type=arbitrage_type,
            initial_price=initial_price,
            initial_shares=initial_shares,
            initial_amount=initial_amount,
            initial_date=initial_date,
            username=username,
            trade_fees=user_fees,
            initial_operation_type=initial_operation_type
        )
        
        
        return jsonify({
            'success': True,
            'record_id': record_id,
            'message': '套利记录创建成功'
        })
    except Exception as e:
        
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/arbitrage/records/<record_id>/complete', methods=['POST'])
@login_required
def complete_arbitrage_record(record_id):
    """完成套利记录"""
    try:
        data = request.get_json()
        final_price = float(data.get('final_price', 0))
        final_shares = data.get('final_shares')
        final_amount = data.get('final_amount')
        final_date = data.get('final_date')
        
        if final_price <= 0:
            return jsonify({
                'success': False,
                'message': '最终价格必须大于0'
            }), 400
        
        if final_shares is not None:
            final_shares = float(final_shares)
        if final_amount is not None:
            final_amount = float(final_amount)
        
        username = session.get('username')
        
        # 获取用户的交易费用设置
        user_fees = TRADE_FEES
        if username:
            user_settings = user_manager.get_user_settings(username)
            user_fees = user_settings.get('trade_fees', TRADE_FEES)
        
        success = arbitrage_recorder.complete_record(
            record_id=record_id,
            final_price=final_price,
            username=username,
            final_shares=final_shares,
            final_amount=final_amount,
            final_date=final_date,
            trade_fees=user_fees
        )
        
        if success:
            username = session.get('username')
            record = arbitrage_recorder.get_record(record_id, username)
            
            # 发送套利完成通知
            if record:
                try:
                    fund_code = record.get('fund_code', '')
                    fund_name = record.get('fund_name', fund_code)
                    arbitrage_type = record.get('arbitrage_type', '')
                    profit = record.get('profit', 0)
                    profit_rate = record.get('profit_rate', 0)
                    
                    # 判断是溢价套利还是折价套利
                    if arbitrage_type == 'premium' or '溢价' in str(arbitrage_type):
                        # 溢价套利：场外申购 → 场内卖出，卖出时通知
                        notification_manager.create_notification(
                            username=username,
                            notification_type=NotificationType.ARBITRAGE_SELL,
                            title=f'套利卖出提醒：{fund_name} ({fund_code})',
                            content=f'溢价套利已完成，可进行场内卖出操作。预期收益 {profit:.2f} 元（{profit_rate:.2f}%）',
                            data={
                                'fund_code': fund_code,
                                'fund_name': fund_name,
                                'arbitrage_type': 'premium',
                                'profit': profit,
                                'profit_rate': profit_rate,
                                'record_id': record_id
                            }
                        )
                    
                    # 发送套利完成通知
                    notification_manager.create_notification(
                        username=username,
                        notification_type=NotificationType.ARBITRAGE_COMPLETED,
                        title=f'套利交易完成：{fund_name} ({fund_code})',
                        content=f'套利交易已完成，最终收益 {profit:.2f} 元（{profit_rate:.2f}%）',
                        data={
                            'fund_code': fund_code,
                            'fund_name': fund_name,
                            'arbitrage_type': arbitrage_type,
                            'profit': profit,
                            'profit_rate': profit_rate,
                            'record_id': record_id
                        }
                    )
                except Exception as e:
                    print(f"发送套利完成通知失败: {e}")
            
            return jsonify({
                'success': True,
                'record': record,
                'message': '套利记录完成'
            })
        else:
            return jsonify({
                'success': False,
                'message': '完成记录失败，记录不存在或状态不正确'
            }), 400
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/arbitrage/records', methods=['GET'])
@login_required
def get_arbitrage_records():
    """获取套利记录列表"""
    try:
        username = session.get('username')
        fund_code = request.args.get('fund_code')
        status = request.args.get('status')
        
        records = arbitrage_recorder.get_all_records(fund_code=fund_code, status=status, username=username)
        
        return jsonify({
            'success': True,
            'records': records,
            'count': len(records)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/arbitrage/records/<record_id>', methods=['GET'])
@login_required
def get_arbitrage_record(record_id):
    """获取单条套利记录"""
    try:
        username = session.get('username')
        record = arbitrage_recorder.get_record(record_id, username=username)
        if record:
            return jsonify({
                'success': True,
                'record': record
            })
        else:
            return jsonify({
                'success': False,
                'message': '记录不存在'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/arbitrage/records/<record_id>', methods=['DELETE'])
@login_required
def delete_arbitrage_record(record_id):
    """删除套利记录"""
    try:
        username = session.get('username')
        success = arbitrage_recorder.delete_record(record_id, username=username)
        if success:
            return jsonify({
                'success': True,
                'message': '记录已删除'
            })
        else:
            return jsonify({
                'success': False,
                'message': '记录不存在'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/arbitrage/records/<record_id>/cancel', methods=['POST'])
@login_required
def cancel_arbitrage_record(record_id):
    """取消套利记录"""
    try:
        username = session.get('username')
        success = arbitrage_recorder.cancel_record(record_id, username=username)
        if success:
            return jsonify({
                'success': True,
                'message': '记录已取消'
            })
        else:
            return jsonify({
                'success': False,
                'message': '记录不存在'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/arbitrage/statistics', methods=['GET'])
@login_required
def get_arbitrage_statistics():
    """获取套利统计信息"""
    try:
        username = session.get('username')
        stats = arbitrage_recorder.get_statistics(username)
        return jsonify({
            'success': True,
            'statistics': stats
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

# ==================== 管理员套利记录管理API ====================

@app.route('/api/admin/arbitrage/records', methods=['GET'])
@admin_required
def get_all_arbitrage_records():
    """获取所有用户的套利记录（仅管理员）"""
    try:
        fund_code = request.args.get('fund_code')
        status = request.args.get('status')
        
        records = arbitrage_recorder.get_all_records(
            fund_code=fund_code,
            status=status
        )
        
        return jsonify({
            'success': True,
            'records': records,
            'count': len(records)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/admin/arbitrage/statistics', methods=['GET'])
@admin_required
def get_all_arbitrage_statistics():
    """获取所有用户的套利统计信息（仅管理员）"""
    try:
        statistics = arbitrage_recorder.get_all_users_statistics()
        return jsonify({
            'success': True,
            'statistics': statistics
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

# ==================== 用户认证相关API ====================

# 初始化防护模块
from captcha import CaptchaManager
from rate_limiter import RateLimiter
from datetime import datetime

captcha_manager = CaptchaManager()
rate_limiter = RateLimiter()

@app.route('/api/auth/captcha', methods=['GET'])
def get_captcha():
    """获取验证码"""
    try:
        session_id = session.get('id', str(time.time()))
        if 'id' not in session:
            session['id'] = session_id
        
        captcha_data = captcha_manager.generate_captcha(session_id)
        # 不返回答案给客户端
        return jsonify({
            'success': True,
            'question': captcha_data['question']
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/auth/check-username', methods=['GET'])
def check_username():
    """检查用户名是否可用"""
    try:
        username = request.args.get('username', '').strip()
        
        if not username:
            return jsonify({
                'available': False,
                'message': '用户名不能为空'
            }), 400
        
        # 验证用户名格式
        if len(username) < 3:
            return jsonify({
                'available': False,
                'message': '用户名至少需要3个字符'
            }), 400
        
        if len(username) > 20:
            return jsonify({
                'available': False,
                'message': '用户名不能超过20个字符'
            }), 400
        
        if not username.isalnum():
            return jsonify({
                'available': False,
                'message': '用户名只能包含字母和数字'
            }), 400
        
        # 检查用户名是否已存在（使用user_manager）
        existing_user = user_manager.get_user(username)
        if existing_user:
            return jsonify({
                'available': False,
                'message': '用户名已存在'
            })
        else:
            return jsonify({
                'available': True,
                'message': '用户名可用'
            })
    except Exception as e:
        return jsonify({
            'available': False,
            'message': f'检查失败: {str(e)}'
        }), 500

@app.route('/api/auth/register', methods=['POST'])
def register():
    """用户注册"""
    try:
        data = request.get_json()
        username = (data.get('username') or '').strip()
        password = data.get('password') or ''
        email = (data.get('email') or '').strip() or None
        captcha_answer = data.get('captcha_answer', '').strip()
        
        
        # 获取客户端IP
        client_ip = request.remote_addr or request.environ.get('HTTP_X_FORWARDED_FOR', '').split(',')[0] or 'unknown'
        
        # 检查频率限制
        rate_ok, rate_message = rate_limiter.check_rate_limit(client_ip, username)
        if not rate_ok:
            rate_limiter.record_attempt(client_ip, username, success=False)
            return jsonify({
                'success': False,
                'message': rate_message
            }), 429  # Too Many Requests
        
        if not username or not password:
            rate_limiter.record_attempt(client_ip, username, success=False)
            return jsonify({
                'success': False,
                'message': '用户名和密码不能为空'
            }), 400
        
        # 验证验证码
        session_id = session.get('id', str(time.time()))
        if 'id' not in session:
            session['id'] = session_id
        
        captcha_ok, captcha_message = captcha_manager.verify_captcha(session_id, captcha_answer)
        if not captcha_ok:
            rate_limiter.record_attempt(client_ip, username, success=False)
            return jsonify({
                'success': False,
                'message': captcha_message
            }), 400
        
        
        success, message = user_manager.register(username, password, email=email)
        
        
        if success:
            # 记录成功的注册
            rate_limiter.record_attempt(client_ip, username, success=True)
            return jsonify({
                'success': True,
                'message': message
            })
        else:
            # 记录失败的注册尝试
            rate_limiter.record_attempt(client_ip, username, success=False)
            return jsonify({
                'success': False,
                'message': message
            }), 400
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/auth/login', methods=['POST'])
def login():
    """登录已由主界面统一管理，此端点仅做兼容处理"""
    # 检查是否已通过主项目登录
    if session.get('logged_in') and session.get('username'):
        user_info = user_manager.get_user(session['username'])
        return jsonify({'success': True, 'message': '已登录', 'user': user_info})
    return jsonify({
        'success': False,
        'message': '请在主界面登录后使用此功能',
        'requires_main_login': True
    }), 401

@app.route('/api/auth/logout', methods=['POST'])
def logout():
    """登出由主界面统一管理"""
    # 不清除 Flask session（session 由 before_request 从主项目同步，清了也会恢复）
    # 直接返回成功，告知前端跳转到主界面登出
    return jsonify({
        'success': True,
        'message': '请在主界面操作登出',
        'redirect': '/'
    })

@app.route('/api/auth/current', methods=['GET'])
def get_current_user():
    """获取当前登录用户信息（认证状态来自主项目 session）"""
    try:
        if session.get('logged_in') and session.get('username'):
            username = session['username']
            # role 由 shared_auth 同步自主项目，直接用 session 值最准确
            role = session.get('role', 'user')
            # 从主项目 DB 获取扩展信息（last_login 等）
            user_info = user_manager.get_user(username)
            if not user_info:
                user_info = {'username': username, 'role': role,
                             'email': None, 'created_at': None, 'last_login': None}
            else:
                # 用主项目 session 里的 role 覆盖，确保与主项目一致
                user_info['role'] = role
            return jsonify({'success': True, 'user': user_info})

        return jsonify({'success': False, 'message': '未登录'}), 401
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# ==================== 用户数据相关API ====================

@app.route('/api/user/favorites', methods=['GET'])
@login_required
def get_user_favorites():
    """获取用户的自选基金列表"""
    username = session.get('username')
    favorites = user_manager.get_user_favorites(username)
    return jsonify({
        'success': True,
        'favorites': favorites
    })

@app.route('/api/user/favorites', methods=['POST'])
@login_required
def update_user_favorites():
    """更新用户的自选基金列表"""
    try:
        username = session.get('username')
        data = request.get_json()
        fund_codes = data.get('favorites', [])
        
        success = user_manager.set_user_favorites(username, fund_codes)
        if success:
            return jsonify({
                'success': True,
                'message': '自选基金已更新'
            })
        else:
            return jsonify({
                'success': False,
                'message': '更新失败'
            }), 400
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/user/favorites/<fund_code>', methods=['POST'])
@login_required
def add_user_favorite(fund_code):
    """添加自选基金"""
    try:
        username = session.get('username')
        success = user_manager.add_user_favorite(username, fund_code)
        if success:
            return jsonify({
                'success': True,
                'message': '已添加到自选'
            })
        else:
            return jsonify({
                'success': False,
                'message': '添加失败'
            }), 400
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/user/favorites/<fund_code>', methods=['DELETE'])
@login_required
def remove_user_favorite(fund_code):
    """移除自选基金"""
    try:
        username = session.get('username')
        success = user_manager.remove_user_favorite(username, fund_code)
        if success:
            return jsonify({
                'success': True,
                'message': '已从自选移除'
            })
        else:
            return jsonify({
                'success': False,
                'message': '移除失败'
            }), 400
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/user/settings', methods=['GET'])
@login_required
def get_user_settings():
    """获取用户的设置"""
    username = session.get('username')
    settings = user_manager.get_user_settings(username)
    return jsonify({
        'success': True,
        'settings': settings
    })

@app.route('/api/user/settings', methods=['POST'])
@login_required
def update_user_settings():
    """更新用户的设置"""
    try:
        username = session.get('username')
        data = request.get_json()
        settings = data.get('settings', {})
        
        success = user_manager.set_user_settings(username, settings)
        if success:
            return jsonify({
                'success': True,
                'message': '设置已保存'
            })
        else:
            return jsonify({
                'success': False,
                'message': '保存失败'
            }), 400
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

# ==================== 数据源配置相关API（仅管理员） ====================

@app.route('/api/data-sources/config', methods=['GET'])
@admin_required
def get_data_source_config():
    """获取数据源配置（仅管理员）"""
    return jsonify({
        'success': True,
        'data': {
            'update_interval': DATA_SOURCE.get('update_interval', 60),
            'data_sources': DATA_SOURCE
        }
    })

@app.route('/api/data-sources/config', methods=['POST'])
@admin_required
def update_data_source_config():
    """更新数据源配置（仅管理员）"""
    try:
        data = request.get_json()
        data_sources = data.get('data_sources', {})
        
        # 更新全局数据源配置
        global DATA_SOURCE, background_updater
        
        # 更新更新间隔
        if 'update_interval' in data_sources:
            DATA_SOURCE['update_interval'] = data_sources['update_interval']
            # 更新后台更新器的更新间隔
            if background_updater:
                background_updater.update_interval = data_sources['update_interval']
        
        # 更新数据时效性配置
        if 'data_freshness' in data_sources:
            if 'data_freshness' not in DATA_SOURCE:
                DATA_SOURCE['data_freshness'] = {}
            DATA_SOURCE['data_freshness'].update(data_sources['data_freshness'])
            
            # 更新后台更新器的数据时效性配置
            if background_updater:
                freshness = data_sources['data_freshness']
                background_updater.price_nav_max_age_seconds = freshness.get('price_nav_max_age_seconds', 300)
                background_updater.purchase_limit_max_age_seconds = freshness.get('purchase_limit_max_age_seconds', 600)
                background_updater.purchase_limit_update_interval = freshness.get('purchase_limit_update_interval', 600)
                print(f"后台更新器配置已更新: 价格/净值过期={background_updater.price_nav_max_age_seconds}秒, "
                      f"申购状态过期={background_updater.purchase_limit_max_age_seconds}秒, "
                      f"申购状态刷新间隔={background_updater.purchase_limit_update_interval}秒")
        
        # 更新各数据源配置
        for source_type in ['price_sources', 'nav_sources', 'fund_list_sources', 'name_sources', 'purchase_limit_sources']:
            if source_type in data_sources:
                if source_type not in DATA_SOURCE:
                    DATA_SOURCE[source_type] = {}
                for source_name, source_config in data_sources[source_type].items():
                    if source_name not in DATA_SOURCE[source_type]:
                        DATA_SOURCE[source_type][source_name] = {}
                    DATA_SOURCE[source_type][source_name].update(source_config)
        
        # 如果更新了SSE配置，重新初始化fetcher
        if 'fund_list_sources' in data_sources and 'sse' in data_sources['fund_list_sources']:
            # 重新初始化数据获取器（仅SSE，不再需要token）
            global data_fetcher
            data_fetcher = LOFDataFetcher()
            data_fetcher.data_source_config = DATA_SOURCE
        
        return jsonify({
            'success': True,
            'message': '数据源配置已更新',
            'background_updater_restarted': True
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

# ==================== 用户管理相关API（仅管理员） ====================

# ==================== 数据库备份和还原API（仅管理员） ====================

_MAIN_DB = os.path.join(os.path.dirname(_LOF1_DIR), "stock_data.db")
# 需要从主库提取备份的用户相关表
_USER_TABLES = ['users', 'user_permissions', 'lof_user_favorites', 'lof_user_settings']

def _export_user_tables_to_sqlite() -> bytes:
    """将主库中用户相关表导出为独立 SQLite 文件（内存），返回字节流。"""
    import sqlite3 as _sqlite3
    dst = _sqlite3.connect(':memory:')
    src = _sqlite3.connect(_MAIN_DB, timeout=5)
    try:
        src_cur = src.cursor()
        dst_cur = dst.cursor()
        for table in _USER_TABLES:
            # 复制表结构
            src_cur.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
            row = src_cur.fetchone()
            if not row:
                continue
            dst_cur.execute(row[0])
            # 复制数据
            src_cur.execute(f"SELECT * FROM {table}")
            rows = src_cur.fetchall()
            if rows:
                placeholders = ','.join(['?'] * len(rows[0]))
                dst_cur.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
        dst.commit()
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tf:
            tmp_path = tf.name
        dst.execute("VACUUM INTO ?", (tmp_path,))
        with open(tmp_path, 'rb') as f:
            data = f.read()
        os.unlink(tmp_path)
        return data
    finally:
        src.close()
        dst.close()

@app.route('/api/admin/backup', methods=['GET'])
@admin_required
def backup_database():
    """备份数据库：lof_arbitrage.db + 主库用户相关表快照"""
    try:
        zip_buffer = io.BytesIO()
        backed_files = []
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # 1. 备份 lof_arbitrage.db
            if os.path.exists(DB_PATH):
                with open(DB_PATH, 'rb') as f:
                    zip_file.writestr('lof_arbitrage.db', f.read())
                backed_files.append('lof_arbitrage.db')

            # 2. 备份用户相关表快照（来自主库 stock_data.db）
            if os.path.exists(_MAIN_DB):
                try:
                    user_db_bytes = _export_user_tables_to_sqlite()
                    zip_file.writestr('user_data.db', user_db_bytes)
                    backed_files.append('user_data.db')
                except Exception as e:
                    print(f"备份用户数据失败（已跳过）: {e}")

            backup_info = {
                'backup_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'backup_version': '3.0',
                'files': backed_files,
                'user_tables': _USER_TABLES,
            }
            zip_file.writestr('backup_info.json', json.dumps(backup_info, ensure_ascii=False, indent=2))

        zip_buffer.seek(0)
        backup_filename = f'lof_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
        return send_file(zip_buffer, mimetype='application/zip', as_attachment=True, download_name=backup_filename)
    except Exception as e:
        return jsonify({'success': False, 'message': f'备份失败: {str(e)}'}), 500

@app.route('/api/admin/restore', methods=['POST'])
@admin_required
def restore_database():
    """还原数据库（从备份文件恢复 SQLite 数据库）"""
    try:
        # 检查是否有文件上传
        if 'backup_file' not in request.files:
            return jsonify({
                'success': False,
                'message': '请选择备份文件'
            }), 400
        
        backup_file = request.files['backup_file']
        if backup_file.filename == '':
            return jsonify({
                'success': False,
                'message': '请选择备份文件'
            }), 400
        
        # 读取ZIP文件
        zip_buffer = io.BytesIO(backup_file.read())
        
        restored_files = []
        failed_files = []
        
        try:
            with zipfile.ZipFile(zip_buffer, 'r') as zip_file:
                # 检查备份信息
                backup_info = None
                if 'backup_info.json' in zip_file.namelist():
                    backup_info_content = zip_file.read('backup_info.json').decode('utf-8')
                    backup_info = json.loads(backup_info_content)
                    print(f"还原备份文件，备份日期: {backup_info.get('backup_date', '未知')}")
                
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')

                # 1. 还原 lof_arbitrage.db
                if 'lof_arbitrage.db' in zip_file.namelist():
                    try:
                        db_content = zip_file.read('lof_arbitrage.db')
                        if os.path.exists(DB_PATH):
                            import shutil
                            shutil.copy2(DB_PATH, f"{DB_PATH}.backup_{ts}")
                        with open(DB_PATH, 'wb') as f:
                            f.write(db_content)
                        restored_files.append('lof_arbitrage.db')
                        print(f"lof_arbitrage.db 已还原")
                    except Exception as e:
                        failed_files.append(f"lof_arbitrage.db ({e})")
                elif any(n.endswith('.json') for n in zip_file.namelist() if n != 'backup_info.json'):
                    return jsonify({'success': False,
                                    'message': '检测到旧格式备份（JSON文件），不支持直接还原'}), 400
                else:
                    return jsonify({'success': False, 'message': '备份文件中未找到 lof_arbitrage.db'}), 400

                # 2. 还原用户相关表快照（user_data.db → stock_data.db）
                if 'user_data.db' in zip_file.namelist() and os.path.exists(_MAIN_DB):
                    try:
                        import sqlite3 as _sqlite3, shutil
                        user_db_bytes = zip_file.read('user_data.db')
                        # 写到临时文件再读取
                        import tempfile
                        with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tf:
                            tmp_path = tf.name
                            tf.write(user_db_bytes)
                        src = _sqlite3.connect(tmp_path)
                        dst = _sqlite3.connect(_MAIN_DB, timeout=10)
                        try:
                            # 备份主库
                            shutil.copy2(_MAIN_DB, f"{_MAIN_DB}.backup_{ts}")
                            dst_cur = dst.cursor()
                            for table in _USER_TABLES:
                                src_cur = src.cursor()
                                src_cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
                                if not src_cur.fetchone():
                                    continue
                                src_cur.execute(f"SELECT * FROM {table}")
                                rows = src_cur.fetchall()
                                dst_cur.execute(f"DELETE FROM {table}")
                                if rows:
                                    placeholders = ','.join(['?'] * len(rows[0]))
                                    dst_cur.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
                            dst.commit()
                            restored_files.append('user_data.db → stock_data.db')
                            print("用户数据已还原到 stock_data.db")
                        finally:
                            src.close()
                            dst.close()
                            os.unlink(tmp_path)
                    except Exception as e:
                        failed_files.append(f"user_data.db ({e})")
        except zipfile.BadZipFile:
            return jsonify({
                'success': False,
                'message': '无效的备份文件格式'
            }), 400
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'还原过程中出错: {str(e)}'
            }), 500
        
        # 重新初始化数据库管理器（重新连接数据库）
        try:
            global arbitrage_recorder, user_manager, notification_manager
            arbitrage_recorder = ArbitrageRecorderDB(DB_PATH)
            user_manager = UserManagerDB()
            notification_manager = NotificationManagerDB(DB_PATH)
            print("数据库管理器已重新初始化")
        except Exception as e:
            print(f"重新初始化数据库管理器失败: {e}")
        
        # 返回结果
        message = f'成功还原 {len(restored_files)} 个文件'
        if failed_files:
            message += f'，失败 {len(failed_files)} 个文件: {", ".join(failed_files)}'
        
        return jsonify({
            'success': True,
            'message': message,
            'restored_files': restored_files,
            'failed_files': failed_files
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'还原失败: {str(e)}'
        }), 500

# ==================== 通知相关API ====================

@app.route('/api/notifications', methods=['GET'])
@login_required
def get_notifications():
    """获取用户通知列表"""
    try:
        username = session.get('username')
        unread_only = request.args.get('unread_only', 'false').lower() == 'true'
        limit = request.args.get('limit', type=int)
        
        notifications = notification_manager.get_notifications(
            username, unread_only=unread_only, limit=limit
        )
        
        return jsonify({
            'success': True,
            'notifications': notifications,
            'count': len(notifications)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/notifications/unread-count', methods=['GET'])
@login_required
def get_unread_count():
    """获取未读通知数量"""
    try:
        username = session.get('username')
        count = notification_manager.get_unread_count(username)
        return jsonify({
            'success': True,
            'count': count
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/notifications/<notification_id>/read', methods=['POST'])
@login_required
def mark_notification_read(notification_id):
    """标记通知为已读"""
    try:
        username = session.get('username')
        success = notification_manager.mark_as_read(username, notification_id)
        if success:
            return jsonify({
                'success': True,
                'message': '已标记为已读'
            })
        else:
            return jsonify({
                'success': False,
                'message': '通知不存在'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/notifications/read-all', methods=['POST'])
@login_required
def mark_all_notifications_read():
    """标记所有通知为已读"""
    try:
        username = session.get('username')
        count = notification_manager.mark_all_as_read(username)
        return jsonify({
            'success': True,
            'message': f'已标记 {count} 条通知为已读',
            'count': count
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/notifications/<notification_id>', methods=['DELETE'])
@login_required
def delete_notification(notification_id):
    """删除通知"""
    try:
        username = session.get('username')
        success = notification_manager.delete_notification(username, notification_id)
        if success:
            return jsonify({
                'success': True,
                'message': '通知已删除'
            })
        else:
            return jsonify({
                'success': False,
                'message': '通知不存在'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/notifications/delete-read', methods=['POST'])
@login_required
def delete_read_notifications():
    """删除所有已读通知"""
    try:
        username = session.get('username')
        count = notification_manager.delete_all_read(username)
        return jsonify({
            'success': True,
            'message': f'已删除 {count} 条已读通知',
            'count': count
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

# ==================== 交易日历风控 API ====================

@app.route('/api/trading-calendar/risk-tips', methods=['GET'])
def get_trading_risk_tips():
    """
    获取套利时间风控提示

    Query params:
        type   premium | discount   套利类型
        date   YYYY-MM-DD           起始日期（默认今天）
    """
    try:
        from trading_calendar import get_risk_tips
        arb_type = request.args.get('type', 'premium')
        date_str = request.args.get('date', '')
        start_date = None
        if date_str:
            try:
                from datetime import date as _date
                start_date = _date.fromisoformat(date_str)
            except ValueError:
                pass

        tips = get_risk_tips(arb_type, start_date)
        return jsonify({'success': True, 'tips': tips})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ==================== 历史折溢价率 API ====================

@app.route('/api/fund/<fund_code>/history', methods=['GET'])
def get_fund_price_history(fund_code):
    """
    获取某只基金的历史折溢价率走势数据

    Query params:
        days   int   查询天数（默认 7，支持 7/30/90）
    """
    try:
        days = int(request.args.get('days', 7))
        days = max(1, min(days, 365))

        from database_models import get_db_manager, PriceHistory
        from datetime import timedelta
        db = get_db_manager(DB_PATH)
        session = db.get_session()
        try:
            cutoff = datetime.now() - timedelta(days=days)
            rows = (session.query(PriceHistory)
                    .filter(PriceHistory.fund_code == fund_code,
                            PriceHistory.recorded_at >= cutoff)
                    .order_by(PriceHistory.recorded_at.asc())
                    .all())

            history = [{
                'time': r.recorded_at.strftime('%Y-%m-%d %H:%M'),
                'price': r.price,
                'nav': r.nav,
                'price_diff_pct': round(r.price_diff_pct or 0, 3),
                'profit_rate': round(r.profit_rate or 0, 3),
            } for r in rows]

            # 附上基金名称
            from config import LOF_FUNDS
            fund_name = LOF_FUNDS.get(fund_code, fund_code)

            return jsonify({
                'success': True,
                'fund_code': fund_code,
                'fund_name': fund_name,
                'days': days,
                'count': len(history),
                'history': history,
            })
        finally:
            session.close()
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ==================== 套利辅助 API ====================

@app.route('/api/arbitrage/calculate', methods=['POST'])
@login_required
def calculate_arbitrage_custom():
    """
    自定义费率套利计算器。
    支持传入自定义券商佣金，返回收益结果并与默认费率对比。

    Body (JSON):
        fund_code  str   基金代码（必填）
        amount     float 模拟投入金额（元），默认 10000
        fees       dict  自定义费率（可选，只传需要覆盖的字段）
                         subscribe_fee / redeem_fee / buy_commission /
                         sell_commission / stamp_tax
    """
    try:
        data = request.get_json() or {}
        fund_code = data.get('fund_code', '').strip()
        if not fund_code:
            return jsonify({'success': False, 'message': '请传入 fund_code'}), 400

        amount = float(data.get('amount', 10000))
        if amount <= 0:
            return jsonify({'success': False, 'message': 'amount 必须大于 0'}), 400

        custom_fees = data.get('fees') or {}

        # 获取基金实时数据
        fund_info = data_fetcher.get_fund_info(fund_code)
        if not fund_info:
            return jsonify({'success': False, 'message': f'获取基金数据失败，代码 {fund_code} 可能不存在'}), 404

        result = calculator.calculate_with_custom_fees(fund_info, custom_fees, amount)
        if not result:
            return jsonify({'success': False, 'message': '无法计算套利收益（价格或净值数据缺失）'}), 400

        # 附上基金名称
        from config import LOF_FUNDS
        result['fund_name'] = LOF_FUNDS.get(fund_code, fund_code)

        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/arbitrage/opportunities/ranking', methods=['GET'])
@login_required
def get_arbitrage_ranking():
    """
    套利机会排行榜。
    从数据库中读取所有基金数据，按净收益率降序排列，并附上年化收益率和持仓天数。

    Query params:
        type      all | premium | discount  （默认 all）
        min_rate  float  最小收益率 %（默认 0，即只要正收益都返回）
        limit     int    最多返回条数（默认 50，最大 200）
    """
    try:
        arb_type = request.args.get('type', 'all')
        min_rate = float(request.args.get('min_rate', 0))
        limit = min(int(request.args.get('limit', 50)), 200)

        username = session.get('username')
        favorites = user_manager.get_user_favorites(username) or []

        all_funds = fund_data_manager.get_all_funds_data()
        results = []
        for f in all_funds:
            rate = f.get('profit_rate', 0) or 0
            if rate < min_rate:
                continue
            f_type = f.get('arbitrage_type', '') or ''
            if arb_type == 'premium' and '溢价' not in f_type:
                continue
            if arb_type == 'discount' and '折价' not in f_type:
                continue
            if rate <= 0:
                continue

            # 补充年化收益率和持仓天数（数据库中没有，根据类型估算）
            is_premium = '溢价' in f_type
            holding_days = 3 if is_premium else 10
            annualized_rate = round((rate / 100) / holding_days * 252 * 100, 1)

            results.append({
                'fund_code': f.get('fund_code'),
                'fund_name': f.get('fund_name', ''),
                'arbitrage_type': f_type,
                'profit_rate': round(rate, 2),
                'annualized_rate': annualized_rate,
                'holding_days': holding_days,
                'price': f.get('price', 0),
                'nav': f.get('nav', 0),
                'price_diff_pct': f.get('price_diff_pct', 0),
                'purchase_limit': f.get('purchase_limit', {}),
                'updated_at': str(f.get('updated_at', '')),
                'is_favorite': f.get('fund_code') in favorites,
            })

        results.sort(key=lambda x: x['profit_rate'], reverse=True)
        results = results[:limit]
        for i, r in enumerate(results):
            r['rank'] = i + 1

        return jsonify({'success': True, 'total': len(results), 'ranking': results})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ==================== Webhook 提醒设置 API ====================

@app.route('/api/user/webhook', methods=['GET'])
@login_required
def get_webhook_config():
    """获取用户的 Webhook 配置"""
    username = session.get('username')
    settings = user_manager.get_user_settings(username) or {}
    webhook_cfg = settings.get('webhook', {})
    # 出于安全考虑，URL 脱敏后返回
    safe_cfg = dict(webhook_cfg)
    if safe_cfg.get('url'):
        url = safe_cfg['url']
        safe_cfg['url_masked'] = url[:30] + '...' if len(url) > 30 else url
    return jsonify({'success': True, 'webhook': safe_cfg})


@app.route('/api/user/webhook', methods=['POST'])
@login_required
def update_webhook_config():
    """
    保存用户的 Webhook 配置

    Body (JSON):
        enabled  bool    是否启用
        type     str     dingtalk | feishu | wecom
        url      str     Webhook URL
        alert_cooldown_minutes  int  同一基金通知冷却时间（分钟，默认 60）
    """
    try:
        username = session.get('username')
        data = request.get_json() or {}

        wtype = data.get('type', 'dingtalk')
        if wtype not in ('dingtalk', 'feishu', 'wecom'):
            return jsonify({'success': False, 'message': 'type 只支持 dingtalk / feishu / wecom'}), 400

        url = (data.get('url') or '').strip()
        enabled = bool(data.get('enabled', True))
        cooldown = int(data.get('alert_cooldown_minutes', 60))

        settings = user_manager.get_user_settings(username) or {}
        settings['webhook'] = {'enabled': enabled, 'type': wtype, 'url': url}
        settings['alert_cooldown_minutes'] = cooldown

        success = user_manager.set_user_settings(username, settings)
        if success:
            return jsonify({'success': True, 'message': 'Webhook 配置已保存'})
        return jsonify({'success': False, 'message': '保存失败'}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/user/webhook/test', methods=['POST'])
@login_required
def test_webhook():
    """发送一条测试消息到用户配置的 Webhook"""
    try:
        username = session.get('username')
        settings = user_manager.get_user_settings(username) or {}
        webhook_cfg = settings.get('webhook', {})

        if not webhook_cfg.get('url'):
            return jsonify({'success': False, 'message': '尚未配置 Webhook URL'}), 400

        title = '[LOF套利工具] 测试消息'
        content = f'您好 {username}，这是一条测试消息，表示您的 Webhook 配置已生效！'
        result = webhook_notifier.send(webhook_cfg, title, content)
        return jsonify({'success': result['success'], 'message': result['message']})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ==================== 基金个性化提醒阈值 API ====================

@app.route('/api/user/alert-thresholds', methods=['GET'])
@login_required
def get_alert_thresholds():
    """获取所有基金的个性化提醒阈值"""
    username = session.get('username')
    settings = user_manager.get_user_settings(username) or {}
    thresholds = settings.get('fund_alert_thresholds', {})
    # 全局默认阈值
    arb_cfg = settings.get('arbitrage_threshold', {})
    default_threshold = arb_cfg.get('min_profit_rate', 0.005) * 100
    return jsonify({
        'success': True,
        'thresholds': thresholds,
        'default_threshold': round(default_threshold, 2),
    })


@app.route('/api/user/alert-thresholds/<fund_code>', methods=['POST'])
@login_required
def set_alert_threshold(fund_code):
    """
    设置某只基金的个性化提醒阈值

    Body (JSON):
        threshold  float  触发提醒的最小收益率（%），例如 0.8 表示 0.8%
    """
    try:
        username = session.get('username')
        data = request.get_json() or {}
        threshold = data.get('threshold')
        if threshold is None:
            return jsonify({'success': False, 'message': '请传入 threshold'}), 400
        threshold = float(threshold)
        if threshold < 0:
            return jsonify({'success': False, 'message': 'threshold 不能为负数'}), 400

        settings = user_manager.get_user_settings(username) or {}
        if 'fund_alert_thresholds' not in settings:
            settings['fund_alert_thresholds'] = {}
        settings['fund_alert_thresholds'][fund_code] = threshold

        success = user_manager.set_user_settings(username, settings)
        if success:
            return jsonify({'success': True, 'message': f'{fund_code} 提醒阈值已设为 {threshold}%'})
        return jsonify({'success': False, 'message': '保存失败'}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/user/alert-thresholds/<fund_code>', methods=['DELETE'])
@login_required
def delete_alert_threshold(fund_code):
    """删除某只基金的个性化阈值（恢复使用全局默认）"""
    try:
        username = session.get('username')
        settings = user_manager.get_user_settings(username) or {}
        thresholds = settings.get('fund_alert_thresholds', {})
        if fund_code in thresholds:
            del thresholds[fund_code]
            settings['fund_alert_thresholds'] = thresholds
            user_manager.set_user_settings(username, settings)
        return jsonify({'success': True, 'message': f'{fund_code} 已恢复默认阈值'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


if __name__ == '__main__':
    # 启动每天自动下载SSE数据的任务（每个交易日9:00）
    print("正在启动SSE数据自动下载任务（每个交易日9:00）...")
    try:
        schedule_daily_download(hour=9, minute=0, only_trading_days=True)
        print("SSE数据自动下载任务已启动（仅在交易日执行）")
    except Exception as e:
        print(f"启动自动下载任务失败: {e}")
        print("提示: 如果Selenium未安装，请运行: pip install selenium")
    
    # 启动后台基金数据更新器
    print("正在启动后台基金数据更新器...")
    try:
        from background_updater import BackgroundFundUpdater
        _interval = DATA_SOURCE.get('update_interval', 60)
        updater = BackgroundFundUpdater(DB_PATH, update_interval=_interval)
        updater.start()
        print(f"后台基金数据更新器已启动（更新间隔: {_interval}秒）")
        
        # 首次启动时，立即更新一次（在后台线程中执行，不阻塞）
        def initial_update():
            time.sleep(5)  # 等待服务启动完成
            print("执行首次基金数据更新...")
            updater.force_update_all()
        
        threading.Thread(target=initial_update, daemon=True).start()
    except Exception as e:
        print(f"启动后台更新器失败: {e}")
        import traceback
        traceback.print_exc()
    
    app.run(debug=True, host='0.0.0.0', port=8505)
