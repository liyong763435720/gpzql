"""
数据库模型和操作
"""
import sqlite3
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import pandas as pd


class Database:
    def __init__(self, db_path: str = None):
        # 支持环境变量指定数据库路径（用于Docker部署）
        if db_path is None:
            import os
            db_path = os.getenv("DB_PATH", "stock_data.db")
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        # journal_mode/synchronous 是数据库级别持久设置，init 时已写入，无需每次重复
        conn.execute("PRAGMA cache_size=-32768")   # 32MB 页缓存（连接级）
        conn.execute("PRAGMA temp_store=MEMORY")   # 临时表放内存（连接级）
        return conn

    def init_database(self):
        """初始化数据库表结构"""
        conn = self.get_connection()
        # WAL模式只在初始化时设置一次，避免每次连接都申请排他锁
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        cursor = conn.cursor()
        
        # 股票基本信息表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                ts_code TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL,
                area TEXT,
                industry TEXT,
                list_date TEXT,
                delist_date TEXT,
                is_hs TEXT,
                exchange TEXT,
                market TEXT,
                currency TEXT
            )
        """)
        
        # 检查并添加新字段（向后兼容）
        cursor.execute("PRAGMA table_info(stocks)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'market' not in columns:
            cursor.execute("ALTER TABLE stocks ADD COLUMN market TEXT")
        if 'currency' not in columns:
            cursor.execute("ALTER TABLE stocks ADD COLUMN currency TEXT")
        
        # 迁移现有A股数据
        cursor.execute("""
            UPDATE stocks
            SET market = 'A', currency = 'CNY'
            WHERE (market IS NULL OR market = '')
            AND exchange IN ('SH', 'SZ', 'SSE', 'SZSE', 'BSE')
        """)
        
        # 行业分类表（申万）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS industry_sw (
                ts_code TEXT,
                industry_name TEXT,
                level TEXT,
                parent_code TEXT,
                market TEXT DEFAULT '',
                PRIMARY KEY (ts_code, industry_name)
            )
        """)

        # 行业分类表（中信）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS industry_citics (
                ts_code TEXT,
                industry_name TEXT,
                level TEXT,
                parent_code TEXT,
                market TEXT DEFAULT '',
                PRIMARY KEY (ts_code, industry_name)
            )
        """)

        # 迁移：给已有行业表添加 market 列并回填数据
        for tbl in ('industry_sw', 'industry_citics'):
            cursor.execute(f"PRAGMA table_info({tbl})")
            cols = [c[1] for c in cursor.fetchall()]
            if 'market' not in cols:
                cursor.execute(f"ALTER TABLE {tbl} ADD COLUMN market TEXT DEFAULT ''")
            # 回填：根据 stocks 表推断 market
            cursor.execute(f"""
                UPDATE {tbl} SET market = (
                    SELECT s.market FROM stocks s WHERE s.ts_code = {tbl}.ts_code
                )
                WHERE (market IS NULL OR market = '') AND ts_code IN (SELECT ts_code FROM stocks)
            """)
        
        # 用户表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                is_active INTEGER NOT NULL DEFAULT 1,
                valid_until TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT
            )
        """)
        
        # 会话表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        
        # 系统配置表（用于存储会话时长等系统配置）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT
            )
        """)
        
        # 用户权限表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                permission_code TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id),
                UNIQUE(user_id, permission_code)
            )
        """)

        # LOF基金套利 - 用户自选基金表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lof_user_favorites (
                username TEXT PRIMARY KEY,
                fund_codes TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT
            )
        """)

        # LOF基金套利 - 用户个性化设置表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lof_user_settings (
                username TEXT PRIMARY KEY,
                settings TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT
            )
        """)

        # 订单表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                plan_code TEXT NOT NULL,
                billing TEXT NOT NULL,
                amount INTEGER NOT NULL,
                pay_method TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                trade_no TEXT,
                paid_at TEXT,
                expires_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)

        # 试用记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                plan_code TEXT NOT NULL,
                applied_at TEXT NOT NULL,
                started_at TEXT,
                expires_at TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                review_note TEXT,
                UNIQUE(user_id, plan_code),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        # 迁移旧表（已存在时补充新字段）
        for col, definition in [
            ('status',      "TEXT NOT NULL DEFAULT 'active'"),
            ('review_note', 'TEXT'),
            ('applied_at',  'TEXT'),
            ('reviewed_at', 'TEXT'),
            ('real_name',   'TEXT'),
            ('phone',       'TEXT'),
            ('id_card',     'TEXT'),
        ]:
            try:
                cursor.execute(f"ALTER TABLE trials ADD COLUMN {col} {definition}")
            except Exception:
                pass

        # 点数账户表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS credit_accounts (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 0,
                gift_balance INTEGER NOT NULL DEFAULT 0,
                total_recharged INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)

        # 点数流水表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS credit_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                credits INTEGER NOT NULL,
                balance_after INTEGER NOT NULL,
                description TEXT,
                order_id TEXT,
                expires_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)

        # 日解锁记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_unlocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                permission_code TEXT NOT NULL,
                unlock_date TEXT NOT NULL,
                credits_cost INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id),
                UNIQUE(user_id, permission_code, unlock_date)
            )
        """)
        # 点数相关索引（首次建库时创建，已存在则跳过）
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_credit_tx_user ON credit_transactions(user_id, created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_unlocks_user_date ON daily_unlocks(user_id, unlock_date)")
        # orders 表关键索引：管理员订单列表、收入统计
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_status_paid_at ON orders(status, paid_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_status_created_at ON orders(status, created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)")
        # users 表：有效订阅用户计数
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_valid_until ON users(valid_until)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id)")

        # 公告表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                content TEXT NOT NULL DEFAULT '',
                style TEXT NOT NULL DEFAULT 'info',
                target TEXT NOT NULL DEFAULT 'all',
                start_at TEXT,
                end_at TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
        """)

        # 服务中断记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS outage_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                interruption_type TEXT NOT NULL DEFAULT 'unplanned',
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_minutes INTEGER,
                compensation_ratio REAL DEFAULT 1.5,
                status TEXT NOT NULL DEFAULT 'ongoing',
                created_by INTEGER,
                created_at TEXT NOT NULL,
                compensated_at TEXT
            )
        """)
        # 兼容旧表：补充 interruption_type 列
        try:
            cursor.execute("ALTER TABLE outage_logs ADD COLUMN interruption_type TEXT NOT NULL DEFAULT 'unplanned'")
        except Exception:
            pass

        # 补偿明细表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS compensation_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                outage_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                compensation_target TEXT NOT NULL DEFAULT 'subscription',
                compensated_minutes INTEGER NOT NULL,
                original_valid_until TEXT,
                new_valid_until TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (outage_id) REFERENCES outage_logs(id),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        # 兼容旧表：补充 compensation_target 列
        try:
            cursor.execute("ALTER TABLE compensation_records ADD COLUMN compensation_target TEXT NOT NULL DEFAULT 'subscription'")
        except Exception:
            pass

        # 兼容旧表：users 表补充注册IP和赠送点数审核字段
        for col, definition in [
            ('reg_ip',      'TEXT DEFAULT ""'),
            ('gift_status', 'TEXT NOT NULL DEFAULT "given"'),   # given/skipped/pending/approved/rejected
            ('gift_amount', 'INTEGER NOT NULL DEFAULT 0'),
        ]:
            try:
                cursor.execute(f"ALTER TABLE users ADD COLUMN {col} {definition}")
            except Exception:
                pass

        # 用户工单表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'other',
                description TEXT NOT NULL,
                page TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'open',
                reply TEXT DEFAULT '',
                replied_by TEXT DEFAULT '',
                replied_at TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)

        # 初始化默认管理员账号（如果不存在）
        cursor.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
        if cursor.fetchone()[0] == 0:
            import bcrypt
            password_hash = bcrypt.hashpw('admin123'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            cursor.execute("""
                INSERT INTO users (username, password_hash, role, is_active, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, ('admin', password_hash, 'admin', 1, datetime.now().strftime('%Y%m%d%H%M%S')))
            import logging
            logging.getLogger(__name__).warning(
                "⚠️  已创建默认管理员账号 admin/admin123，请登录后立即修改密码！"
            )
        
        # 初始化系统配置（会话时长，默认24小时）
        cursor.execute("SELECT COUNT(*) FROM system_config WHERE key = 'session_duration_hours'")
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO system_config (key, value, updated_at)
                VALUES (?, ?, ?)
            """, ('session_duration_hours', '24', datetime.now().strftime('%Y%m%d%H%M%S')))
        
        # 提交所有更改
        conn.commit()
        
        # 月K线数据表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monthly_kline (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                open REAL,
                close REAL,
                high REAL,
                low REAL,
                vol REAL,
                amount REAL,
                pct_chg REAL,
                data_source TEXT DEFAULT 'akshare',
                market TEXT,
                currency TEXT,
                UNIQUE(ts_code, trade_date, data_source)
            )
        """)
        
        # 检查表结构和约束
        cursor.execute("PRAGMA table_info(monthly_kline)")
        columns = [col[1] for col in cursor.fetchall()]
        
        # 检查表定义中的UNIQUE约束
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='monthly_kline'")
        table_sql = cursor.fetchone()
        has_old_constraint = False
        if table_sql and 'UNIQUE(ts_code, trade_date)' in table_sql[0] and 'UNIQUE(ts_code, trade_date, data_source)' not in table_sql[0]:
            has_old_constraint = True
        
        # 迁移：给已有 monthly_kline 表添加 market/currency 列
        if 'market' not in columns:
            cursor.execute("ALTER TABLE monthly_kline ADD COLUMN market TEXT")
        if 'currency' not in columns:
            cursor.execute("ALTER TABLE monthly_kline ADD COLUMN currency TEXT")
        # 重新读取 columns（迁移后可能已变化）
        cursor.execute("PRAGMA table_info(monthly_kline)")
        columns = [col[1] for col in cursor.fetchall()]

        if 'data_source' not in columns or has_old_constraint:
            # 需要重建表
            print("检测到旧的表结构，正在重建表以支持多数据源...")
            
            # 备份数据
            cursor.execute("SELECT * FROM monthly_kline")
            old_data = cursor.fetchall()
            old_columns = [desc[0] for desc in cursor.description]
            
            # 删除旧表
            cursor.execute("DROP TABLE IF EXISTS monthly_kline")
            
            # 创建新表
            cursor.execute("""
                CREATE TABLE monthly_kline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    open REAL,
                    close REAL,
                    high REAL,
                    low REAL,
                    vol REAL,
                    amount REAL,
                    pct_chg REAL,
                    data_source TEXT DEFAULT 'akshare',
                    market TEXT,
                    currency TEXT,
                    UNIQUE(ts_code, trade_date, data_source)
                )
            """)
            
            # 恢复数据（如果有data_source字段则使用，否则默认为akshare）
            if old_data:
                data_source_col_idx = old_columns.index('data_source') if 'data_source' in old_columns else None
                for row in old_data:
                    data_source = row[data_source_col_idx] if data_source_col_idx is not None and row[data_source_col_idx] else 'akshare'
                    cursor.execute("""
                        INSERT INTO monthly_kline 
                        (ts_code, trade_date, year, month, open, close, high, low, vol, amount, pct_chg, data_source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row[old_columns.index('ts_code')],
                        row[old_columns.index('trade_date')],
                        row[old_columns.index('year')],
                        row[old_columns.index('month')],
                        row[old_columns.index('open')],
                        row[old_columns.index('close')],
                        row[old_columns.index('high')],
                        row[old_columns.index('low')],
                        row[old_columns.index('vol')],
                        row[old_columns.index('amount')],
                        row[old_columns.index('pct_chg')],
                        data_source
                    ))
            
            print("✓ 表重建完成")
        else:
            # 如果字段已存在，确保有唯一索引
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_monthly_kline_unique ON monthly_kline(ts_code, trade_date, data_source)")
        
        # 创建索引
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_monthly_kline_code ON monthly_kline(ts_code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_monthly_kline_date ON monthly_kline(trade_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_monthly_kline_year_month ON monthly_kline(year, month)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stocks_delist ON stocks(delist_date)")
        # 补充复合索引：批量月份统计查询（month, year 顺序更优）
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_monthly_kline_month_year ON monthly_kline(month, year)")
        # 单股带数据源过滤查询
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_monthly_kline_code_source ON monthly_kline(ts_code, data_source)")
        # 市场筛选
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stocks_market ON stocks(market)")
        # monthly_kline market 字段索引（get_market_statistics 用）
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_monthly_kline_market ON monthly_kline(market)")
        # 覆盖索引：数据源/市场统计 GROUP BY + MIN/MAX(trade_date) 无需全表扫描
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_monthly_kline_source_date ON monthly_kline(data_source, trade_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_monthly_kline_source_ts ON monthly_kline(data_source, ts_code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_monthly_kline_market_date ON monthly_kline(market, trade_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_monthly_kline_market_ts ON monthly_kline(market, ts_code)")
        # 行业表索引：get_industry_stocks / get_all_industries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_industry_sw_name_market ON industry_sw(industry_name, market)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_industry_sw_market ON industry_sw(market)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_industry_citics_name_market ON industry_citics(industry_name, market)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_industry_citics_market ON industry_citics(market)")

        conn.commit()
        conn.close()



    def save_stocks(self, stocks_df: pd.DataFrame):
        """保存股票基本信息（按市场增量保存，不覆盖其他市场数据）"""
        if stocks_df.empty:
            return
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # 检测本次数据属于哪个市场
            market = None
            if 'market' in stocks_df.columns:
                markets = stocks_df['market'].dropna().unique().tolist()
                if len(markets) == 1:
                    market = markets[0]

            if market:
                # 只删除该市场的旧数据，保留其他市场
                cursor.execute("DELETE FROM stocks WHERE market = ?", (market,))
            else:
                # 无法确定市场时才全量替换（兼容旧逻辑）
                cursor.execute("DELETE FROM stocks")

            stocks_df.to_sql('stocks', conn, if_exists='append', index=False)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def save_monthly_kline(self, kline_df: pd.DataFrame, data_source: str = 'akshare', market: str = None, currency: str = None):
        """保存月K线数据（使用INSERT OR REPLACE避免重复，支持多数据源）
        
        Args:
            kline_df: 月K线数据DataFrame
            data_source: 数据源名称
            market: 市场类型（A/HK/US），如果为None则从ts_code自动检测
            currency: 货币类型（CNY/HKD/USD），如果为None则根据market自动设置
        """
        if kline_df is None or kline_df.empty:
            return
        # 过滤掉 ts_code/trade_date/year/month 为空的行（NOT NULL 字段）
        kline_df = kline_df.dropna(subset=['ts_code', 'trade_date', 'year', 'month'])
        kline_df = kline_df[kline_df['ts_code'] != '']
        if kline_df.empty:
            return

        conn = self.get_connection()
        cursor = conn.cursor()

        # 如果没有提供market，从ts_code自动检测
        if market is None and not kline_df.empty:
            ts_code = kline_df.iloc[0].get('ts_code', '')
            if ts_code:
                # 简单的市场检测逻辑
                if ts_code.endswith('.HK'):
                    market = 'HK'
                    currency = currency or 'HKD'
                elif ts_code.endswith('.US') or (len(ts_code) <= 5 and ts_code.isalpha()):
                    market = 'US'
                    currency = currency or 'USD'
                else:
                    market = 'A'
                    currency = currency or 'CNY'
        
        # 如果没有提供currency，根据market设置默认值
        if currency is None:
            if market == 'HK':
                currency = 'HKD'
            elif market == 'US':
                currency = 'USD'
            else:
                currency = 'CNY'
        
        def _nan_to_none(v):
            """把 float NaN 转为 None，避免写入数据库时出错"""
            if v is None:
                return None
            try:
                import math
                if isinstance(v, float) and math.isnan(v):
                    return None
            except Exception:
                pass
            return v

        rows = [
            (
                row.get('ts_code'),
                row.get('trade_date'),
                int(row.get('year')),
                int(row.get('month')),
                _nan_to_none(row.get('open')),
                _nan_to_none(row.get('close')),
                _nan_to_none(row.get('high')),
                _nan_to_none(row.get('low')),
                _nan_to_none(row.get('vol')),
                _nan_to_none(row.get('amount')),
                _nan_to_none(row.get('pct_chg')),
                data_source,
                market,
                currency,
            )
            for _, row in kline_df.iterrows()
        ]
        cursor.executemany("""
            INSERT OR REPLACE INTO monthly_kline
            (ts_code, trade_date, year, month, open, close, high, low, vol, amount, pct_chg, data_source, market, currency)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

        conn.commit()
        conn.close()
    
    def delete_monthly_kline_by_source(self, data_source: str):
        """删除指定数据源的所有月K线数据"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM monthly_kline WHERE data_source = ?", (data_source,))
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        return deleted_count

    def delete_monthly_kline_by_market(self, market: str):
        """删除指定市场的所有月K线数据"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM monthly_kline
            WHERE ts_code IN (
                SELECT ts_code FROM stocks WHERE market = ?
            )
        """, (market,))
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        return deleted_count
    
    # ========== 用户和权限管理方法 ==========
    
    def get_user_by_username(self, username: str) -> Optional[Dict]:
        """根据用户名获取用户信息"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, username, password_hash, role, is_active, valid_until, created_at
            FROM users
            WHERE username = ?
        """, (username,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'id': row[0],
                'username': row[1],
                'password_hash': row[2],
                'role': row[3],
                'is_active': bool(row[4]),
                'valid_until': row[5],
                'created_at': row[6]
            }
        return None
    
    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """根据ID获取用户信息"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, username, password_hash, role, is_active, valid_until, created_at
            FROM users
            WHERE id = ?
        """, (user_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'id': row[0],
                'username': row[1],
                'password_hash': row[2],
                'role': row[3],
                'is_active': bool(row[4]),
                'valid_until': row[5],
                'created_at': row[6]
            }
        return None
    
    def create_user(self, username: str, password: str, role: str = 'user', valid_until: str = None) -> int:
        """创建用户"""
        import bcrypt
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO users (username, password_hash, role, is_active, valid_until, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (username, password_hash, role, 1, valid_until, datetime.now().strftime('%Y%m%d%H%M%S')))
            user_id = cursor.lastrowid
            conn.commit()
            return user_id
        except sqlite3.IntegrityError:
            raise ValueError(f"用户名 {username} 已存在")
        finally:
            conn.close()
    
    # 哨兵：区分"调用者未传该字段"（跳过更新）与"调用者显式传 None"（清空字段）
    _UNSET = object()

    def update_user(self, user_id: int, username: str = None, password: str = None,
                   role: str = None, is_active=_UNSET, valid_until=_UNSET):
        """更新用户信息
        is_active / valid_until 传 None 表示清空，不传（默认 _UNSET）表示不修改。
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        updates = []
        params = []

        if username is not None:
            updates.append("username = ?")
            params.append(username)
        if password is not None:
            import bcrypt
            password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            updates.append("password_hash = ?")
            params.append(password_hash)
        if role is not None:
            updates.append("role = ?")
            params.append(role)
        if is_active is not self._UNSET:
            updates.append("is_active = ?")
            params.append(1 if is_active else 0)
            # 禁用用户时立即清除其所有活跃会话
            if not is_active:
                cursor.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        if valid_until is not self._UNSET:
            updates.append("valid_until = ?")
            params.append(valid_until)  # None → 存入 NULL（清空有效期）
        
        if updates:
            updates.append("updated_at = ?")
            params.append(datetime.now().strftime('%Y%m%d%H%M%S'))
            params.append(user_id)
            
            cursor.execute(f"""
                UPDATE users
                SET {', '.join(updates)}
                WHERE id = ?
            """, params)
            conn.commit()
        
        conn.close()
    
    def delete_user(self, user_id: int):
        """删除用户"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
        cursor.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
    
    def get_all_users(self) -> List[Dict]:
        """获取所有用户列表"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT u.id, u.username, u.role, u.is_active, u.valid_until, u.created_at,
                   COALESCE(c.balance, 0) as balance,
                   COALESCE(c.gift_balance, 0) as gift_balance
            FROM users u
            LEFT JOIN credit_accounts c ON c.user_id = u.id
            ORDER BY u.created_at DESC
        """)
        users = []
        for row in cursor.fetchall():
            users.append({
                'id': row[0],
                'username': row[1],
                'role': row[2],
                'is_active': bool(row[3]),
                'valid_until': row[4],
                'created_at': row[5],
                'credits_balance': row[6],
                'credits_gift': row[7],
                'credits_total': row[6] + row[7]
            })
        conn.close()
        return users
    
    def create_session(self, user_id: int, session_id: str, expires_at: str) -> bool:
        """创建会话"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO sessions (session_id, user_id, expires_at, created_at)
                VALUES (?, ?, ?, ?)
            """, (session_id, user_id, expires_at, datetime.now().strftime('%Y%m%d%H%M%S')))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # 如果session_id已存在，更新它
            cursor.execute("""
                UPDATE sessions
                SET user_id = ?, expires_at = ?, created_at = ?
                WHERE session_id = ?
            """, (user_id, expires_at, datetime.now().strftime('%Y%m%d%H%M%S'), session_id))
            conn.commit()
            return True
        finally:
            conn.close()
    
    def get_session(self, session_id: str) -> Optional[Dict]:
        """获取会话信息"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT s.session_id, s.user_id, s.expires_at, u.username, u.role, u.is_active
            FROM sessions s
            JOIN users u ON s.user_id = u.id
            WHERE s.session_id = ?
        """, (session_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            expires_at = datetime.strptime(row[2], '%Y%m%d%H%M%S')
            if expires_at < datetime.now():
                # 会话已过期，删除它
                self.delete_session(session_id)
                return None
            
            return {
                'session_id': row[0],
                'user_id': row[1],
                'expires_at': row[2],
                'username': row[3],
                'role': row[4],
                'is_active': bool(row[5])
            }
        return None
    
    def delete_session(self, session_id: str):
        """删除会话"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
        conn.commit()
        conn.close()
    
    def cleanup_expired_sessions(self):
        """清理过期会话"""
        conn = self.get_connection()
        cursor = conn.cursor()
        current_time = datetime.now().strftime('%Y%m%d%H%M%S')
        cursor.execute("DELETE FROM sessions WHERE expires_at < ?", (current_time,))
        conn.commit()
        conn.close()
    
    def get_system_config(self, key: str, default: str = None) -> Optional[str]:
        """获取系统配置"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM system_config WHERE key = ?", (key,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else default
    
    def set_system_config(self, key: str, value: str):
        """设置系统配置"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO system_config (key, value, updated_at)
            VALUES (?, ?, ?)
        """, (key, value, datetime.now().strftime('%Y%m%d%H%M%S')))
        conn.commit()
        conn.close()
    
    # ========== 权限管理方法 ==========
    
    def get_user_permissions(self, user_id: int) -> List[str]:
        """获取用户权限列表"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT permission_code
            FROM user_permissions
            WHERE user_id = ?
        """, (user_id,))
        permissions = [row[0] for row in cursor.fetchall()]
        conn.close()
        return permissions
    
    def get_user_auth_data(self, session_id: str) -> Optional[Dict]:
        """一次连接完成所有认证查询（session + user + permissions + trial），替代原来的4次独立连接"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # 1. session + user 合并查询
            cursor.execute("""
                SELECT s.user_id, s.expires_at, u.username, u.role, u.is_active, u.valid_until
                FROM sessions s
                JOIN users u ON s.user_id = u.id
                WHERE s.session_id = ?
            """, (session_id,))
            row = cursor.fetchone()
            if not row:
                return None

            expires_at = datetime.strptime(row[1], '%Y%m%d%H%M%S')
            if expires_at < datetime.now():
                cursor.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
                conn.commit()
                return None

            if not row[4]:  # is_active
                cursor.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
                conn.commit()
                return None

            user_id = row[0]

            # 2. 权限查询
            cursor.execute(
                "SELECT permission_code FROM user_permissions WHERE user_id = ?", (user_id,)
            )
            permissions = [r[0] for r in cursor.fetchall()]

            # 3. 有效试用查询
            now_str = datetime.now().strftime('%Y%m%d%H%M%S')
            cursor.execute("""
                SELECT plan_code, expires_at, started_at, status, id
                FROM trials
                WHERE user_id = ? AND status = 'active' AND expires_at != '' AND expires_at > ?
                LIMIT 1
            """, (user_id, now_str))
            trial_row = cursor.fetchone()
            trial = dict(trial_row) if trial_row else None

            return {
                'user_id':    user_id,
                'username':   row[2],
                'role':       row[3],
                'valid_until': row[5],
                'permissions': permissions,
                'trial':      trial,
            }
        finally:
            conn.close()

    def set_user_permissions(self, user_id: int, permission_codes: List[str]):
        """设置用户权限（覆盖原有权限）"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # 先删除所有现有权限
        cursor.execute("DELETE FROM user_permissions WHERE user_id = ?", (user_id,))
        
        # 添加新权限
        current_time = datetime.now().strftime('%Y%m%d%H%M%S')
        for code in permission_codes:
            cursor.execute("""
                INSERT INTO user_permissions (user_id, permission_code, created_at)
                VALUES (?, ?, ?)
            """, (user_id, code, current_time))
        
        conn.commit()
        conn.close()
    
    def add_user_permission(self, user_id: int, permission_code: str):
        """添加单个权限"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO user_permissions (user_id, permission_code, created_at)
                VALUES (?, ?, ?)
            """, (user_id, permission_code, datetime.now().strftime('%Y%m%d%H%M%S')))
            conn.commit()
        except sqlite3.IntegrityError:
            # 权限已存在，忽略
            pass
        finally:
            conn.close()
    
    def remove_user_permission(self, user_id: int, permission_code: str):
        """移除单个权限"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM user_permissions
            WHERE user_id = ? AND permission_code = ?
        """, (user_id, permission_code))
        conn.commit()
        conn.close()
    
    def has_permission(self, user_id: int, permission_code: str) -> bool:
        """检查用户是否有指定权限"""
        # 管理员始终拥有所有权限
        user = self.get_user_by_id(user_id)
        if user and user['role'] == 'admin':
            return True
        
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM user_permissions
            WHERE user_id = ? AND permission_code = ?
        """, (user_id, permission_code))
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    
    # ========== 订单管理方法 ==========

    def create_order(self, order_id: str, user_id: int, plan_code: str,
                     billing: str, amount: int, pay_method: str, expires_at: str) -> None:
        """创建订单"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO orders (id, user_id, plan_code, billing, amount, pay_method,
                                status, expires_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
        """, (order_id, user_id, plan_code, billing, amount, pay_method,
              expires_at, datetime.now().strftime('%Y%m%d%H%M%S')))
        conn.commit()
        conn.close()

    def get_order(self, order_id: str) -> Optional[Dict]:
        """获取订单信息"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, user_id, plan_code, billing, amount, pay_method,
                   status, trade_no, paid_at, expires_at, created_at
            FROM orders WHERE id = ?
        """, (order_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return {
                'id': row[0], 'user_id': row[1], 'plan_code': row[2],
                'billing': row[3], 'amount': row[4], 'pay_method': row[5],
                'status': row[6], 'trade_no': row[7], 'paid_at': row[8],
                'expires_at': row[9], 'created_at': row[10],
            }
        return None

    def update_order_paid(self, order_id: str, trade_no: str) -> None:
        """标记订单已支付"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE orders SET status='paid', trade_no=?, paid_at=?
            WHERE id = ? AND status='pending'
        """, (trade_no, datetime.now().strftime('%Y%m%d%H%M%S'), order_id))
        conn.commit()
        conn.close()

    def get_user_orders(self, user_id: int) -> List[Dict]:
        """获取用户订单列表"""
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, user_id, plan_code, billing, amount, pay_method, status, trade_no, paid_at, expires_at, created_at
            FROM orders WHERE user_id = ? ORDER BY created_at DESC LIMIT 50
        """, (user_id,))
        rows = cursor.fetchall()
        conn.close()
        plan_names = {'free': '免费版', 'basic': '基础版', 'pro': '专业版'}
        billing_names = {'monthly': '月付', 'quarterly': '季付', 'yearly': '年付'}
        status_names = {'pending': '待支付', 'paid': '已支付', 'expired': '已过期', 'failed': '支付失败'}
        result = []
        for row in rows:
            r = dict(row)
            r['plan_name'] = plan_names.get(r['plan_code'], r['plan_code'])
            r['billing_name'] = billing_names.get(r['billing'], r['billing'])
            r['status_name'] = status_names.get(r['status'], r['status'])
            r['amount_yuan'] = r['amount'] / 100
            result.append(r)
        return result

    def get_all_orders(self, status: str = None, page: int = 1, page_size: int = 20) -> List[Dict]:
        """获取所有订单（管理员）"""
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        offset = (page - 1) * page_size
        if status:
            cursor.execute("""
                SELECT o.*, u.username FROM orders o
                LEFT JOIN users u ON o.user_id = u.id
                WHERE o.status = ? ORDER BY o.created_at DESC LIMIT ? OFFSET ?
            """, (status, page_size, offset))
        else:
            cursor.execute("""
                SELECT o.*, u.username FROM orders o
                LEFT JOIN users u ON o.user_id = u.id
                ORDER BY o.created_at DESC LIMIT ? OFFSET ?
            """, (page_size, offset))
        rows = cursor.fetchall()
        conn.close()
        plan_names = {'free': '免费版', 'basic': '基础版', 'pro': '专业版'}
        billing_names = {'monthly': '月付', 'quarterly': '季付', 'yearly': '年付'}
        status_names = {'pending': '待支付', 'paid': '已支付', 'expired': '已过期', 'failed': '支付失败'}
        result = []
        for row in rows:
            r = dict(row)
            r['plan_name'] = plan_names.get(r['plan_code'], r['plan_code'])
            r['billing_name'] = billing_names.get(r['billing'], r['billing'])
            r['status_name'] = status_names.get(r['status'], r['status'])
            r['amount_yuan'] = r['amount'] / 100
            result.append(r)
        return result

    def count_orders(self, status: str = None) -> int:
        """统计订单数量"""
        conn = self.get_connection()
        cursor = conn.cursor()
        if status:
            cursor.execute("SELECT COUNT(*) FROM orders WHERE status = ?", (status,))
        else:
            cursor.execute("SELECT COUNT(*) FROM orders")
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def get_revenue_stats(self) -> Dict:
        """收入统计数据"""
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        from datetime import datetime, timedelta
        now = datetime.now()
        month_start = now.strftime('%Y%m') + '01000000'

        # 本月收入（订阅 + 充值）
        cursor.execute("SELECT COALESCE(SUM(amount),0) FROM orders WHERE status='paid' AND paid_at >= ?", (month_start,))
        month_revenue = cursor.fetchone()[0] / 100

        # 本月收入拆分：订阅 / 点数充值
        cursor.execute("SELECT COALESCE(SUM(amount),0) FROM orders WHERE status='paid' AND paid_at >= ? AND plan_code != 'credits'", (month_start,))
        month_sub_revenue = cursor.fetchone()[0] / 100
        cursor.execute("SELECT COALESCE(SUM(amount),0) FROM orders WHERE status='paid' AND paid_at >= ? AND plan_code = 'credits'", (month_start,))
        month_credit_revenue = cursor.fetchone()[0] / 100

        # 本月新增订单
        cursor.execute("SELECT COUNT(*) FROM orders WHERE status='paid' AND paid_at >= ?", (month_start,))
        month_orders = cursor.fetchone()[0]

        # 总收入（订阅 + 充值）
        cursor.execute("SELECT COALESCE(SUM(amount),0) FROM orders WHERE status='paid'")
        total_revenue = cursor.fetchone()[0] / 100

        # 总收入拆分
        cursor.execute("SELECT COALESCE(SUM(amount),0) FROM orders WHERE status='paid' AND plan_code != 'credits'")
        total_sub_revenue = cursor.fetchone()[0] / 100
        cursor.execute("SELECT COALESCE(SUM(amount),0) FROM orders WHERE status='paid' AND plan_code = 'credits'")
        total_credit_revenue = cursor.fetchone()[0] / 100

        # 总订单数
        cursor.execute("SELECT COUNT(*) FROM orders WHERE status='paid'")
        total_orders = cursor.fetchone()[0]

        # 当前有效订阅用户数（valid_until > now）
        now_str = now.strftime('%Y%m%d%H%M%S')
        cursor.execute("SELECT COUNT(*) FROM users WHERE valid_until > ? AND role != 'admin'", (now_str,))
        active_subscribers = cursor.fetchone()[0]

        # 近30天每日收入 —— 一次 GROUP BY 替代 60 次循环查询
        thirty_days_ago = (now - timedelta(days=29)).strftime('%Y%m%d') + '000000'
        cursor.execute("""
            SELECT substr(paid_at, 1, 8) AS day,
                   COALESCE(SUM(amount), 0) AS total_amount,
                   COUNT(*) AS order_count
            FROM orders
            WHERE status='paid' AND paid_at >= ?
            GROUP BY substr(paid_at, 1, 8)
        """, (thirty_days_ago,))
        day_map = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
        daily = []
        for i in range(29, -1, -1):
            day = now - timedelta(days=i)
            day_str = day.strftime('%Y%m%d')
            amt_raw, cnt = day_map.get(day_str, (0, 0))
            daily.append({'date': day.strftime('%m-%d'), 'revenue': amt_raw / 100, 'orders': cnt})

        # 套餐分布（已支付订单按plan_code统计）
        cursor.execute("SELECT plan_code, COUNT(*) as cnt FROM orders WHERE status='paid' GROUP BY plan_code")
        plan_dist = {row['plan_code']: row['cnt'] for row in cursor.fetchall()}

        # 月付/年付比例
        cursor.execute("SELECT billing, COUNT(*) as cnt FROM orders WHERE status='paid' GROUP BY billing")
        billing_dist = {row['billing']: row['cnt'] for row in cursor.fetchall()}

        conn.close()
        return {
            'month_revenue': month_revenue,
            'month_sub_revenue': month_sub_revenue,
            'month_credit_revenue': month_credit_revenue,
            'month_orders': month_orders,
            'total_revenue': total_revenue,
            'total_sub_revenue': total_sub_revenue,
            'total_credit_revenue': total_credit_revenue,
            'total_orders': total_orders,
            'active_subscribers': active_subscribers,
            'daily': daily,
            'plan_dist': plan_dist,
            'billing_dist': billing_dist,
        }

    # ========== 试用管理方法 ==========

    def get_trial(self, user_id: int, plan_code: str) -> Optional[Dict]:
        """获取指定用户对指定套餐的试用记录（无论是否已过期）"""
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM trials WHERE user_id=? AND plan_code=?",
            (user_id, plan_code)
        )
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_active_trial(self, user_id: int) -> Optional[Dict]:
        """获取用户当前有效的试用记录（status=active 且未过期，取层级最高的）"""
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        now = datetime.now().strftime('%Y%m%d%H%M%S')
        cursor.execute(
            "SELECT * FROM trials WHERE user_id=? AND status='active' AND expires_at != '' AND expires_at > ?",
            (user_id, now)
        )
        rows = cursor.fetchall()
        conn.close()
        if not rows:
            return None
        plan_order = ['free', 'basic', 'pro']
        best = max(rows, key=lambda r: plan_order.index(r['plan_code']) if r['plan_code'] in plan_order else 0)
        return dict(best)

    def has_active_or_pending_trial(self, user_id: int) -> Optional[Dict]:
        """检查用户是否有进行中（active未过期）或待审核（pending）的试用"""
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        now = datetime.now().strftime('%Y%m%d%H%M%S')
        cursor.execute("""
            SELECT * FROM trials
            WHERE user_id=?
              AND (status='pending' OR (status='active' AND expires_at != '' AND expires_at > ?))
            LIMIT 1
        """, (user_id, now))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def create_trial(self, user_id: int, plan_code: str,
                     real_name: str, phone: str, id_card: str) -> Dict:
        """创建待审核试用申请。
        - 首次申请：直接创建
        - 已有 rejected 记录且距拒绝时间超过 7 天：删旧记录后重新申请
        - 已有 rejected 记录但未满 7 天：抛 ValueError(含剩余天数)
        - 其他状态已存在：抛 ValueError
        """
        existing = self.get_trial(user_id, plan_code)
        if existing:
            db_status = existing.get('status', 'active')
            if db_status == 'rejected':
                reviewed_at_str = existing.get('reviewed_at') or existing.get('applied_at', '')
                if reviewed_at_str:
                    try:
                        reviewed_dt = datetime.strptime(reviewed_at_str, '%Y%m%d%H%M%S')
                        days_passed = (datetime.now() - reviewed_dt).days
                        cooldown = 7
                        if days_passed < cooldown:
                            remaining = cooldown - days_passed
                            raise ValueError(f"申请被拒绝后需冷却 {cooldown} 天，还需等待 {remaining} 天后可重新申请")
                    except ValueError:
                        raise
                    except Exception:
                        pass
                # 冷却结束或无法解析时间，删旧记录后重新创建
                conn = self.get_connection()
                conn.execute("DELETE FROM trials WHERE user_id=? AND plan_code=?", (user_id, plan_code))
                conn.commit()
                conn.close()
            else:
                raise ValueError("已申请过该套餐试用")

        conn = self.get_connection()
        cursor = conn.cursor()
        applied_at = datetime.now().strftime('%Y%m%d%H%M%S')
        cursor.execute(
            """INSERT INTO trials
               (user_id, plan_code, applied_at, started_at, expires_at, status, real_name, phone, id_card)
               VALUES (?,?,?,'','','pending',?,?,?)""",
            (user_id, plan_code, applied_at, real_name, phone, id_card)
        )
        conn.commit()
        conn.close()
        return {'plan_code': plan_code, 'applied_at': applied_at, 'status': 'pending'}

    def approve_trial(self, trial_id: int, days: int) -> bool:
        """审批通过试用，计算有效期从审批时刻起"""
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.now()
        started_at = now.strftime('%Y%m%d%H%M%S')
        expires_at = (now + timedelta(days=days)).strftime('%Y%m%d%H%M%S')
        cursor.execute(
            "UPDATE trials SET status='active', started_at=?, expires_at=?, review_note='' WHERE id=? AND status='pending'",
            (started_at, expires_at, trial_id)
        )
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        return affected > 0

    def reject_trial(self, trial_id: int, note: str = '') -> bool:
        """拒绝试用申请，记录拒绝时间（用于冷却计算）"""
        conn = self.get_connection()
        cursor = conn.cursor()
        reviewed_at = datetime.now().strftime('%Y%m%d%H%M%S')
        cursor.execute(
            "UPDATE trials SET status='rejected', review_note=?, reviewed_at=? WHERE id=? AND status='pending'",
            (note, reviewed_at, trial_id)
        )
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        return affected > 0

    def get_all_trials(self) -> list:
        """获取所有试用记录，关联用户名"""
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT t.id, t.user_id, u.username, t.plan_code,
                   t.applied_at, t.started_at, t.expires_at,
                   t.status, t.review_note, t.reviewed_at,
                   t.real_name, t.phone, t.id_card
            FROM trials t
            LEFT JOIN users u ON t.user_id = u.id
            ORDER BY t.applied_at DESC
        """)
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ========== 公告管理方法 ==========

    def get_active_announcements(self, now_str: str, plan_code: str = 'free') -> list:
        """获取当前有效公告（已启用、在时间范围内、匹配目标用户）"""
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM announcements
            WHERE enabled=1
              AND (start_at IS NULL OR start_at='' OR start_at <= ?)
              AND (end_at IS NULL OR end_at='' OR end_at >= ?)
            ORDER BY sort_order DESC, id DESC
        """, (now_str, now_str))
        rows = cursor.fetchall()
        conn.close()
        plan_order = ['free', 'basic', 'pro']
        is_logged_in = plan_code != '__guest__'
        cur_idx = plan_order.index(plan_code) if plan_code in plan_order else 0
        result = []
        for r in rows:
            target = r['target']
            if target == 'all':
                result.append(dict(r))
            elif target == 'logged_in' and is_logged_in:
                result.append(dict(r))
            elif target == 'basic' and cur_idx >= 1:
                result.append(dict(r))
            elif target == 'pro' and cur_idx >= 2:
                result.append(dict(r))
        return result

    def get_all_announcements(self) -> list:
        """获取所有公告（管理员用）"""
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM announcements ORDER BY sort_order DESC, id DESC")
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def create_announcement(self, title: str, content: str, style: str,
                             target: str, start_at: str, end_at: str,
                             enabled: int, sort_order: int) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        created_at = datetime.now().strftime('%Y%m%d%H%M%S')
        cursor.execute("""
            INSERT INTO announcements (title, content, style, target, start_at, end_at, enabled, sort_order, created_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (title, content, style, target, start_at or '', end_at or '', enabled, sort_order, created_at))
        new_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return new_id

    def update_announcement(self, ann_id: int, title: str, content: str, style: str,
                             target: str, start_at: str, end_at: str,
                             enabled: int, sort_order: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE announcements
            SET title=?, content=?, style=?, target=?, start_at=?, end_at=?, enabled=?, sort_order=?
            WHERE id=?
        """, (title, content, style, target, start_at or '', end_at or '', enabled, sort_order, ann_id))
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        return affected > 0

    def delete_announcement(self, ann_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM announcements WHERE id=?", (ann_id,))
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        return affected > 0

    def expire_stale_orders(self) -> None:
        """将超时未支付的订单标记为 expired"""
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.now().strftime('%Y%m%d%H%M%S')
        cursor.execute("""
            UPDATE orders SET status='expired'
            WHERE status='pending' AND expires_at < ?
        """, (now,))
        conn.commit()
        conn.close()

    def get_stocks(self, exclude_delisted: bool = True, market: str = None) -> pd.DataFrame:
        """
        获取股票列表
        market: 'A', 'HK', 'US' 或 None（全部）
        """
        conn = self.get_connection()
        query = "SELECT * FROM stocks WHERE 1=1"
        params = []
        
        if exclude_delisted:
            query += " AND (delist_date IS NULL OR delist_date = '')"
        
        if market:
            query += " AND market = ?"
            params.append(market)
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    
    def get_stock_by_code(self, code: str) -> Optional[Dict]:
        """根据代码获取股票信息（支持港股和美股代码标准化）"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # 先获取表结构，确定有哪些列
        cursor.execute("PRAGMA table_info(stocks)")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        # 标准化代码（尝试多种格式）
        code_variants = [code]
        
        # 如果是港股代码（5位数字），尝试添加.HK后缀
        if code.isdigit() and len(code) == 5:
            code_variants.append(f"{code}.HK")
            code_variants.append(f"{int(code):05d}.HK")  # 确保5位数字格式
        
        # 如果是美股代码（字母），尝试添加.US后缀（如果需要）
        if code.isalpha() and len(code) <= 5:
            code_variants.append(f"{code}.US")
        
        # 如果代码已经包含后缀，也尝试不带后缀的版本
        if code.endswith('.HK'):
            code_variants.append(code.replace('.HK', ''))
        elif code.endswith('.US'):
            code_variants.append(code.replace('.US', ''))
        
        # 构建查询语句，尝试所有可能的代码格式
        if column_names:
            select_cols = ', '.join(column_names)
            placeholders = ', '.join(['?'] * len(code_variants))
            query = f"""
                SELECT {select_cols}
                FROM stocks 
                WHERE symbol IN ({placeholders}) OR ts_code IN ({placeholders})
                LIMIT 1
            """
            params = code_variants * 2  # symbol和ts_code都要匹配
        else:
            # 如果表不存在或为空，使用默认列
            placeholders = ', '.join(['?'] * len(code_variants))
            query = f"""
                SELECT ts_code, symbol, name, list_date, delist_date, exchange 
                FROM stocks 
                WHERE symbol IN ({placeholders}) OR ts_code IN ({placeholders})
                LIMIT 1
            """
            params = code_variants * 2
            column_names = ['ts_code', 'symbol', 'name', 'list_date', 'delist_date', 'exchange']
        
        cursor.execute(query, params)
        row = cursor.fetchone()
        conn.close()
        
        if row:
            result = {}
            for i, col_name in enumerate(column_names):
                if i < len(row):
                    result[col_name] = row[i]
                else:
                    result[col_name] = None
            return result
        return None
    
    def search_stocks(self, keyword: str, limit: int = 20, market: str = None) -> List[Dict]:
        """根据关键词搜索股票（支持代码和名称，可按市场过滤）"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # 模糊搜索：匹配代码或名称
        keyword_pattern = f"%{keyword}%"
        market_clause = "AND market = ?" if market else ""
        params_base = [keyword_pattern, keyword_pattern, keyword_pattern]
        if market:
            params_base.append(market)
        cursor.execute(f"""
            SELECT ts_code, symbol, name, exchange, market
            FROM stocks
            WHERE (symbol LIKE ? OR ts_code LIKE ? OR name LIKE ?)
            AND (delist_date IS NULL OR delist_date = '')
            {market_clause}
            ORDER BY
                CASE
                    WHEN symbol = ? THEN 1
                    WHEN ts_code = ? THEN 2
                    WHEN symbol LIKE ? THEN 3
                    WHEN ts_code LIKE ? THEN 4
                    WHEN name LIKE ? THEN 5
                    ELSE 6
                END,
                symbol
            LIMIT ?
        """, (*params_base,
              keyword, keyword, f"{keyword}%", f"{keyword}%", f"{keyword}%", limit))

        results = []
        for row in cursor.fetchall():
            results.append({
                'ts_code': row[0],
                'symbol': row[1],
                'name': row[2],
                'exchange': row[3],
                'market': row[4]
            })
        
        conn.close()
        return results
    
    def get_monthly_kline(self, ts_code: str = None, year: int = None, 
                          month: int = None, start_year: int = None, 
                          end_year: int = None, data_source: str = None) -> pd.DataFrame:
        """获取月K线数据（支持按数据源过滤）"""
        conn = self.get_connection()
        query = "SELECT * FROM monthly_kline WHERE 1=1"
        params = []
        
        if ts_code:
            query += " AND ts_code = ?"
            params.append(ts_code)
        if year:
            query += " AND year = ?"
            params.append(year)
        if month:
            query += " AND month = ?"
            params.append(month)
        if start_year:
            query += " AND year >= ?"
            params.append(start_year)
        if end_year:
            query += " AND year <= ?"
            params.append(end_year)
        if data_source:
            query += " AND data_source = ?"
            params.append(data_source)
        
        query += " ORDER BY trade_date"
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    
    def get_available_data_sources(self, ts_code: str = None) -> List[str]:
        """获取可用的数据源列表"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if ts_code:
            cursor.execute("SELECT DISTINCT data_source FROM monthly_kline WHERE ts_code = ?", (ts_code,))
        else:
            cursor.execute("SELECT DISTINCT data_source FROM monthly_kline")
        
        sources = [row[0] for row in cursor.fetchall() if row[0]]
        conn.close()
        return sources
    
    def get_data_source_statistics(self) -> List[Dict]:
        """获取每个数据源的统计信息（数据量和最新日期）"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # 去掉 COALESCE 让 idx_monthly_kline_code_source 索引生效
        # stock_count 用子查询替代 COUNT(DISTINCT) 避免全表聚合
        cursor.execute("""
            SELECT
                data_source,
                COUNT(*)        AS data_count,
                MIN(trade_date) AS earliest_date,
                MAX(trade_date) AS latest_date
            FROM monthly_kline
            WHERE data_source IS NOT NULL
            GROUP BY data_source
            ORDER BY data_source
        """)
        rows = cursor.fetchall()

        # 每个数据源的股票数用单独索引查询（比 COUNT(DISTINCT) 快得多）
        results = []
        for row in rows:
            cursor.execute(
                "SELECT COUNT(DISTINCT ts_code) FROM monthly_kline WHERE data_source = ?",
                (row[0],)
            )
            stock_count = cursor.fetchone()[0]

        results = []
        for row in cursor.fetchall():
            data_count = row[1]
            results.append({
                'data_source': row[0],
                'data_count':  data_count,
                'earliest_date': row[2],
                'latest_date':   row[3],
                'stock_count':   stock_count,
                'avg_months': round(data_count / stock_count, 1) if stock_count else 0
            })

        conn.close()
        return results

    def get_market_statistics(self) -> List[Dict]:
        """获取各市场的K线统计信息（数据量、股票数、最新日期）"""
        conn = self.get_connection()
        cursor = conn.cursor()
        # 去掉 COALESCE 让 idx_monthly_kline_market 索引生效，NULL 单独处理
        cursor.execute("""
            SELECT
                market,
                COUNT(*)        AS data_count,
                MIN(trade_date) AS earliest_date,
                MAX(trade_date) AS latest_date
            FROM monthly_kline
            WHERE market IS NOT NULL
            GROUP BY market
            ORDER BY market
        """)
        rows = cursor.fetchall()
        market_names = {'A': 'A股', 'HK': '港股', 'US': '美股'}
        results = []
        for row in rows:
            cursor.execute(
                "SELECT COUNT(DISTINCT ts_code) FROM monthly_kline WHERE market = ?",
                (row[0],)
            )
            stock_count = cursor.fetchone()[0]
            results.append({
                'market':       row[0],
                'market_name':  market_names.get(row[0], row[0]),
                'data_count':   row[1],
                'earliest_date': row[2],
                'latest_date':   row[3],
                'stock_count':   stock_count,
            })
        conn.close()
        return results

    def get_market_completeness(self) -> List[Dict]:
        """获取各市场数据完整度：库中股票数 vs 有K线数据的股票数"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                s.market,
                COUNT(DISTINCT s.ts_code) AS total_stocks,
                COUNT(DISTINCT k.ts_code) AS stocks_with_klines
            FROM stocks s
            LEFT JOIN monthly_kline k ON s.ts_code = k.ts_code
            WHERE (s.delist_date IS NULL OR s.delist_date = '')
            GROUP BY s.market
            ORDER BY s.market
        """)
        market_names = {'A': 'A股', 'HK': '港股', 'US': '美股'}
        results = []
        for row in cursor.fetchall():
            market, total, with_klines = row[0], row[1] or 0, row[2] or 0
            results.append({
                'market':               market,
                'market_name':          market_names.get(market, market),
                'total_stocks':         total,
                'stocks_with_klines':   with_klines,
                'stocks_no_klines':     total - with_klines,
                'kline_coverage':       round(with_klines / total * 100, 1) if total > 0 else 0,
            })
        conn.close()
        return results

    def compare_data_sources(self, ts_code: str, trade_date: str = None, 
                            month: int = None, year: int = None) -> pd.DataFrame:
        """对比不同数据源的数据"""
        conn = self.get_connection()
        query = """
            SELECT ts_code, trade_date, year, month, open, close, pct_chg, data_source
            FROM monthly_kline 
            WHERE ts_code = ?
        """
        params = [ts_code]
        
        if trade_date:
            query += " AND trade_date = ?"
            params.append(trade_date)
        if month:
            query += " AND month = ?"
            params.append(month)
        if year:
            query += " AND year = ?"
            params.append(year)
        
        query += " ORDER BY trade_date, data_source"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    
    def get_latest_close(self, ts_code: str, data_source: str = None) -> Optional[float]:
        """获取某只股票最新一条记录的收盘价（用于增量更新时修复首行pct_chg NaN）"""
        conn = self.get_connection()
        cursor = conn.cursor()
        query = "SELECT close FROM monthly_kline WHERE ts_code = ?"
        params = [ts_code]
        if data_source:
            query += " AND data_source = ?"
            params.append(data_source)
        query += " ORDER BY trade_date DESC LIMIT 1"
        cursor.execute(query, params)
        result = cursor.fetchone()
        conn.close()
        return float(result[0]) if result and result[0] is not None else None

    def get_latest_trade_date(self, ts_code: str = None, data_source: str = None) -> Optional[str]:
        """获取最新的交易日期（支持按数据源过滤）"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = "SELECT MAX(trade_date) FROM monthly_kline WHERE 1=1"
        params = []
        
        if ts_code:
            query += " AND ts_code = ?"
            params.append(ts_code)
        
        if data_source:
            query += " AND data_source = ?"
            params.append(data_source)
        
        cursor.execute(query, params)
        result = cursor.fetchone()
        conn.close()
        return result[0] if result and result[0] else None
    
    def save_industry(self, ts_code: str, industry_name: str, level: str,
                     parent_code: str, industry_type: str = 'sw', market: str = ''):
        """保存行业分类"""
        conn = self.get_connection()
        cursor = conn.cursor()
        table = 'industry_sw' if industry_type == 'sw' else 'industry_citics'
        cursor.execute(f"""
            INSERT OR REPLACE INTO {table} (ts_code, industry_name, level, parent_code, market)
            VALUES (?, ?, ?, ?, ?)
        """, (ts_code, industry_name, level, parent_code, market))
        conn.commit()
        conn.close()

    def save_industry_batch(self, records: list, industry_type: str = 'sw'):
        """批量保存行业分类，records = [(ts_code, industry_name, level, parent_code, market), ...]"""
        if not records:
            return
        conn = self.get_connection()
        cursor = conn.cursor()
        table = 'industry_sw' if industry_type == 'sw' else 'industry_citics'
        cursor.executemany(f"""
            INSERT OR REPLACE INTO {table} (ts_code, industry_name, level, parent_code, market)
            VALUES (?, ?, ?, ?, ?)
        """, records)
        conn.commit()
        conn.close()

    def get_industry_stocks(self, industry_name: str, industry_type: str = 'sw', market: str = None) -> List[str]:
        """获取行业下的股票代码列表，可按市场过滤"""
        conn = self.get_connection()
        table = 'industry_sw' if industry_type == 'sw' else 'industry_citics'
        cursor = conn.cursor()
        if market:
            cursor.execute(f"SELECT DISTINCT ts_code FROM {table} WHERE industry_name = ? AND market = ?", (industry_name, market))
        else:
            cursor.execute(f"SELECT DISTINCT ts_code FROM {table} WHERE industry_name = ?", (industry_name,))
        results = cursor.fetchall()
        conn.close()
        return [r[0] for r in results]

    def get_all_industries(self, industry_type: str = 'sw', market: str = None) -> List[str]:
        """获取所有行业名称，可按市场过滤"""
        conn = self.get_connection()
        table = 'industry_sw' if industry_type == 'sw' else 'industry_citics'
        cursor = conn.cursor()
        if market:
            cursor.execute(f"SELECT DISTINCT industry_name FROM {table} WHERE market = ? ORDER BY industry_name", (market,))
        else:
            cursor.execute(f"SELECT DISTINCT industry_name FROM {table} ORDER BY industry_name")
        results = cursor.fetchall()
        conn.close()
        return [r[0] for r in results]

    def get_kline_bulk_for_month(self, month: int, start_year: int, end_year: int,
                                  data_source: str = None,
                                  stock_codes: list = None) -> pd.DataFrame:
        """批量获取指定月份范围内所有（或指定）股票的K线数据，一次查询替代N次单股查询"""
        query = "SELECT ts_code, year, month, pct_chg, data_source FROM monthly_kline WHERE month = ? AND year >= ? AND year <= ?"
        params = [month, start_year, end_year]
        if data_source:
            query += " AND data_source = ?"
            params.append(data_source)
        if stock_codes:
            placeholders = ','.join('?' * len(stock_codes))
            query += f" AND ts_code IN ({placeholders})"
            params.extend(stock_codes)
        conn = self.get_connection()
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df

    def get_all_industry_stock_mapping(self, industry_type: str = 'sw', market: str = None) -> pd.DataFrame:
        """获取全部行业股票映射表，一次查询替代N次逐行业查询，可按市场过滤"""
        table = 'industry_sw' if industry_type == 'sw' else 'industry_citics'
        conn = self.get_connection()
        if market:
            df = pd.read_sql_query(f"SELECT industry_name, ts_code FROM {table} WHERE market = ?", conn, params=[market])
        else:
            df = pd.read_sql_query(f"SELECT industry_name, ts_code FROM {table}", conn)
        conn.close()
        return df

    # ─── 服务中断补偿 ──────────────────────────────────────────────────────────

    def create_outage(self, title: str, description: str, started_at: str, created_by: int,
                      compensation_ratio: float = 1.5,
                      interruption_type: str = 'unplanned') -> int:
        """创建服务中断记录"""
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.now().strftime('%Y%m%d%H%M%S')
        cursor.execute("""
            INSERT INTO outage_logs
                (title, description, interruption_type, started_at, compensation_ratio, status, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, 'ongoing', ?, ?)
        """, (title, description, interruption_type, started_at, compensation_ratio, created_by, now))
        outage_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return outage_id

    def resolve_outage(self, outage_id: int, ended_at: str) -> bool:
        """标记服务中断结束，自动计算时长"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT started_at, status FROM outage_logs WHERE id = ?", (outage_id,))
        row = cursor.fetchone()
        if not row or row[1] != 'ongoing':
            conn.close()
            return False
        try:
            started = datetime.strptime(row[0], '%Y%m%d%H%M%S')
            ended = datetime.strptime(ended_at, '%Y%m%d%H%M%S')
            duration_minutes = max(1, int((ended - started).total_seconds() / 60))
        except ValueError:
            conn.close()
            return False
        cursor.execute("""
            UPDATE outage_logs
            SET ended_at = ?, duration_minutes = ?, status = 'resolved'
            WHERE id = ?
        """, (ended_at, duration_minutes, outage_id))
        conn.commit()
        conn.close()
        return True

    def get_all_outages(self) -> List[Dict]:
        """获取所有服务中断记录"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, title, description, interruption_type, started_at, ended_at,
                   duration_minutes, compensation_ratio, status, created_by, created_at, compensated_at
            FROM outage_logs
            ORDER BY created_at DESC
        """)
        rows = cursor.fetchall()
        conn.close()
        return [
            {
                'id': r[0], 'title': r[1], 'description': r[2],
                'interruption_type': r[3], 'started_at': r[4], 'ended_at': r[5],
                'duration_minutes': r[6], 'compensation_ratio': r[7], 'status': r[8],
                'created_by': r[9], 'created_at': r[10], 'compensated_at': r[11]
            }
            for r in rows
        ]

    def get_outage_by_id(self, outage_id: int) -> Optional[Dict]:
        """根据ID获取服务中断记录"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, title, description, interruption_type, started_at, ended_at,
                   duration_minutes, compensation_ratio, status, created_by, created_at, compensated_at
            FROM outage_logs WHERE id = ?
        """, (outage_id,))
        r = cursor.fetchone()
        conn.close()
        if not r:
            return None
        return {
            'id': r[0], 'title': r[1], 'description': r[2],
            'interruption_type': r[3], 'started_at': r[4], 'ended_at': r[5],
            'duration_minutes': r[6], 'compensation_ratio': r[7], 'status': r[8],
            'created_by': r[9], 'created_at': r[10], 'compensated_at': r[11]
        }

    def apply_outage_compensation(self, outage_id: int) -> Dict:
        """对所有有效订阅用户及试用用户发放补偿，返回补偿统计"""
        outage = self.get_outage_by_id(outage_id)
        if not outage:
            return {'success': False, 'message': '服务中断记录不存在'}
        if outage['status'] != 'resolved':
            return {'success': False, 'message': '请先标记中断结束再发放补偿'}

        duration = outage['duration_minutes'] or 0
        ratio = outage['compensation_ratio'] or 1.5
        comp_minutes = int(duration * ratio)
        if comp_minutes <= 0:
            return {'success': False, 'message': '补偿时长为0，请检查中断时长'}

        now = datetime.now()
        now_str = now.strftime('%Y%m%d%H%M%S')

        conn = self.get_connection()
        cursor = conn.cursor()

        sub_count = 0   # 付费订阅补偿人数
        trial_count = 0  # 试用补偿人数

        # ── 1. 付费订阅用户（users.valid_until 有效）──────────────────────────
        cursor.execute("""
            SELECT id, username, valid_until FROM users
            WHERE is_active = 1 AND role = 'user'
              AND valid_until IS NOT NULL AND valid_until > ?
        """, (now_str,))
        paid_users = cursor.fetchall()

        for user_id, username, valid_until in paid_users:
            try:
                expire_dt = datetime.strptime(valid_until, '%Y%m%d%H%M%S')
            except ValueError:
                continue
            new_valid_until = (expire_dt + timedelta(minutes=comp_minutes)).strftime('%Y%m%d%H%M%S')
            cursor.execute(
                "UPDATE users SET valid_until = ?, updated_at = ? WHERE id = ?",
                (new_valid_until, now_str, user_id)
            )
            cursor.execute("""
                INSERT INTO compensation_records
                    (outage_id, user_id, username, compensation_target,
                     compensated_minutes, original_valid_until, new_valid_until, created_at)
                VALUES (?, ?, ?, 'subscription', ?, ?, ?, ?)
            """, (outage_id, user_id, username, comp_minutes, valid_until, new_valid_until, now_str))
            sub_count += 1

        # ── 2. 试用用户（trials.status='active' 且 expires_at 有效）──────────
        # 排除已在付费订阅中补偿过的用户（valid_until 有效的已处理）
        paid_user_ids = {row[0] for row in paid_users}
        cursor.execute("""
            SELECT t.id, t.user_id, u.username, t.expires_at
            FROM trials t
            JOIN users u ON t.user_id = u.id
            WHERE t.status = 'active'
              AND t.expires_at IS NOT NULL AND t.expires_at > ?
              AND u.is_active = 1
        """, (now_str,))
        trial_rows = cursor.fetchall()

        for trial_id, user_id, username, expires_at in trial_rows:
            # 已有付费订阅的用户跳过（避免重复补偿）
            if user_id in paid_user_ids:
                continue
            try:
                expire_dt = datetime.strptime(expires_at, '%Y%m%d%H%M%S')
            except ValueError:
                continue
            new_expires_at = (expire_dt + timedelta(minutes=comp_minutes)).strftime('%Y%m%d%H%M%S')
            cursor.execute(
                "UPDATE trials SET expires_at = ? WHERE id = ?",
                (new_expires_at, trial_id)
            )
            cursor.execute("""
                INSERT INTO compensation_records
                    (outage_id, user_id, username, compensation_target,
                     compensated_minutes, original_valid_until, new_valid_until, created_at)
                VALUES (?, ?, ?, 'trial', ?, ?, ?, ?)
            """, (outage_id, user_id, username, comp_minutes, expires_at, new_expires_at, now_str))
            trial_count += 1

        cursor.execute("""
            UPDATE outage_logs SET status = 'compensated', compensated_at = ?
            WHERE id = ?
        """, (now_str, outage_id))

        conn.commit()
        conn.close()

        total = sub_count + trial_count
        return {
            'success': True,
            'compensated_users': total,
            'subscription_count': sub_count,
            'trial_count': trial_count,
            'compensated_minutes': comp_minutes,
            'message': f'已向 {total} 名用户延长 {comp_minutes} 分钟（订阅 {sub_count} 人，试用 {trial_count} 人）'
        }

    def get_compensation_records(self, outage_id: int) -> List[Dict]:
        """获取某次服务中断的补偿明细"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, outage_id, user_id, username, compensation_target,
                   compensated_minutes, original_valid_until, new_valid_until, created_at
            FROM compensation_records
            WHERE outage_id = ?
            ORDER BY created_at DESC
        """, (outage_id,))
        rows = cursor.fetchall()
        conn.close()
        return [
            {
                'id': r[0], 'outage_id': r[1], 'user_id': r[2], 'username': r[3],
                'compensation_target': r[4], 'compensated_minutes': r[5],
                'original_valid_until': r[6], 'new_valid_until': r[7], 'created_at': r[8]
            }
            for r in rows
        ]

    def get_user_compensation_records(self, user_id: int) -> List[Dict]:
        """获取某用户收到的所有补偿记录"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT cr.id, cr.outage_id, ol.title, cr.compensation_target,
                   cr.compensated_minutes, cr.original_valid_until, cr.new_valid_until, cr.created_at
            FROM compensation_records cr
            JOIN outage_logs ol ON cr.outage_id = ol.id
            WHERE cr.user_id = ?
            ORDER BY cr.created_at DESC
        """, (user_id,))
        rows = cursor.fetchall()
        conn.close()
        return [
            {
                'id': r[0], 'outage_id': r[1], 'outage_title': r[2],
                'compensation_target': r[3], 'compensated_minutes': r[4],
                'original_valid_until': r[5], 'new_valid_until': r[6], 'created_at': r[7]
            }
            for r in rows
        ]

