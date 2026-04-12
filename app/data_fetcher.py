"""
数据获取服务（支持tushare/BaoStock/akshare/yfinance/jqdata/alpha_vantage）
"""
import os
import tushare as ts
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import time
import requests
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from app.config import Config

# akshare 使用国内服务器（东方财富、腾讯等），不应走系统代理
# 强制将这些域名追加到 NO_PROXY，避免被系统代理拦截导致超时
_AKSHARE_NO_PROXY_DOMAINS = [
    # 东方财富
    'push2his.eastmoney.com', 'push2.eastmoney.com', 'datacenter-web.eastmoney.com',
    'push2delay.eastmoney.com', 'stock.eastmoney.com',
    'api.fund.eastmoney.com', 'fund.eastmoney.com', 'fundf10.eastmoney.com',
    '82.push2.eastmoney.com', '83.push2.eastmoney.com', '84.push2.eastmoney.com',
    '85.push2.eastmoney.com', '86.push2.eastmoney.com',
    # 新浪财经
    'hq.sinajs.cn', 'qt.gtimg.cn', 'ifzq.gtimg.cn',
    'finchina.com', 'finance.sina.com.cn', 'vip.stock.finance.sina.com.cn',
    # 上交所 / 深交所（获取股票列表）
    'query.sse.com.cn', 'www.sse.com.cn',
    'www.szse.cn', 'szse.cn',
    # 北交所
    'www.bse.cn', 'bse.cn',
    # 其他国内数据源
    'datacenter.eastmoney.com', 'push2ex.eastmoney.com',
    'quote.eastmoney.com', 'nufm.dfcfw.com',
]
def _merge_no_proxy(extra_domains):
    existing = os.environ.get('NO_PROXY', '') or os.environ.get('no_proxy', '')
    existing_set = {d.strip() for d in existing.split(',') if d.strip()}
    merged = ','.join(existing_set | set(extra_domains))
    os.environ['NO_PROXY'] = merged
    os.environ['no_proxy'] = merged
_merge_no_proxy(_AKSHARE_NO_PROXY_DOMAINS)

try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False

try:
    import jqdatasdk as jq
    JQDATA_AVAILABLE = True
except ImportError:
    JQDATA_AVAILABLE = False


class DataFetcher:
    _av_last_call: float = 0.0  # Alpha Vantage 速率限制时间戳（类级别）

    def __init__(self, config: Config):
        self.config = config
        self.data_source = config.get('data_source', 'tushare')
        self._init_data_source()
        # 代理配置：供 yfinance 等海外数据源使用
        proxy_enabled = config.get('proxy.enabled', False)
        proxy_http = config.get('proxy.http', '')
        proxy_https = config.get('proxy.https', '')
        if proxy_enabled and (proxy_http or proxy_https):
            if proxy_http:
                os.environ['HTTP_PROXY'] = proxy_http
            if proxy_https:
                os.environ['HTTPS_PROXY'] = proxy_https
        else:
            os.environ['HTTP_PROXY'] = ''
            os.environ['HTTPS_PROXY'] = ''
    
    def detect_market(self, ts_code: str) -> str:
        """
        识别股票所属市场
        返回: 'A', 'HK', 'US'
        """
        code_upper = ts_code.upper()
        
        # 港股：以.HK结尾
        if code_upper.endswith('.HK'):
            return 'HK'
        
        # 美股：以.US结尾，或纯字母代码（不带后缀）
        if code_upper.endswith('.US') or code_upper.endswith('.NYSE') or code_upper.endswith('.NASDAQ'):
            return 'US'
        
        # A股：以.SH或.SZ结尾
        if code_upper.endswith('.SH') or code_upper.endswith('.SZ'):
            return 'A'
        
        # 根据代码规则判断
        code_only = code_upper.split('.')[0]
        
        # A股：6位数字，以0/3/6/9开头（9开头为北交所）
        if code_only.isdigit() and len(code_only) == 6:
            if code_only[0] in ['0', '3', '6', '9']:
                return 'A'
        
        # 港股：5位数字，或1-4位数字（补0后是5位）
        if code_only.isdigit():
            if len(code_only) == 5:
                return 'HK'
            elif len(code_only) <= 4:  # 1-4位数字可能是港股（如700 -> 00700）
                # 检查是否可能是港股（港股代码通常是5位，A股是6位）
                if len(code_only) < 6:
                    return 'HK'
        
        # 美股：1-5位字母（默认）
        if code_only.isalpha() and 1 <= len(code_only) <= 5:
            return 'US'
        
        # 默认返回A股（向后兼容）
        return 'A'
    
    def normalize_code(self, code: str, market: str = None) -> str:
        """
        标准化股票代码格式
        A股: 000001.SZ, 600000.SH
        港股: 00700.HK (5位数字补0)
        美股: AAPL (不带后缀)
        """
        if market is None:
            market = self.detect_market(code)
        
        code_only = code.upper().split('.')[0]
        
        if market == 'A':
            # A股：保持现有格式（9开头为北交所）
            if code_only.startswith('0') or code_only.startswith('3'):
                return f"{code_only}.SZ"
            elif code_only.startswith('9'):
                return f"{code_only}.BJ"
            else:
                return f"{code_only}.SH"
        
        elif market == 'HK':
            # 港股：5位数字 + .HK（如 00700.HK）
            code_only = code_only.lstrip('0').zfill(5)  # 补0到5位（DB存储标准格式）
            return f"{code_only}.HK"

        elif market == 'US':
            # 美股：字母代码（不带后缀）
            return code_only

        return code

    def _to_yfinance_hk_code(self, ts_code: str) -> str:
        """将标准5位港股代码转为yfinance使用的4位格式（如 00700.HK → 0700.HK）"""
        code_only = ts_code.upper().split('.')[0]
        return f"{code_only.lstrip('0').zfill(4)}.HK"
    
    def get_data_source_for_market(self, market: str) -> str:
        """
        根据市场选择数据源
        """
        market_sources = self.config.get('market_data_sources', {})
        default_source = market_sources.get(market, self.data_source)
        return default_source
    
    def _init_data_source(self):
        """初始化所有激活数据源（market_data_sources 中配置的全部初始化）"""
        # 收集所有需要初始化的数据源
        active = {self.data_source}
        active.update(self.config.get('market_data_sources', {}).values())

        if 'tushare' in active:
            token = self.config.get('tushare.token', '')
            if token:
                ts.set_token(token)
                self.pro = ts.pro_api()

        if 'baostock' in active:
            lg = bs.login()
            if lg.error_code != '0':
                raise Exception(f"BaoStock登录失败: {lg.error_msg}")

        if 'akshare' in active:
            if not AKSHARE_AVAILABLE:
                raise ImportError("akshare未安装，请使用: pip install akshare")

        if 'jqdata' in active:
            if not JQDATA_AVAILABLE:
                raise ImportError("jqdatasdk未安装，请使用: pip install jqdatasdk")
            username = self.config.get('jqdata.username', '')
            password = self.config.get('jqdata.password', '')
            if username and password:
                jq.auth(username, password)
            else:
                raise ValueError("聚宽账号未配置，请填写 jqdata.username 和 jqdata.password")
    
    def get_stock_list(self, market: str = None) -> pd.DataFrame:
        """
        获取股票列表
        market: 'A', 'HK', 'US' 或 None（全部三个市场合并）
        """
        if market == 'HK':
            return self._get_stock_list_hk()
        elif market == 'US':
            return self._get_stock_list_us()
        elif market == 'A':
            return self._get_stock_list_a()
        elif market is None:
            # 全部市场：合并 A + HK + US
            dfs = []
            a_df = self._get_stock_list_a()
            if not a_df.empty:
                dfs.append(a_df)
            hk_df = self._get_stock_list_hk()
            if not hk_df.empty:
                dfs.append(hk_df)
            us_df = self._get_stock_list_us()
            if not us_df.empty:
                dfs.append(us_df)
            if not dfs:
                return pd.DataFrame()
            return pd.concat(dfs, ignore_index=True)
        else:
            raise ValueError(f"Unsupported market: {market}")

    def _get_stock_list_a(self) -> pd.DataFrame:
        """获取A股股票列表（按配置选择数据源）"""
        a_source = self.get_data_source_for_market('A')
        if a_source == 'tushare':
            return self._get_stock_list_tushare()
        elif a_source == 'baostock':
            return self._get_stock_list_baostock()
        elif a_source == 'akshare':
            return self._get_stock_list_akshare()
        elif a_source == 'jqdata':
            return self._get_stock_list_jqdata()
        else:
            raise ValueError(f"Unsupported data source for A股: {a_source}")
    
    def _normalize_hk_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """将含 ts_code/name 的港股 DataFrame 标准化为统一输出格式"""
        df = df[df['ts_code'].notna() & (df['ts_code'] != '')].copy()
        df['ts_code'] = df['ts_code'].apply(lambda x: self.normalize_code(str(x), 'HK'))
        df['symbol'] = df['ts_code'].str.replace('.HK', '', regex=False)
        df['exchange'] = 'HK'
        df['market'] = 'HK'
        df['currency'] = 'HKD'
        df['delist_date'] = df.get('delist_date', '')
        df['is_hs'] = df.get('is_hs', '')
        for col in ('area', 'industry', 'list_date', 'delist_date', 'is_hs'):
            if col not in df.columns:
                df[col] = ''
            else:
                df[col] = df[col].fillna('')
        df['name'] = df['name'].fillna('') if 'name' in df.columns else df['ts_code']
        df.loc[df['name'] == '', 'name'] = df.loc[df['name'] == '', 'ts_code']
        df = df[df['name'].notna() & (df['name'] != '')]
        return df[['ts_code', 'symbol', 'name', 'area', 'industry',
                   'list_date', 'delist_date', 'is_hs', 'exchange', 'market', 'currency']]

    def _get_stock_list_from_db(self, market: str) -> pd.DataFrame:
        """从数据库缓存获取股票列表（毫秒级，无需网络）"""
        try:
            from app.database import Database
            db = Database()
            df = db.get_stocks(exclude_delisted=True, market=market)
            if not df.empty:
                print(f"从数据库缓存获取到 {len(df)} 只{market}股票")
                return df
        except Exception as e:
            print(f"从数据库缓存获取{market}股票列表失败: {e}")
        return pd.DataFrame()

    def _get_stock_list_hk(self) -> pd.DataFrame:
        """获取港股股票列表（优先Tushare → 数据库缓存 → akshare兜底）"""
        # 1. 尝试 Tushare
        try:
            if self.data_source == 'tushare':
                pro = self.pro
            else:
                token = self.config.get('tushare.token', '')
                if token:
                    ts.set_token(token)
                    pro = ts.pro_api()
                else:
                    pro = None

            if pro is not None:
                df = pro.hk_basic(exchange='HKEX', list_status='L',
                                  fields='ts_code,symbol,name,list_date,area,industry')
                if df is not None and not df.empty:
                    print(f"从Tushare获取到 {len(df)} 只港股")
                    return self._normalize_hk_df(df)
        except Exception as e:
            print(f"Tushare获取港股列表失败: {e}，尝试数据库缓存...")

        # 2. 数据库缓存（上次成功更新后保留，毫秒级）
        db_df = self._get_stock_list_from_db('HK')
        if not db_df.empty:
            return db_df

        # 3. akshare 兜底（无需Token，但耗时约2-3分钟）
        print("数据库无港股缓存，尝试akshare（耗时约2-3分钟）...")
        return self._get_stock_list_hk_akshare()

    def _get_stock_list_hk_akshare(self) -> pd.DataFrame:
        """从akshare获取港股股票列表（备用，无需Token）
        先尝试 stock_hk_spot_em（46页，约2分钟，更稳定），
        失败再尝试 stock_hk_spot（99页，约3分钟）
        """
        if not AKSHARE_AVAILABLE:
            print("akshare未安装，无法获取港股列表")
            return pd.DataFrame()

        def _parse_hk_df(df: pd.DataFrame) -> pd.DataFrame:
            code_col = next((c for c in ('代码',) if c in df.columns), None)
            name_col = next((c for c in ('中文名称', '名称', '英文名称') if c in df.columns), None)
            if code_col is None:
                return pd.DataFrame()
            result = pd.DataFrame()
            result['ts_code'] = df[code_col].astype(str).apply(
                lambda x: self.normalize_code(x, 'HK'))
            result['symbol'] = result['ts_code'].str.replace('.HK', '', regex=False)
            result['name'] = df[name_col].fillna('') if name_col else result['ts_code']
            result.loc[result['name'] == '', 'name'] = result.loc[result['name'] == '', 'ts_code']
            for col in ('area', 'industry', 'list_date', 'delist_date', 'is_hs'):
                result[col] = ''
            result['exchange'] = 'HK'
            result['market'] = 'HK'
            result['currency'] = 'HKD'
            result = result[result['name'].notna() & (result['name'] != '')]
            return result[['ts_code', 'symbol', 'name', 'area', 'industry',
                           'list_date', 'delist_date', 'is_hs', 'exchange', 'market', 'currency']]

        # 优先用东方财富接口（46页，约2分钟，更稳定）
        try:
            df = ak.stock_hk_spot_em()
            if df is not None and not df.empty:
                result = _parse_hk_df(df)
                if not result.empty:
                    print(f"从akshare(东财)获取到 {len(result)} 只港股")
                    return result
        except Exception as e:
            print(f"akshare(东财)港股列表失败: {e}，尝试新浪接口...")

        # 次备用：新浪接口（99页，约3分钟）
        try:
            df = ak.stock_hk_spot()
            if df is not None and not df.empty:
                result = _parse_hk_df(df)
                if not result.empty:
                    print(f"从akshare(新浪)获取到 {len(result)} 只港股")
                    return result
        except Exception as e:
            print(f"akshare(新浪)港股列表失败: {e}")

        return pd.DataFrame()
    
    def _fetch_us_basic_all(self, pro) -> pd.DataFrame:
        """分页获取tushare完整美股列表（单次最多6000条，需翻页）"""
        PAGE_SIZE = 6000
        all_dfs = []
        offset = 0
        while True:
            try:
                batch = pro.us_basic(exchange='', list_status='L',
                                     limit=PAGE_SIZE, offset=offset)
            except Exception as e:
                print(f"us_basic 分页请求失败 offset={offset}: {e}")
                break
            if batch is None or batch.empty:
                break
            valid = batch.dropna(subset=['ts_code'])
            all_dfs.append(valid)
            print(f"  us_basic offset={offset} 获取 {len(valid)} 条")
            if len(batch) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            time.sleep(0.5)
        return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    def _normalize_us_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """将含 symbol/ts_code/name 的美股 DataFrame 标准化为统一输出格式"""
        if 'symbol' in df.columns:
            df = df[df['symbol'].notna() & (df['symbol'] != '')].copy()
            df['ts_code'] = df['symbol'].apply(lambda x: self.normalize_code(str(x), 'US'))
            df['symbol'] = df['ts_code']
        elif 'ts_code' in df.columns:
            df = df[df['ts_code'].notna() & (df['ts_code'] != '')].copy()
            df['ts_code'] = df['ts_code'].apply(lambda x: self.normalize_code(str(x), 'US'))
            df['symbol'] = df['ts_code']
        else:
            first_col = df.columns[0]
            df = df[df[first_col].notna()].copy()
            df['ts_code'] = df[first_col].apply(lambda x: self.normalize_code(str(x), 'US'))
            df['symbol'] = df['ts_code']
        if 'exchange' not in df.columns:
            df['exchange'] = 'NYSE'
        df['market'] = 'US'
        df['currency'] = 'USD'
        df['delist_date'] = ''
        df['is_hs'] = ''
        for col in ('area', 'industry', 'list_date'):
            if col not in df.columns:
                df[col] = ''
            else:
                df[col] = df[col].fillna('')
        if 'name' not in df.columns:
            df['name'] = ''
        if 'enname' in df.columns:
            df['name'] = df['name'].fillna(df['enname'])
        df['name'] = df['name'].fillna(df['ts_code'])
        df.loc[df['name'] == '', 'name'] = df.loc[df['name'] == '', 'ts_code']
        df['symbol'] = df['symbol'].fillna(df['ts_code'])
        df['is_hs'] = df['is_hs'].fillna('') if 'is_hs' in df.columns else ''
        df = df[df['name'].notna() & (df['name'] != '')]
        df = df.drop_duplicates(subset=['ts_code'], keep='first')
        return df[['ts_code', 'symbol', 'name', 'area', 'industry',
                   'list_date', 'delist_date', 'is_hs', 'exchange', 'market', 'currency']]

    def _get_stock_list_us(self) -> pd.DataFrame:
        """获取美股股票列表（优先Tushare → Alpha Vantage → 数据库缓存 → akshare兜底）"""
        us_source = self.get_data_source_for_market('US')

        # 1. 尝试 Tushare
        try:
            if self.data_source == 'tushare':
                pro = self.pro
            else:
                token = self.config.get('tushare.token', '')
                if token:
                    ts.set_token(token)
                    pro = ts.pro_api()
                else:
                    pro = None

            if pro is not None:
                df = self._fetch_us_basic_all(pro)
                if df is not None and not df.empty:
                    print(f"从Tushare获取到 {len(df)} 只美股")
                    return self._normalize_us_df(df)
        except Exception as e:
            print(f"Tushare获取美股列表失败: {e}，尝试下一数据源...")

        # 2. Alpha Vantage（当US数据源配置为alpha_vantage时）
        if us_source == 'alpha_vantage':
            av_df = self._get_stock_list_us_alpha_vantage()
            if not av_df.empty:
                return av_df
            print("Alpha Vantage获取美股列表失败，尝试数据库缓存...")

        # 3. 数据库缓存（上次成功更新后保留，毫秒级）
        db_df = self._get_stock_list_from_db('US')
        if not db_df.empty:
            return db_df

        # 4. akshare 兜底（无需Token，但耗时约15-20分钟）
        print("数据库无美股缓存，尝试akshare（耗时约15-20分钟）...")
        return self._get_stock_list_us_akshare()

    def _get_stock_list_us_alpha_vantage(self) -> pd.DataFrame:
        """从Alpha Vantage LISTING_STATUS获取美股股票列表（返回CSV，一次请求覆盖全量）"""
        api_key = self.config.get('alpha_vantage.api_key', '')
        if not api_key:
            print("Alpha Vantage API Key未配置，无法获取美股列表")
            return pd.DataFrame()
        try:
            from io import StringIO
            url = f"https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={api_key}"
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text))
            # 只保留在市普通股，过滤ETF
            df = df[(df['status'] == 'Active') & (df['assetType'] == 'Stock')].copy()
            if df.empty:
                return pd.DataFrame()
            result = pd.DataFrame()
            result['ts_code']     = df['symbol'].apply(lambda x: self.normalize_code(str(x), 'US'))
            result['symbol']      = result['ts_code']
            result['name']        = df['name'].fillna(df['symbol'])
            result['exchange']    = df['exchange'].fillna('NYSE')
            result['list_date']   = df['ipoDate'].fillna('').astype(str).str.replace('-', '', regex=False)
            result['delist_date'] = ''
            result['area']        = ''
            result['industry']    = ''
            result['is_hs']       = ''
            result['market']      = 'US'
            result['currency']    = 'USD'
            result = result[result['name'].notna() & (result['name'] != '')]
            print(f"从Alpha Vantage获取到 {len(result)} 只美股")
            return result[['ts_code', 'symbol', 'name', 'area', 'industry',
                           'list_date', 'delist_date', 'is_hs', 'exchange', 'market', 'currency']]
        except Exception as e:
            print(f"Alpha Vantage获取美股列表失败: {e}")
            return pd.DataFrame()

    def _get_stock_list_us_akshare(self) -> pd.DataFrame:
        """从akshare获取美股股票列表（备用，无需Token）"""
        if not AKSHARE_AVAILABLE:
            print("akshare未安装，无法获取美股列表")
            return pd.DataFrame()
        try:
            df = ak.get_us_stock_name()
            if df is None or df.empty:
                return pd.DataFrame()

            # akshare 返回列: 'symbol'（如 AAPL）、'cname'（中文名）、'name'（英文名）
            df = df[df['symbol'].notna() & (df['symbol'] != '')].copy()
            result = pd.DataFrame()
            result['ts_code'] = df['symbol'].astype(str).apply(
                lambda x: self.normalize_code(x, 'US'))
            result['symbol'] = result['ts_code']
            if 'cname' in df.columns:
                result['name'] = df['cname'].fillna(df.get('name', result['ts_code']))
            elif 'name' in df.columns:
                result['name'] = df['name'].fillna(result['ts_code'])
            else:
                result['name'] = result['ts_code']
            result['name'] = result['name'].fillna(result['ts_code'])
            result.loc[result['name'] == '', 'name'] = result.loc[result['name'] == '', 'ts_code']
            for col in ('area', 'industry', 'list_date', 'delist_date', 'is_hs'):
                result[col] = ''
            result['exchange'] = 'NYSE'
            result['market'] = 'US'
            result['currency'] = 'USD'
            result = result[result['name'].notna() & (result['name'] != '')]
            print(f"从akshare获取到 {len(result)} 只美股")
            return result[['ts_code', 'symbol', 'name', 'area', 'industry',
                           'list_date', 'delist_date', 'is_hs', 'exchange', 'market', 'currency']]
        except Exception as e:
            print(f"akshare获取美股列表失败: {e}")
            return pd.DataFrame()
    
    def _get_stock_list_tushare(self) -> pd.DataFrame:
        """从tushare获取股票列表"""
        df = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,delist_date,is_hs,exchange')
        if df is None or df.empty:
            return pd.DataFrame()
        # 补充 market/currency 字段
        df['market'] = 'A'
        df['currency'] = 'CNY'
        # NOT NULL 兜底
        df['name'] = df['name'].fillna(df['ts_code']) if 'name' in df.columns else df['ts_code']
        df.loc[df['name'] == '', 'name'] = df.loc[df['name'] == '', 'ts_code']
        df['symbol'] = df['symbol'].fillna(df['ts_code']) if 'symbol' in df.columns else df['ts_code']
        for col in ('area', 'industry', 'list_date', 'delist_date', 'is_hs'):
            if col in df.columns:
                df[col] = df[col].fillna('')
            else:
                df[col] = ''
        df = df[df['name'].notna() & (df['name'] != '')]
        return df
    
    def _get_stock_list_baostock(self) -> pd.DataFrame:
        """从BaoStock获取股票列表"""
        # BaoStock没有提供股票列表接口，需要从数据库或其他数据源获取
        # 优先从数据库获取已有的A股股票列表
        try:
            from app.database import Database
            db = Database()
            stocks_df = db.get_stocks(exclude_delisted=True, market='A')
            if not stocks_df.empty:
                print(f"从数据库获取到 {len(stocks_df)} 只A股股票")
                return stocks_df
        except Exception as e:
            print(f"从数据库获取股票列表失败: {e}")
        
        # 如果数据库没有股票列表，尝试从akshare获取（akshare不需要token）
        try:
            if AKSHARE_AVAILABLE:
                print("尝试从akshare获取股票列表...")
                df = ak.stock_info_a_code_name()
                # 转换格式以匹配tushare格式
                df['ts_code'] = df['code'].apply(lambda x: f"{x}.SZ" if x.startswith('0') or x.startswith('3') else (f"{x}.BJ" if x.startswith('9') else f"{x}.SH"))
                df['symbol'] = df['code']
                df['name'] = df['name']
                df['list_date'] = ''  # akshare不提供上市日期
                df['delist_date'] = ''
                df['exchange'] = df['code'].apply(lambda x: 'SZ' if x.startswith('0') or x.startswith('3') else ('BSE' if x.startswith('9') else 'SH'))
                df['market'] = 'A'
                df['currency'] = 'CNY'
                result_df = df[['ts_code', 'symbol', 'name', 'list_date', 'delist_date', 'exchange', 'market', 'currency']]
                print(f"从akshare获取到 {len(result_df)} 只股票")
                return result_df
        except Exception as e:
            print(f"从akshare获取股票列表失败: {e}")
        
        # 如果akshare失败，尝试从tushare获取（如果配置了token）
        try:
            token = self.config.get('tushare.token', '')
            if token:
                print("尝试从tushare获取股票列表...")
                ts.set_token(token)
                pro = ts.pro_api()
                df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,delist_date,is_hs,exchange')
                print(f"从tushare获取到 {len(df)} 只股票")
                return df
        except Exception as e:
            print(f"从tushare获取股票列表失败: {e}")
        
        # 如果都失败，返回空DataFrame
        print("错误: 无法获取股票列表，baostock数据源需要先有其他数据源（akshare或tushare）的股票列表")
        return pd.DataFrame()
    
    def _get_stock_list_akshare(self) -> pd.DataFrame:
        """从akshare获取股票列表（带超时保护和tushare备用）"""
        import threading

        result_box = [None]
        error_box = [None]

        def _fetch_ak():
            try:
                result_box[0] = ak.stock_info_a_code_name()
            except Exception as e:
                error_box[0] = e

        t = threading.Thread(target=_fetch_ak, daemon=True)
        t.start()
        t.join(timeout=30)  # 最多等30秒，守护线程不阻塞主流程

        df_ak = None
        if t.is_alive():
            print("akshare股票列表获取超时（>30s），尝试备用数据源")
        elif error_box[0]:
            print(f"akshare股票列表获取失败: {error_box[0]}")
        else:
            df_ak = result_box[0]

        if df_ak is not None and not df_ak.empty:
            df = df_ak
            df['ts_code'] = df['code'].apply(lambda x: f"{x}.SZ" if x.startswith('0') or x.startswith('3') else (f"{x}.BJ" if x.startswith('9') else f"{x}.SH"))
            df['symbol'] = df['code']
            df['list_date'] = ''
            df['delist_date'] = ''
            df['exchange'] = df['code'].apply(lambda x: 'SZ' if x.startswith('0') or x.startswith('3') else ('BSE' if x.startswith('9') else 'SH'))
            df['market'] = 'A'
            df['currency'] = 'CNY'
            print(f"从akshare获取到 {len(df)} 只A股股票")
            return df[['ts_code', 'symbol', 'name', 'list_date', 'delist_date', 'exchange', 'market', 'currency']]

        # akshare失败 → 尝试tushare备用
        token = self.config.get('tushare.token', '')
        if token:
            try:
                print("akshare获取失败，改用tushare获取A股股票列表...")
                ts.set_token(token)
                pro = ts.pro_api()
                df = pro.stock_basic(exchange='', list_status='L',
                                     fields='ts_code,symbol,name,area,industry,list_date,delist_date,is_hs,exchange')
                if df is not None and not df.empty:
                    df['market'] = 'A'
                    df['currency'] = 'CNY'
                    for col in ('area', 'industry', 'is_hs'):
                        if col not in df.columns:
                            df[col] = ''
                    print(f"从tushare获取到 {len(df)} 只A股股票")
                    return df
            except Exception as e2:
                print(f"tushare备用也失败: {e2}")

        # akshare + tushare 均失败 → 数据库缓存兜底
        db_df = self._get_stock_list_from_db('A')
        if not db_df.empty:
            return db_df

        return pd.DataFrame()

    def _get_stock_list_jqdata(self) -> pd.DataFrame:
        """从聚宽获取A股股票列表"""
        try:
            if not JQDATA_AVAILABLE:
                raise ImportError("jqdatasdk未安装")
            df = jq.get_all_securities('stock')
            df = df.reset_index()
            df.columns = ['jq_code', 'display_name', 'name', 'start_date', 'end_date', 'type']
            # 转换代码格式：000001.XSHE → 000001.SZ，600000.XSHG → 600000.SH
            def jq_to_ts(code):
                c, exchange = code.split('.')
                if exchange == 'XSHE':
                    return f"{c}.SZ"
                elif exchange == 'XSHG':
                    return f"{c}.SH"
                return code
            df['ts_code'] = df['jq_code'].apply(jq_to_ts)
            df['symbol'] = df['ts_code'].apply(lambda x: x.split('.')[0])
            df['name'] = df['display_name']
            df['list_date'] = pd.to_datetime(df['start_date']).dt.strftime('%Y%m%d')
            df['delist_date'] = df['end_date'].apply(
                lambda x: pd.to_datetime(x).strftime('%Y%m%d') if pd.notna(x) and str(x) != '2200-01-01' else '')
            df['exchange'] = df['ts_code'].apply(lambda x: 'SZ' if x.endswith('.SZ') else 'SH')
            df['market'] = 'A'
            df['currency'] = 'CNY'
            return df[['ts_code', 'symbol', 'name', 'list_date', 'delist_date', 'exchange', 'market', 'currency']]
        except Exception as e:
            print(f"Error fetching stock list from jqdata: {e}")
            return pd.DataFrame()

    def get_monthly_kline(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取月K线数据（自动识别市场并选择数据源）"""
        market = self.detect_market(ts_code)
        data_source = self.get_data_source_for_market(market)
        
        # 根据市场选择数据源和方法
        if market == 'HK':
            if data_source == 'yfinance':
                return self._get_monthly_kline_yfinance_hk(ts_code, start_date, end_date)
            elif data_source == 'tushare':
                return self._get_monthly_kline_tushare_hk(ts_code, start_date, end_date)
            elif data_source == 'akshare':
                return self._get_monthly_kline_akshare_hk(ts_code, start_date, end_date)
            else:
                # 默认使用yfinance
                return self._get_monthly_kline_yfinance_hk(ts_code, start_date, end_date)
        
        elif market == 'US':
            if data_source == 'yfinance':
                return self._get_monthly_kline_yfinance_us(ts_code, start_date, end_date)
            elif data_source == 'tushare':
                return self._get_monthly_kline_tushare_us(ts_code, start_date, end_date)
            elif data_source == 'akshare':
                return self._get_monthly_kline_akshare_us(ts_code, start_date, end_date)
            elif data_source == 'alpha_vantage':
                return self._get_monthly_kline_alpha_vantage_us(ts_code, start_date, end_date)
            else:
                # 默认使用yfinance
                return self._get_monthly_kline_yfinance_us(ts_code, start_date, end_date)
        
        else:
            # A股：根据 market_data_sources 配置选择
            a_source = self.get_data_source_for_market('A')
            # 北交所（.BJ）baostock 不覆盖，强制降级为 akshare
            is_bj = ts_code.endswith('.BJ')
            if a_source == 'tushare':
                return self._get_monthly_kline_tushare(ts_code, start_date, end_date)
            elif a_source == 'baostock':
                if is_bj:
                    return self._get_monthly_kline_akshare(ts_code, start_date, end_date)
                return self._get_monthly_kline_baostock(ts_code, start_date, end_date)
            elif a_source == 'akshare':
                return self._get_monthly_kline_akshare(ts_code, start_date, end_date)
            elif a_source == 'jqdata':
                if is_bj:
                    return self._get_monthly_kline_akshare(ts_code, start_date, end_date)
                return self._get_monthly_kline_jqdata(ts_code, start_date, end_date)
            else:
                raise ValueError(f"Unsupported data source for A股: {a_source}")
    
    def _get_monthly_kline_tushare(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从tushare获取月K线（使用前复权数据）"""
        try:
            # 使用pro_bar获取前复权月线数据
            import tushare as ts
            df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date, freq='M')
            if df is not None and not df.empty:
                # 处理日期格式
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
                df['year'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.year
                df['month'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.month
                # 确保有ts_code字段
                if 'ts_code' not in df.columns:
                    df['ts_code'] = ts_code
                # 计算涨跌幅（如果需要）
                df = self.calculate_pct_chg(df)
                return df
        except Exception as e:
            print(f"Error fetching monthly adjusted data from tushare: {e}")
        
        # 如果月线数据获取失败，从日线前复权数据计算月线
        try:
            import tushare as ts
            df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date, freq='D')
            if df is not None and not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                df['year'] = df['trade_date'].dt.year
                df['month'] = df['trade_date'].dt.month
                
                # 按月聚合
                # 开盘价：取每月第一个交易日的开盘价
                # 收盘价：取每月最后一天的收盘价
                monthly_first = df.groupby(['year', 'month']).first().reset_index()
                monthly_last = df.groupby(['year', 'month']).last().reset_index()
                
                # 合并数据
                monthly_df = monthly_last[['year', 'month', 'trade_date']].copy()
                monthly_df['trade_date'] = monthly_df['trade_date'].dt.strftime('%Y%m%d')
                monthly_df['ts_code'] = ts_code
                monthly_df['open'] = monthly_first['open'].values  # 第一个交易日的开盘价
                monthly_df['close'] = monthly_last['close'].values  # 最后一天的收盘价
                
                # 计算月K涨跌幅：需要获取上月的收盘价
                monthly_df = monthly_df.sort_values('trade_date')
                for idx, row in monthly_df.iterrows():
                    year = row['year']
                    month = row['month']
                    # 计算上月日期
                    if month == 1:
                        prev_year = year - 1
                        prev_month = 12
                    else:
                        prev_year = year
                        prev_month = month - 1
                    
                    # 获取上月最后一天的收盘价
                    try:
                        prev_start = f"{prev_year}{prev_month:02d}01"
                        prev_end = f"{prev_year}{prev_month:02d}31"
                        prev_df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=prev_start, end_date=prev_end, freq='D')
                        if prev_df is not None and not prev_df.empty:
                            prev_df = prev_df.sort_values('trade_date')
                            prev_close = prev_df.iloc[-1]['close']
                            current_close = row['close']
                            if pd.notna(prev_close) and pd.notna(current_close) and prev_close > 0:
                                monthly_df.loc[idx, 'pct_chg'] = (current_close - prev_close) / prev_close * 100
                    except:
                        pass
                
                # 如果没有pct_chg，使用close的pct_change
                if 'pct_chg' not in monthly_df.columns or monthly_df['pct_chg'].isna().all():
                    monthly_df = self.calculate_pct_chg(monthly_df)
                
                return monthly_df
        except Exception as e:
            print(f"Error fetching daily adjusted data from tushare: {e}")
        
        return pd.DataFrame()
    
    def _get_monthly_kline_baostock(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从BaoStock获取月K线"""
        try:
            # 确保已登录（如果登录失败，重新登录）
            try:
                lg = bs.login()
                if lg.error_code != '0':
                    print(f"BaoStock登录失败: {lg.error_msg}")
                    return pd.DataFrame()
            except:
                pass  # 如果已经登录，忽略错误
            
            # BaoStock代码格式转换（如000001.SZ -> sz.000001）
            if ts_code.endswith('.SZ'):
                code = f"sz.{ts_code.replace('.SZ', '')}"
            elif ts_code.endswith('.SH'):
                code = f"sh.{ts_code.replace('.SH', '')}"
            elif ts_code.endswith('.BJ'):
                code = f"bj.{ts_code.replace('.BJ', '')}"
            else:
                # 如果没有后缀，根据代码判断
                if ts_code.startswith('0') or ts_code.startswith('3'):
                    code = f"sz.{ts_code}"
                elif ts_code.startswith('9'):
                    code = f"bj.{ts_code}"
                else:
                    code = f"sh.{ts_code}"
            
            # BaoStock日期格式需要是 YYYY-MM-DD
            start_date_formatted = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
            end_date_formatted = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
            
            rs = bs.query_history_k_data_plus(
                code,
                "date,open,high,low,close,volume,amount",
                start_date=start_date_formatted,
                end_date=end_date_formatted,
                frequency="m",
                adjustflag="3"  # 前复权
            )
            
            # 检查返回结果
            if rs is None:
                print(f"BaoStock查询返回None: {ts_code} ({code})")
                return pd.DataFrame()
            
            if rs.error_code != '0':
                print(f"BaoStock查询错误 {ts_code} ({code}): {rs.error_msg}")
                return pd.DataFrame()
            
            df = rs.get_data()
            if df.empty:
                print(f"BaoStock返回空数据: {ts_code} ({code})")
                return pd.DataFrame()
            
            # 确保有date列
            if 'date' not in df.columns:
                print(f"BaoStock返回数据缺少date列: {ts_code} ({code})")
                return pd.DataFrame()
            
            # 转换数值列为数值类型（baostock返回的是字符串）
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df['ts_code'] = ts_code
            df['trade_date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
            df['year'] = pd.to_datetime(df['date']).dt.year
            df['month'] = pd.to_datetime(df['date']).dt.month
            df['pct_chg'] = df['close'].pct_change() * 100
            df = df.rename(columns={'volume': 'vol'})
            
            return df[['ts_code', 'trade_date', 'year', 'month', 'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg']]
        except Exception as e:
            print(f"Error fetching baostock data for {ts_code}: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def _get_monthly_kline_akshare(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从akshare获取月K线（前复权）—— 使用新浪接口（stock_zh_a_daily），单次请求，无线程问题"""
        try:
            code_only = ts_code.split('.')[0]
            if ts_code.endswith('.SZ'):
                exchange = 'sz'
            elif ts_code.endswith('.BJ'):
                exchange = 'bj'
            else:
                exchange = 'sh'
            symbol = f"{exchange}{code_only}"

            start_dt = pd.to_datetime(start_date, format='%Y%m%d')
            end_dt = pd.to_datetime(end_date, format='%Y%m%d')

            # stock_zh_a_daily 返回全量日线数据（单次请求，无 tqdm 输出）
            df = ak.stock_zh_a_daily(symbol=symbol, adjust='qfq')

            if df is None or df.empty:
                return pd.DataFrame()

            df['date'] = pd.to_datetime(df['date'])

            # 往前多取一个月，用于计算首月涨跌幅
            prev_month_start = (start_dt - pd.DateOffset(months=1)).replace(day=1)
            df = df[(df['date'] >= prev_month_start) & (df['date'] <= end_dt)]
            if df.empty:
                return pd.DataFrame()

            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['trade_date'] = df['date'].dt.strftime('%Y%m%d')

            # 获取上个月最后收盘价（用于首月涨跌幅）
            prev_close = None
            prev_df = df[df['date'] < start_dt]
            if not prev_df.empty:
                prev_close = prev_df.sort_values('date').iloc[-1]['close']

            # 只保留请求范围内的数据
            df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
            if df.empty:
                return pd.DataFrame()

            # 按月聚合
            monthly_first = df.groupby(['year', 'month']).first().reset_index()
            monthly_last  = df.groupby(['year', 'month']).last().reset_index()

            monthly_df = monthly_last[['year', 'month', 'trade_date']].copy()
            monthly_df['ts_code'] = ts_code
            monthly_df['open']   = monthly_first['open'].values
            monthly_df['close']  = monthly_last['close'].values
            monthly_df['high']   = df.groupby(['year', 'month'])['high'].max().values
            monthly_df['low']    = df.groupby(['year', 'month'])['low'].min().values
            monthly_df['vol']    = df.groupby(['year', 'month'])['volume'].sum().values if 'volume' in df.columns else 0
            monthly_df['amount'] = df.groupby(['year', 'month'])['amount'].sum().values if 'amount' in df.columns else 0

            # 计算月K涨跌幅
            monthly_df = monthly_df.sort_values('trade_date').reset_index(drop=True)
            prev_month_close = prev_close
            for idx, row in monthly_df.iterrows():
                current_close = row['close']
                current_open  = row['open']
                if prev_month_close is not None and pd.notna(prev_month_close) and prev_month_close > 0:
                    if pd.notna(current_close):
                        monthly_df.loc[idx, 'pct_chg'] = (current_close - prev_month_close) / prev_month_close * 100
                elif pd.notna(current_open) and current_open > 0:
                    if pd.notna(current_close):
                        monthly_df.loc[idx, 'pct_chg'] = (current_close - current_open) / current_open * 100
                prev_month_close = row['close']

            if 'pct_chg' not in monthly_df.columns or monthly_df['pct_chg'].isna().all():
                monthly_df = self.calculate_pct_chg(monthly_df)

            return monthly_df[['ts_code', 'trade_date', 'year', 'month', 'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg']]
        except Exception as e:
            print(f"Error fetching monthly kline from akshare: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def _get_monthly_kline_jqdata(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从聚宽获取A股月K线（前复权）"""
        try:
            if not JQDATA_AVAILABLE:
                raise ImportError("jqdatasdk未安装")
            # 转换代码格式：000001.SZ → 000001.XSHE，600000.SH → 600000.XSHG
            code, market = ts_code.split('.')
            jq_code = f"{code}.XSHE" if market == 'SZ' else f"{code}.XSHG"
            # 扩展起始日期一个月，用于计算首月涨跌幅
            start_dt = pd.to_datetime(start_date, format='%Y%m%d')
            extended_start = (start_dt - pd.DateOffset(months=1)).strftime('%Y-%m-%d')
            end_str = pd.to_datetime(end_date, format='%Y%m%d').strftime('%Y-%m-%d')
            df = jq.get_price(jq_code, start_date=extended_start, end_date=end_str,
                              frequency='monthly', fields=['open', 'close', 'high', 'low', 'volume', 'money'],
                              panel=False, fq='pre')
            if df is None or df.empty:
                return pd.DataFrame()
            df = df.reset_index()
            df.rename(columns={'index': 'trade_date', 'volume': 'vol', 'money': 'amount'}, inplace=True)
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df['year'] = df['trade_date'].dt.year
            df['month'] = df['trade_date'].dt.month
            df['trade_date'] = df['trade_date'].dt.strftime('%Y%m%d')
            df['ts_code'] = ts_code
            # 过滤回请求范围
            df = df[df['trade_date'] >= start_date]
            df = self.calculate_pct_chg(df)
            return df[['ts_code', 'trade_date', 'year', 'month', 'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg']]
        except Exception as e:
            print(f"Error fetching monthly kline from jqdata ({ts_code}): {e}")
            return pd.DataFrame()

    def get_industry_classification(self, industry_type: str = 'sw') -> Dict[str, List[str]]:
        """获取行业分类"""
        if self.data_source == 'tushare':
            return self._get_industry_tushare(industry_type)
        else:
            return {}

    def get_industry_by_yfinance(self, ts_codes: List[str], market: str,
                                  progress_callback=None, stop_check=None) -> Dict[str, List[str]]:
        """通过yfinance逐只获取行业分类（港股/美股）

        Args:
            ts_codes: 股票代码列表
            market: 'HK' 或 'US'
            progress_callback: 进度回调 callback(current, total, message)
            stop_check: 停止检查回调，返回 True 表示需要停止

        Returns:
            {industry_name: [ts_code, ...]}
        """
        if not YFINANCE_AVAILABLE:
            print("yfinance未安装，无法获取行业分类")
            return {}

        market_name = {'HK': '港股', 'US': '美股'}.get(market, market)
        industry_dict: Dict[str, List[str]] = {}
        total = len(ts_codes)

        for i, ts_code in enumerate(ts_codes):
            if stop_check and stop_check():
                break
            try:
                if market == 'HK':
                    yf_code = self._to_yfinance_hk_code(ts_code)
                else:
                    yf_code = ts_code

                ticker = yf.Ticker(yf_code)
                info = ticker.info
                # 优先用 sector（大类），fallback 用 industry（细分）
                sector = info.get('sector') or info.get('industry')
                if sector:
                    if sector not in industry_dict:
                        industry_dict[sector] = []
                    industry_dict[sector].append(ts_code)
            except Exception as e:
                print(f"获取 {ts_code} 行业信息失败: {e}")

            if progress_callback and (i + 1) % 20 == 0:
                progress_callback(i + 1, total,
                                  f"yfinance行业分类: {i+1}/{total} 只{market_name}")

        return industry_dict
    
    def _get_industry_tushare(self, industry_type: str = 'sw') -> Dict[str, List[str]]:
        """从tushare获取行业分类"""
        try:
            # 使用stock_basic获取行业信息
            stocks_df = self.pro.stock_basic(exchange='', list_status='L', 
                                            fields='ts_code,industry')
            
            industry_dict = {}
            for idx, row in stocks_df.iterrows():
                if pd.notna(row['industry']) and row['industry']:
                    industry_name = row['industry']
                    if industry_name not in industry_dict:
                        industry_dict[industry_name] = []
                    industry_dict[industry_name].append(row['ts_code'])
            
            return industry_dict
        except Exception as e:
            print(f"Error fetching industry classification: {e}")
            # 如果失败，尝试使用index_classify
            try:
                if industry_type == 'sw':
                    df = self.pro.index_classify(level='L1', src='SW2021')
                elif industry_type == 'citics':
                    df = self.pro.index_classify(level='L1', src='CSI')
                else:
                    return {}
                
                industry_dict = {}
                for idx_code in df['index_code'].unique():
                    idx_info = df[df['index_code'] == idx_code].iloc[0]
                    industry_name = idx_info['industry_name']
                    cons_df = self.pro.index_weight(index_code=idx_code)
                    if not cons_df.empty:
                        industry_dict[industry_name] = cons_df['con_code'].tolist()
                return industry_dict
            except Exception as e2:
                print(f"Error with index_classify: {e2}")
                return {}
    
    def calculate_pct_chg(self, df: pd.DataFrame, prev_close: float = None) -> pd.DataFrame:
        """计算涨跌幅（如果数据源没有提供）

        Args:
            df: K线数据
            prev_close: 前一期收盘价（用于增量更新时修复首行NaN）
        """
        if 'pct_chg' in df.columns and not df['pct_chg'].isna().all():
            # 检查pct_chg的格式：如果最大值小于1，可能是小数形式，需要转换为百分比
            valid_pct = df[df['pct_chg'].notna()]['pct_chg']
            if len(valid_pct) > 0:
                max_abs = valid_pct.abs().max()
                # 如果绝对值最大值小于2，可能是小数形式（如0.058），需要乘以100
                # 阈值2：月涨跌幅超过2%极为常见，小数形式不可能 >2；低于1的阈值会误判低波动期
                if max_abs < 2:
                    df['pct_chg'] = df['pct_chg'] * 100
            # 用prev_close修复首行NaN
            if prev_close and prev_close > 0:
                first_idx = df.index[0]
                if pd.isna(df.at[first_idx, 'pct_chg']):
                    df.at[first_idx, 'pct_chg'] = (df.at[first_idx, 'close'] - prev_close) / prev_close * 100
            return df

        df = df.sort_values('trade_date')
        df['pct_chg'] = df['close'].pct_change() * 100
        # 用prev_close修复首行NaN
        if prev_close and prev_close > 0:
            first_idx = df.index[0]
            if pd.isna(df.at[first_idx, 'pct_chg']):
                df.at[first_idx, 'pct_chg'] = (df.at[first_idx, 'close'] - prev_close) / prev_close * 100
        return df
    
    def get_monthly_kline_batch(self, ts_codes: list, start_date: str, end_date: str) -> dict:
        """批量获取月K线数据（港股/美股专用，使用yfinance批量接口）

        Args:
            ts_codes: 股票代码列表，如 ['00700.HK', '09988.HK', ...]
            start_date: 起始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        Returns:
            dict: {ts_code: DataFrame}，失败的股票对应空DataFrame
        """
        if not YFINANCE_AVAILABLE:
            raise ImportError("yfinance未安装，请使用: pip install yfinance")

        if not ts_codes:
            return {}

        # 将所有代码按市场分组
        hk_codes = []
        us_codes = []
        for code in ts_codes:
            market = self.detect_market(code)
            if market == 'HK':
                hk_codes.append(code)
            elif market == 'US':
                us_codes.append(code)

        results = {}

        # 批量下载港股
        if hk_codes:
            batch_results = self._batch_download_yfinance(hk_codes, 'HK', start_date, end_date)
            results.update(batch_results)

        # 批量下载美股
        if us_codes:
            batch_results = self._batch_download_yfinance(us_codes, 'US', start_date, end_date)
            results.update(batch_results)

        return results

    def _batch_download_yfinance(self, ts_codes: list, market: str, start_date: str, end_date: str,
                                  batch_size: int = 50) -> dict:
        """使用yfinance批量下载，自动分批处理

        Args:
            ts_codes: 股票代码列表
            market: 'HK' 或 'US'
            start_date/end_date: YYYYMMDD格式
            batch_size: 每批数量（默认50，yfinance建议不超过100）
        Returns:
            dict: {原始ts_code: DataFrame}
        """
        results = {}
        start_dt = pd.to_datetime(start_date, format='%Y%m%d')
        end_dt = pd.to_datetime(end_date, format='%Y%m%d')

        # 构建 yfinance code -> 原始code 的映射
        code_map = {}  # yf_code -> original_ts_code
        yf_codes = []
        for code in ts_codes:
            if market == 'HK':
                yf_code = self._to_yfinance_hk_code(code)
            else:
                yf_code = self.normalize_code(code, market)
            code_map[yf_code] = code
            yf_codes.append(yf_code)

        # 分批下载
        for batch_start in range(0, len(yf_codes), batch_size):
            batch = yf_codes[batch_start: batch_start + batch_size]

            for attempt in range(3):
                try:
                    if attempt > 0:
                        time.sleep(5 * attempt)

                    if len(batch) == 1:
                        # 单只股票直接用 Ticker（避免download的格式差异）
                        ticker = yf.Ticker(batch[0])
                        raw = ticker.history(start=start_dt, end=end_dt, interval="1mo")
                        if not raw.empty:
                            raw = raw.reset_index()
                            raw.columns = [c if c != 'Datetime' else 'Date' for c in raw.columns]
                            data_map = {batch[0]: raw}
                        else:
                            data_map = {batch[0]: pd.DataFrame()}
                    else:
                        raw = yf.download(
                            tickers=batch,
                            start=start_dt,
                            end=end_dt,
                            interval="1mo",
                            group_by='ticker',
                            auto_adjust=True,
                            progress=False,
                            threads=True,
                        )
                        data_map = self._split_batch_download(raw, batch)

                    # 转换每只股票的数据格式
                    for yf_code, hist in data_map.items():
                        original_code = code_map.get(yf_code, yf_code)
                        if hist is None or (hasattr(hist, 'empty') and hist.empty):
                            results[original_code] = pd.DataFrame()
                            continue
                        df = self._convert_yfinance_to_kline(hist, original_code)
                        results[original_code] = df

                    break  # 成功，退出重试

                except Exception as e:
                    error_str = str(e)
                    is_rate_limit = ('rate limit' in error_str.lower() or
                                     'too many requests' in error_str.lower() or
                                     'YFRateLimitError' in error_str)
                    print(f"Batch download attempt {attempt+1} failed: {error_str[:100]}")

                    if attempt == 2 or (is_rate_limit and attempt >= 1):
                        # 最终失败：逐只fallback
                        print(f"Falling back to individual download for {len(batch)} stocks")
                        for yf_code in batch:
                            original_code = code_map.get(yf_code, yf_code)
                            try:
                                ticker = yf.Ticker(yf_code)
                                hist = ticker.history(start=start_dt, end=end_dt, interval="1mo")
                                results[original_code] = self._convert_yfinance_to_kline(
                                    hist.reset_index() if not hist.empty else hist, original_code
                                )
                                time.sleep(1)
                            except Exception:
                                results[original_code] = pd.DataFrame()
                        break

            # 批次间延迟（避免rate limit）
            if batch_start + batch_size < len(yf_codes):
                time.sleep(2)

        return results

    def _split_batch_download(self, raw: pd.DataFrame, tickers: list) -> dict:
        """将yfinance批量下载结果按ticker拆分"""
        data_map = {}

        if raw.empty:
            return {t: pd.DataFrame() for t in tickers}

        if isinstance(raw.columns, pd.MultiIndex):
            # yfinance 不同版本 MultiIndex 层级顺序不同：
            # 旧版 group_by='ticker' → (ticker, field)，level=0 是 ticker
            # 新版 (≥0.2.x) 部分情况 → (field, ticker)，level=1 是 ticker
            # 自动探测：优先在 level=0 找，找不到再试 level=1
            level0_vals = set(raw.columns.get_level_values(0))
            ticker_level = 0 if any(t in level0_vals for t in tickers) else 1
            for ticker in tickers:
                try:
                    if ticker in raw.columns.get_level_values(ticker_level):
                        df = raw.xs(ticker, axis=1, level=ticker_level).copy()
                        df = df.reset_index()
                        df.columns = [c if c != 'Datetime' else 'Date' for c in df.columns]
                        data_map[ticker] = df
                    else:
                        data_map[ticker] = pd.DataFrame()
                except Exception:
                    data_map[ticker] = pd.DataFrame()
        else:
            # 单ticker返回普通列
            if len(tickers) == 1:
                raw = raw.reset_index()
                raw.columns = [c if c != 'Datetime' else 'Date' for c in raw.columns]
                data_map[tickers[0]] = raw

        return data_map

    def _convert_yfinance_to_kline(self, hist: pd.DataFrame, ts_code: str) -> pd.DataFrame:
        """将yfinance原始数据转换为标准月K线格式"""
        if hist is None or hist.empty:
            return pd.DataFrame()

        try:
            hist = hist.copy()
            # 处理Date列
            if 'Date' not in hist.columns and 'Datetime' in hist.columns:
                hist = hist.rename(columns={'Datetime': 'Date'})

            # 确保Date列是datetime类型
            if not pd.api.types.is_datetime64_any_dtype(hist['Date']):
                hist['Date'] = pd.to_datetime(hist['Date'])
            # 去除时区信息
            if hasattr(hist['Date'].dt, 'tz') and hist['Date'].dt.tz is not None:
                hist['Date'] = hist['Date'].dt.tz_localize(None)

            hist['ts_code'] = ts_code
            hist['trade_date'] = hist['Date'].dt.strftime('%Y%m%d')
            hist['year'] = hist['Date'].dt.year
            hist['month'] = hist['Date'].dt.month
            hist['open'] = hist['Open']
            hist['close'] = hist['Close']
            hist['high'] = hist['High']
            hist['low'] = hist['Low']
            hist['vol'] = hist['Volume'].fillna(0)
            hist['amount'] = (hist['Volume'] * hist['Close']).fillna(0)

            hist = hist.sort_values('trade_date')
            hist['pct_chg'] = hist['close'].pct_change() * 100

            return hist[['ts_code', 'trade_date', 'year', 'month',
                         'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg']]
        except Exception as e:
            print(f"Error converting yfinance data for {ts_code}: {e}")
            return pd.DataFrame()

    def _get_monthly_kline_yfinance_hk(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从yfinance获取港股月K线数据"""
        if not YFINANCE_AVAILABLE:
            raise ImportError("yfinance未安装，请使用: pip install yfinance")
        
        try:
            # yfinance使用4位格式：0700.HK
            code = self._to_yfinance_hk_code(ts_code)
            
            # 转换日期格式
            start_dt = pd.to_datetime(start_date, format='%Y%m%d')
            end_dt = pd.to_datetime(end_date, format='%Y%m%d')
            
            # 获取数据（带重试）
            ticker = yf.Ticker(code)
            
            max_retries = 3  # 减少重试次数，避免长时间等待
            hist = None
            last_error = None
            
            for i in range(max_retries):
                try:
                    # 每次请求前延迟，避免rate limit
                    if i > 0:
                        delay = min(3 * (i + 1), 15)  # 线性退避，最多15秒
                        time.sleep(delay)
                    
                    hist = ticker.history(start=start_dt, end=end_dt, interval="1mo")
                    if not hist.empty:
                        break
                    time.sleep(2)  # 延迟避免速率限制
                except Exception as e:
                    last_error = e
                    error_str = str(e)
                    # 如果是rate limit错误，直接抛出，不重试（避免长时间等待）
                    if 'rate limit' in error_str.lower() or 'too many requests' in error_str.lower() or 'YFRateLimitError' in error_str:
                        # rate limit时，只重试一次，然后放弃
                        if i < 1:  # 只重试一次
                            delay = 10  # 等待10秒
                            print(f"Rate limit detected for {ts_code}, waiting {delay} seconds before retry {i+1}/{max_retries}...")
                            time.sleep(delay)
                            continue
                        else:
                            # 重试后仍然失败，直接抛出
                            raise
                    
                    if i < max_retries - 1:
                        delay = min(2 ** i, 10)  # 指数退避，最多10秒
                        time.sleep(delay)
                    else:
                        raise
            
            # 如果所有重试都失败，抛出最后一个错误
            if hist is None and last_error:
                raise last_error
            
            if hist is None or hist.empty:
                return pd.DataFrame()

            return self._convert_yfinance_to_kline(hist.reset_index(), ts_code)

        except Exception as e:
            print(f"Error fetching yfinance HK data for {ts_code}: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def _get_monthly_kline_yfinance_us(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从yfinance获取美股月K线数据"""
        if not YFINANCE_AVAILABLE:
            raise ImportError("yfinance未安装，请使用: pip install yfinance")

        try:
            code = self.normalize_code(ts_code, 'US')
            start_dt = pd.to_datetime(start_date, format='%Y%m%d')
            end_dt = pd.to_datetime(end_date, format='%Y%m%d')

            ticker = yf.Ticker(code)
            max_retries = 3
            hist = None
            for i in range(max_retries):
                try:
                    hist = ticker.history(start=start_dt, end=end_dt, interval="1mo")
                    if not hist.empty:
                        break
                    time.sleep(0.5)
                except Exception as e:
                    if i < max_retries - 1:
                        time.sleep(2 ** i)
                    else:
                        raise

            if hist is None or hist.empty:
                return pd.DataFrame()

            return self._convert_yfinance_to_kline(hist.reset_index(), code)

        except Exception as e:
            print(f"Error fetching yfinance US data for {ts_code}: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def _get_monthly_kline_tushare_hk(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从tushare获取港股月K线数据"""
        try:
            # 标准化代码格式
            code = self.normalize_code(ts_code, 'HK')
            
            # 确保有tushare pro对象
            if not hasattr(self, 'pro') or self.data_source != 'tushare':
                token = self.config.get('tushare.token', '')
                if not token:
                    return pd.DataFrame()
                import tushare as ts
                ts.set_token(token)
                pro = ts.pro_api()
            else:
                pro = self.pro
            
            # 获取日K线数据
            df = pro.hk_daily(ts_code=code, start_date=start_date, end_date=end_date)
            
            if df.empty:
                return pd.DataFrame()
            
            # 转换为月K线
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df['year'] = df['trade_date'].dt.year
            df['month'] = df['trade_date'].dt.month
            
            # 按月聚合
            monthly = df.groupby(['year', 'month']).agg({
                'open': 'first',
                'close': 'last',
                'high': 'max',
                'low': 'min',
                'vol': 'sum',
                'amount': 'sum'
            }).reset_index()
            
            # 添加日期字段（每月最后一天）
            monthly['trade_date'] = monthly.apply(
                lambda x: df[(df['year'] == x['year']) & (df['month'] == x['month'])]['trade_date'].max(),
                axis=1
            )
            monthly['trade_date'] = pd.to_datetime(monthly['trade_date']).dt.strftime('%Y%m%d')
            monthly['ts_code'] = code
            
            # 计算涨跌幅
            monthly = monthly.sort_values('trade_date')
            monthly['pct_chg'] = monthly['close'].pct_change() * 100
            
            return monthly[['ts_code', 'trade_date', 'year', 'month', 
                           'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg']]
        
        except Exception as e:
            print(f"Error fetching tushare HK data for {ts_code}: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def _get_monthly_kline_akshare_hk(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从akshare获取港股月K线数据（优先东财，失败自动切新浪）"""
        if not AKSHARE_AVAILABLE:
            raise ImportError("akshare未安装，请使用: pip install akshare")
        symbol = ts_code.replace('.HK', '')

        # 1. 优先尝试东财接口（stock_hk_hist）
        try:
            df = ak.stock_hk_hist(
                symbol=symbol, period='monthly',
                start_date=start_date, end_date=end_date, adjust='qfq'
            )
            if df is not None and not df.empty:
                df = df.rename(columns={
                    '日期': '_date', '开盘': 'open', '收盘': 'close',
                    '最高': 'high', '最低': 'low', '成交量': 'vol',
                    '成交额': 'amount', '涨跌幅': 'pct_chg',
                })
                df['trade_date'] = pd.to_datetime(df['_date']).dt.strftime('%Y%m%d')
                df['year']  = pd.to_datetime(df['_date']).dt.year
                df['month'] = pd.to_datetime(df['_date']).dt.month
                df['ts_code'] = ts_code
                df = df.sort_values('trade_date')
                return df[['ts_code', 'trade_date', 'year', 'month',
                           'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg']]
        except Exception as e:
            print(f"akshare东财HK接口失败({ts_code}): {e}，切换新浪接口...")

        # 2. 东财失败，切新浪日线聚合月线（stock_hk_daily）
        try:
            df = ak.stock_hk_daily(symbol=symbol, adjust='qfq')
            if df is None or df.empty:
                return pd.DataFrame()
            df['_date'] = pd.to_datetime(df['date'])
            s_dt = pd.to_datetime(start_date, format='%Y%m%d')
            e_dt = pd.to_datetime(end_date, format='%Y%m%d')
            df = df[(df['_date'] >= s_dt) & (df['_date'] <= e_dt)].copy()
            if df.empty:
                return pd.DataFrame()
            df['year']  = df['_date'].dt.year
            df['month'] = df['_date'].dt.month
            monthly = df.groupby(['year', 'month']).agg(
                open=('open', 'first'), close=('close', 'last'),
                high=('high', 'max'), low=('low', 'min'),
                vol=('volume', 'sum'), amount=('amount', 'sum'),
                trade_date=('_date', 'max'),
            ).reset_index()
            monthly['trade_date'] = monthly['trade_date'].dt.strftime('%Y%m%d')
            monthly['ts_code'] = ts_code
            monthly['pct_chg'] = monthly['close'].pct_change() * 100
            monthly = monthly.sort_values('trade_date')
            return monthly[['ts_code', 'trade_date', 'year', 'month',
                            'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg']]
        except Exception as e:
            print(f"Error fetching akshare HK data for {ts_code}: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def _get_monthly_kline_tushare_us(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从tushare获取美股月K线数据"""
        try:
            # 标准化代码格式（tushare美股代码不带后缀）
            code = self.normalize_code(ts_code, 'US')
            
            # 确保有tushare pro对象
            if not hasattr(self, 'pro') or self.data_source != 'tushare':
                token = self.config.get('tushare.token', '')
                if not token:
                    return pd.DataFrame()
                import tushare as ts
                ts.set_token(token)
                pro = ts.pro_api()
            else:
                pro = self.pro
            
            # 获取日K线数据
            df = pro.us_daily(ts_code=code, start_date=start_date, end_date=end_date)
            
            if df.empty:
                return pd.DataFrame()
            
            # 转换为月K线（与港股相同逻辑）
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df['year'] = df['trade_date'].dt.year
            df['month'] = df['trade_date'].dt.month
            
            # 按月聚合
            monthly = df.groupby(['year', 'month']).agg({
                'open': 'first',
                'close': 'last',
                'high': 'max',
                'low': 'min',
                'vol': 'sum',
                'amount': 'sum'
            }).reset_index()
            
            # 添加日期字段
            monthly['trade_date'] = monthly.apply(
                lambda x: df[(df['year'] == x['year']) & (df['month'] == x['month'])]['trade_date'].max(),
                axis=1
            )
            monthly['trade_date'] = pd.to_datetime(monthly['trade_date']).dt.strftime('%Y%m%d')
            monthly['ts_code'] = code
            
            # 计算涨跌幅
            monthly = monthly.sort_values('trade_date')
            monthly['pct_chg'] = monthly['close'].pct_change() * 100
            
            return monthly[['ts_code', 'trade_date', 'year', 'month', 
                           'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg']]
        
        except Exception as e:
            print(f"Error fetching tushare US data for {ts_code}: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def _get_monthly_kline_akshare_us(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从akshare获取美股月K线数据（stock_us_daily，日线聚合月线）"""
        if not AKSHARE_AVAILABLE:
            raise ImportError("akshare未安装，请使用: pip install akshare")
        # 美股代码格式：去掉交易所后缀，只保留 symbol（如 AAPL.O → AAPL）
        symbol = ts_code.split('.')[0] if '.' in ts_code else ts_code
        try:
            df = ak.stock_us_daily(symbol=symbol, adjust='qfq')
            if df is None or df.empty:
                return pd.DataFrame()
            df['_date'] = pd.to_datetime(df['date'])
            s_dt = pd.to_datetime(start_date, format='%Y%m%d')
            e_dt = pd.to_datetime(end_date, format='%Y%m%d')
            df = df[(df['_date'] >= s_dt) & (df['_date'] <= e_dt)].copy()
            if df.empty:
                return pd.DataFrame()
            df['year']  = df['_date'].dt.year
            df['month'] = df['_date'].dt.month
            monthly = df.groupby(['year', 'month']).agg(
                open=('open', 'first'), close=('close', 'last'),
                high=('high', 'max'), low=('low', 'min'),
                vol=('volume', 'sum'),
                trade_date=('_date', 'max'),
            ).reset_index()
            monthly['trade_date'] = monthly['trade_date'].dt.strftime('%Y%m%d')
            monthly['ts_code'] = ts_code
            monthly['amount'] = 0.0
            monthly['pct_chg'] = monthly['close'].pct_change() * 100
            monthly = monthly.sort_values('trade_date')
            return monthly[['ts_code', 'trade_date', 'year', 'month',
                            'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg']]
        except Exception as e:
            print(f"Error fetching akshare US data for {ts_code}: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def _get_monthly_kline_alpha_vantage_us(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从Alpha Vantage获取美股月K线数据（TIME_SERIES_MONTHLY_ADJUSTED，含复权收盘价）"""
        api_key = self.config.get('alpha_vantage.api_key', '')
        if not api_key:
            raise ValueError("Alpha Vantage API Key未配置，请在设置中填写")

        # 速率限制：默认5次/分钟（免费版），可通过 alpha_vantage.requests_per_minute 调整
        rpm = float(self.config.get('alpha_vantage.requests_per_minute', 5))
        min_interval = 60.0 / max(rpm, 1)
        elapsed = time.time() - DataFetcher._av_last_call
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        DataFetcher._av_last_call = time.time()

        # 代码格式：只保留 ticker 符号（AAPL, TSLA...），去掉 .US 等后缀
        symbol = ts_code.split('.')[0] if '.' in ts_code else ts_code

        try:
            url = (
                "https://www.alphavantage.co/query"
                f"?function=TIME_SERIES_MONTHLY_ADJUSTED"
                f"&symbol={symbol}"
                f"&apikey={api_key}"
            )
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if 'Monthly Adjusted Time Series' not in data:
                if 'Note' in data:
                    print(f"Alpha Vantage rate limit for {ts_code}: {data['Note'][:120]}")
                elif 'Information' in data:
                    print(f"Alpha Vantage info for {ts_code}: {data['Information'][:120]}")
                return pd.DataFrame()

            monthly_data = data['Monthly Adjusted Time Series']
            if not monthly_data:
                return pd.DataFrame()

            rows = []
            for date_str, v in monthly_data.items():
                trade_date = date_str.replace('-', '')
                # Alpha Vantage 返回全量历史，在本地过滤日期范围
                if trade_date < start_date or trade_date > end_date:
                    continue
                try:
                    rows.append({
                        'trade_date': trade_date,
                        'open':  float(v['1. open']),
                        'high':  float(v['2. high']),
                        'low':   float(v['3. low']),
                        'close': float(v['5. adjusted close']),  # 使用复权收盘价
                        'vol':   float(v['6. volume']),
                    })
                except (KeyError, ValueError):
                    continue

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows)
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df['year']  = df['trade_date'].dt.year
            df['month'] = df['trade_date'].dt.month
            df['trade_date'] = df['trade_date'].dt.strftime('%Y%m%d')
            df['ts_code'] = ts_code
            df['amount'] = df['vol'] * df['close']
            df = df.sort_values('trade_date')
            df['pct_chg'] = df['close'].pct_change() * 100

            return df[['ts_code', 'trade_date', 'year', 'month',
                       'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg']]

        except Exception as e:
            print(f"Error fetching Alpha Vantage US data for {ts_code}: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

