# -*- coding: utf-8 -*-
"""
LOF基金数据获取模块
获取场内价格和场外净值
"""

import requests
import json
import time
import os
from typing import Dict, Optional, List
from datetime import datetime, timedelta
from config import DATA_SOURCE

# 模块所在目录（用于构造绝对路径，避免工作目录影响）
_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

# 尝试导入pandas（用于读取Excel文件）
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("警告: pandas未安装，无法从SSE Excel文件读取数据")

# 导入缓存管理器（延迟导入，避免循环依赖）
_cache_manager = None

def get_cache_manager():
    """获取缓存管理器实例（延迟导入）"""
    global _cache_manager
    if _cache_manager is None:
        try:
            from cache_manager import cache_manager
            _cache_manager = cache_manager
        except ImportError:
            # 如果缓存模块不存在，返回None（向后兼容）
            pass
    return _cache_manager

# Tushare已移除，仅使用SSE数据源
TUSHARE_AVAILABLE = False

# 尝试导入akshare
try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False
    print("警告: akshare未安装，将使用其他数据源")

# 尝试导入baostock
try:
    import baostock as bs
    BAOSTOCK_AVAILABLE = True
except ImportError:
    BAOSTOCK_AVAILABLE = False
    print("警告: baostock未安装，将使用其他数据源")

class LOFDataFetcher:
    """LOF基金数据获取器 - 仅使用SSE数据源"""
    
    def __init__(self, tushare_token: str = None):
        # tushare_token参数保留以保持向后兼容，但不再使用
        self.session = requests.Session()
        # 禁用代理，避免代理错误
        self.session.proxies = {
            'http': None,
            'https': None
        }
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'http://fund.eastmoney.com/',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
        })
        
        # 保存数据源配置（从全局配置导入，支持运行时更新）
        self.data_source_config = DATA_SOURCE
        
        # Tushare已移除，不再初始化
        self.tushare_pro = None
        
        
        print("数据获取器已初始化（仅使用SSE数据源）")
        
        # baostock在需要时再登录
        self.baostock_logged_in = False
        
        # 缓存akshare基金申购信息（避免重复下载所有基金数据）
        self._akshare_purchase_cache = None
        self._akshare_purchase_cache_time = None
    
    def _is_source_enabled(self, source_type: str, source_name: str) -> bool:
        """检查数据源是否启用"""
        sources = self.data_source_config.get(source_type, {})
        source_config = sources.get(source_name, {})
        return source_config.get('enabled', True)  # 默认启用（向后兼容）
    
    def _get_enabled_sources(self, source_type: str) -> List[tuple]:
        """获取启用的数据源列表（按优先级排序）"""
        sources = self.data_source_config.get(source_type, {})
        enabled = [(name, config) for name, config in sources.items() if config.get('enabled', True)]
        enabled.sort(key=lambda x: x[1].get('priority', 999))
        return enabled
    
    def _classify_fund_type(self, name: str, fund_type: str = '') -> str:
        """
        分类基金类型：指数型或股票型
        
        Args:
            name: 基金名称
            fund_type: 基金类型字段
            
        Returns:
            'index' 或 'stock'
        """
        name_lower = name.lower()
        type_lower = fund_type.lower() if fund_type else ''
        
        # 指数型LOF的特征
        index_keywords = [
            '指数', 'index', 'etf', '中证', '国证', '上证', '深证',
            '沪深', '创业板', '中小板', '行业', '主题', '分级'
        ]
        
        # 股票型LOF的特征（注意：'主题' 已移到 index_keywords，此处不再重复）
        stock_keywords = [
            '混合', '股票', '成长', '价值', '精选', '优选', '灵活',
            '配置', '策略', '行业精选'
        ]
        
        # 优先判断指数型（特征更明显）
        for keyword in index_keywords:
            if keyword in name_lower or keyword in type_lower:
                return 'index'
        
        # 判断股票型
        for keyword in stock_keywords:
            if keyword in name_lower or keyword in type_lower:
                return 'stock'
        
        # 默认：如果名称包含"指数"相关词汇，归为指数型，否则归为股票型
        if any(kw in name_lower for kw in ['指数', 'index', 'etf']):
            return 'index'
        else:
            return 'stock'
    
    def get_lof_funds_list_sse(self) -> List[Dict]:
        """
        从上海证券交易所网站获取LOF基金列表
        优先从本地Excel文件读取（如果存在），否则尝试在线获取
        
        Returns:
            基金列表，包含代码和名称
        """
        
        # 方式1: 优先从本地Excel文件读取（如果存在）
        if PANDAS_AVAILABLE:
            excel_file = os.path.join(_MODULE_DIR, 'data', 'LOF基金列表.xlsx')
            if os.path.exists(excel_file):
                try:
                    # 读取Excel文件
                    df = pd.read_excel(excel_file)
                    
                    # 查找基金代码和名称列
                    # 根据分析，列名可能是：证券代码、证券简称
                    code_col = None
                    name_col = None
                    
                    for col in df.columns:
                        col_str = str(col)
                        if any(kw in col_str for kw in ['证券代码', '代码', 'code', 'fund_code']):
                            code_col = col
                        if any(kw in col_str for kw in ['证券简称', '名称', 'name', 'fund_name', '简称']):
                            name_col = col
                    
                    if code_col and name_col:
                        lof_funds = []
                        for _, row in df.iterrows():
                            code = str(row[code_col]).strip()
                            name = str(row[name_col]).strip()
                            
                            # 确保代码是6位数字
                            if code and code.isdigit():
                                if len(code) == 6:
                                    lof_funds.append({
                                        'code': code,
                                        'name': name
                                    })
                                elif len(code) > 6:
                                    # 如果代码超过6位，取前6位
                                    code = code[:6]
                                    lof_funds.append({
                                        'code': code,
                                        'name': name
                                    })
                        
                        if lof_funds:
                            print(f"从SSE Excel文件读取到 {len(lof_funds)} 只LOF基金")
                            return lof_funds
                except Exception as e:
                    print(f"读取SSE Excel文件失败: {e}")
        
        # 方式2: 尝试直接访问可能的API接口
        sse_base_url = 'https://fund.sse.org.cn'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://fund.sse.org.cn/marketdata/lof/index.html'
        }
        
        # 可能的API端点
        api_endpoints = [
            '/marketdata/lof/list',
            '/api/marketdata/lof/list',
            '/queryFundLOFList.do',
        ]
        
        for endpoint in api_endpoints:
            try:
                url = sse_base_url + endpoint
                response = self.session.get(url, headers=headers, timeout=10, params={'catalogId': 'fund_lof'})
                
                if response.status_code == 200:
                    content_type = response.headers.get('Content-Type', '')
                    # 尝试解析JSON
                    if 'json' in content_type:
                        try:
                            data = response.json()
                            # 解析数据格式（需要根据实际返回格式调整）
                            if isinstance(data, dict) and 'data' in data:
                                funds_data = data['data']
                            elif isinstance(data, list):
                                funds_data = data
                            else:
                                continue
                            
                            lof_funds = []
                            for item in funds_data:
                                if isinstance(item, dict):
                                    code = str(item.get('fundCode', item.get('code', ''))).strip()
                                    name = str(item.get('fundName', item.get('name', ''))).strip()
                                    if code and len(code) == 6 and code.isdigit():
                                        lof_funds.append({
                                            'code': code,
                                            'name': name
                                        })
                            
                            if lof_funds:
                                print(f"从SSE API获取到 {len(lof_funds)} 只LOF基金")
                                return lof_funds
                        except json.JSONDecodeError:
                            pass
                    # 如果不是JSON，尝试从HTML中提取
                    elif 'html' in content_type or 'text' in content_type:
                        import re
                        # 从HTML响应中提取1开头的6位数字代码
                        pattern = r'\b(1\d{5})\b'
                        matches = re.findall(pattern, response.text)
                        unique_codes = list(set(matches))
                        if unique_codes:
                            lof_funds = [{'code': code, 'name': f'LOF基金{code}'} for code in unique_codes[:200]]
                            print(f"从SSE API HTML响应中提取到 {len(lof_funds)} 只LOF基金")
                            return lof_funds
            except Exception as e:
                continue
        
        # 方式3: 尝试直接请求页面并解析HTML（不使用Selenium）
        try:
            response = self.session.get('https://fund.sse.org.cn/marketdata/lof/index.html', headers=headers, timeout=15)
            if response.status_code == 200:
                import re
                # 从HTML中提取1开头的6位数字代码
                pattern = r'\b(1\d{5})\b'
                matches = re.findall(pattern, response.text)
                unique_codes = list(set(matches))
                if unique_codes and len(unique_codes) > 10:  # 至少找到10个代码才认为有效
                    lof_funds = []
                    # 尝试找到代码和名称的对应关系
                    for code in unique_codes[:200]:  # 限制数量
                        # 在代码附近查找基金名称
                        code_index = response.text.find(code)
                        if code_index != -1:
                            context = response.text[max(0, code_index-100):code_index+200]
                            name_match = re.search(r'([^<>]{5,30}LOF[^<>]{0,20})', context)
                            if name_match:
                                name = name_match.group(1).strip()
                                name = re.sub(r'<[^>]+>', '', name)
                                name = re.sub(r'&[^;]+;', '', name)
                                if len(name) > 2:
                                    lof_funds.append({'code': code, 'name': name})
                                else:
                                    lof_funds.append({'code': code, 'name': f'LOF基金{code}'})
                            else:
                                lof_funds.append({'code': code, 'name': f'LOF基金{code}'})
                    
                    if lof_funds:
                        print(f"从SSE页面HTML中提取到 {len(lof_funds)} 只LOF基金")
                        return lof_funds
        except Exception as e:
            print(f"从SSE页面HTML提取失败: {e}")
        
        # 方式2: 尝试使用Selenium（如果可用）
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            
            driver = webdriver.Chrome(options=chrome_options)
            try:
                driver.get('https://fund.sse.org.cn/marketdata/lof/index.html')
                # 等待页面加载（尝试多种可能的元素）
                try:
                    WebDriverWait(driver, 15).until(
                        EC.any_of(
                            EC.presence_of_element_located((By.CLASS_NAME, 'report-container')),
                            EC.presence_of_element_located((By.TAG_NAME, 'table')),
                            EC.presence_of_element_located((By.CSS_SELECTOR, '[class*="table"]'))
                        )
                    )
                except:
                    pass  # 如果等待超时，继续尝试
                time.sleep(5)  # 等待数据加载（增加等待时间）
                
                # 尝试从页面中提取基金数据
                page_source = driver.page_source
                import re
                
                lof_funds = []
                seen_codes = set()
                
                # 方法1：尝试从表格中提取数据（更准确）
                table_rows = driver.find_elements(By.CSS_SELECTOR, 'table tr')
                print(f"从表格中提取: 找到 {len(table_rows)} 个表格行")
                
                for row in table_rows:
                    try:
                        cells = row.find_elements(By.TAG_NAME, 'td')
                        if len(cells) >= 2:
                            # 第一列通常是代码，第二列是名称
                            code_text = cells[0].text.strip()
                            name_text = cells[1].text.strip() if len(cells) > 1 else ''
                            
                            # 提取6位数字代码
                            code_match = re.search(r'(\d{6})', code_text)
                            if code_match:
                                code = code_match.group(1)
                                if code.isdigit() and len(code) == 6 and code not in seen_codes:
                                    # 优先提取1开头的基金（SSE标准基金）
                                    if code.startswith('1') or 'LOF' in name_text or len(name_text) > 2:
                                        lof_funds.append({
                                            'code': code,
                                            'name': name_text if name_text else code
                                        })
                                        seen_codes.add(code)
                    except:
                        continue
                
                # 方法2：如果表格提取失败或提取的基金太少，使用正则表达式从页面源码中提取
                if len([f for f in lof_funds if f['code'].startswith('1')]) < 10:
                    print("表格提取的1开头基金太少，尝试从页面源码中提取...")
                    # 查找所有1开头的6位数字代码（SSE标准LOF基金代码）
                    pattern_1 = r'\b(1\d{5})\b'
                    matches_1 = re.findall(pattern_1, page_source)
                    unique_codes_1 = list(set(matches_1))
                    
                    # 尝试找到代码和名称的对应关系
                    for code in unique_codes_1:
                        if code not in seen_codes:
                            # 在代码附近查找基金名称（查找包含LOF的文本）
                            code_index = page_source.find(code)
                            if code_index != -1:
                                # 在代码前后200个字符内查找名称
                                context = page_source[max(0, code_index-100):code_index+200]
                                # 查找包含LOF的文本作为名称
                                name_match = re.search(r'([^<>]{5,30}LOF[^<>]{0,20})', context)
                                if name_match:
                                    name = name_match.group(1).strip()
                                    # 清理HTML标签
                                    name = re.sub(r'<[^>]+>', '', name)
                                    name = re.sub(r'&[^;]+;', '', name)
                                    if len(name) > 2:
                                        lof_funds.append({
                                            'code': code,
                                            'name': name
                                        })
                                        seen_codes.add(code)
                    
                    # 如果还是太少，尝试更宽松的模式
                    if len([f for f in lof_funds if f['code'].startswith('1')]) < 10:
                        print("尝试更宽松的提取模式...")
                        # 查找所有1开头的6位数字，即使没有找到名称也添加
                        for code in unique_codes_1[:200]:  # 限制数量，避免过多
                            if code not in seen_codes:
                                lof_funds.append({
                                    'code': code,
                                    'name': f'LOF基金{code}'  # 临时名称
                                })
                                seen_codes.add(code)
                
                print(f"Selenium提取结果: 找到 {len(lof_funds)} 只基金，其中1开头: {len([f for f in lof_funds if f['code'].startswith('1')])} 只")
                
                if lof_funds:
                    return lof_funds
                    
            finally:
                driver.quit()
        except ImportError:
            # Selenium未安装，跳过
            pass
        except Exception as e:
            pass
        
        
        return []
    
    def get_fund_chinese_name(self, fund_code: str) -> Optional[str]:
        """
        获取基金的中文名称
        
        Args:
            fund_code: 基金代码
            
        Returns:
            基金中文名称，如果失败返回None
        """
        
        # 仅从SSE Excel文件读取
        if PANDAS_AVAILABLE:
            # 尝试从基金列表Excel文件读取
            fund_list_file = os.path.join(_MODULE_DIR, 'data', 'LOF基金列表.xlsx')
            if os.path.exists(fund_list_file):
                try:
                    df = pd.read_excel(fund_list_file)
                    
                    # 查找列名
                    code_col = None
                    name_col = None
                    
                    for col in df.columns:
                        col_str = str(col)
                        if any(kw in col_str for kw in ['证券代码', '代码', 'code', 'fund_code']):
                            code_col = col
                        if any(kw in col_str for kw in ['证券简称', '名称', 'name', 'fund_name', '简称']):
                            name_col = col
                    
                    if code_col and name_col:
                        fund_row = df[df[code_col].astype(str).str.strip() == str(fund_code).strip()]
                        if not fund_row.empty:
                            name = str(fund_row.iloc[0][name_col]).strip()
                            if name:
                                return name
                except Exception as e:
                    print(f"从SSE Excel读取基金名称失败 {fund_code}: {e}")
            
            # 如果基金列表中没有，尝试从净值列表读取
            nav_file = os.path.join(_MODULE_DIR, 'data', 'LOF最新净值列表.xlsx')
            if os.path.exists(nav_file):
                try:
                    df = pd.read_excel(nav_file)
                    
                    code_col = None
                    name_col = None
                    
                    for col in df.columns:
                        col_str = str(col)
                        if any(kw in col_str for kw in ['基金代码', '代码', 'code']):
                            code_col = col
                        if any(kw in col_str for kw in ['基金名称', '名称', 'name']):
                            name_col = col
                    
                    if code_col and name_col:
                        fund_row = df[df[code_col].astype(str).str.strip() == str(fund_code).strip()]
                        if not fund_row.empty:
                            name = str(fund_row.iloc[0][name_col]).strip()
                            if name:
                                return name
                except Exception as e:
                    pass
        
        # 如果SSE数据源失败，返回None（不再使用其他数据源）
        return None
        
        # 方法1：从基金JS文件获取（备用数据源）
        if self._is_source_enabled('name_sources', 'eastmoney_js'):
            try:
                url2 = f'http://fund.eastmoney.com/js/{fund_code}.js'
                response2 = self.session.get(url2, timeout=5)
                if response2.status_code == 200:
                    import re
                    content = response2.text
                    # 提取中文名称：DWJC:"基金名称"
                    name_match = re.search(r'DWJC:"([^"]+)"', content)
                    if name_match:
                        name = name_match.group(1)
                        if any('\u4e00' <= char <= '\u9fff' for char in name):
                            return name
            except Exception as e:
                pass
        
        return None
    
    def get_lof_funds_list(self) -> list:
        """
        获取LOF基金列表（SSE为主，akShare补充50开头的基金）
        
        Returns:
            基金列表，包含代码和名称
        """
        
        # 主数据源：SSE
        all_funds = []
        if self._is_source_enabled('fund_list_sources', 'sse'):
            sse_funds = self.get_lof_funds_list_sse()
            if sse_funds and len(sse_funds) > 0:
                all_funds.extend(sse_funds)
                print(f"从SSE获取到 {len(sse_funds)} 只LOF基金")
            else:
                print(f"警告: SSE基金列表为空或未获取到数据，将只使用akShare补充的50开头基金")
        
        # 补充数据源：akShare（补充50开头的LOF基金，SSE数据源缺少）
        if AKSHARE_AVAILABLE:
            try:
                # 获取所有基金列表
                df = ak.fund_name_em()
                if df is not None and not df.empty:
                    # 查找LOF基金（名称包含LOF）
                    lof_funds = df[df['基金简称'].str.contains('LOF', na=False)]
                    
                    # 只补充50开头的LOF基金（SSE数据源缺少）
                    if '基金代码' in lof_funds.columns:
                        codes_50 = lof_funds[lof_funds['基金代码'].astype(str).str.startswith('50')]
                        
                        # 创建基金代码集合，用于去重
                        existing_codes = {f['code'] for f in all_funds}
                        
                        # 添加50开头的LOF基金（去重）
                        added_count = 0
                        for _, row in codes_50.iterrows():
                            code = str(row['基金代码']).strip()
                            name = str(row['基金简称']).strip()
                            
                            # 确保代码是6位数字且不在已有列表中
                            if code and code.isdigit() and len(code) == 6:
                                if code not in existing_codes:
                                    all_funds.append({
                                        'code': code,
                                        'name': name
                                    })
                                    existing_codes.add(code)
                                    added_count += 1
                        
                        if added_count > 0:
                            print(f"从akShare补充 {added_count} 只50开头的LOF基金")
            except Exception as e:
                print(f"从akShare补充50开头LOF基金失败: {e}")
        
        # 返回合并后的基金列表
        if all_funds:
            return all_funds
        
        # 如果所有数据源都失败，返回空列表
        
        return []
    
    def get_fund_price(self, fund_code: str, market: str = 'auto') -> Optional[Dict]:
        """
        获取LOF基金场内实时价格
        补充数据源：仅使用东方财富套利API（SSE不提供价格数据）
        
        Args:
            fund_code: 基金代码（6位数字）
            market: 市场代码，'sz'=深圳(1), 'sh'=上海(0), 'auto'=自动判断（当前未使用）
            
        Returns:
            包含价格信息的字典，如果失败返回None
        """
        # SSE Excel文件通常不包含实时价格数据（价格是实时变动的）
        # 使用备用数据源获取价格数据
        prices = []
        
        # 判断市场代码（深圳=sz，上海=sh）
        if market == 'auto':
            # 自动判断：深圳基金代码通常以1开头，上海以5开头
            if fund_code.startswith('1'):
                market_code = 'sz'
            elif fund_code.startswith('5'):
                market_code = 'sh'
            else:
                # 默认尝试深圳
                market_code = 'sz'
        elif market == 'sz':
            market_code = 'sz'
        else:
            market_code = 'sh'
        
        # 方法1：新浪财经（优先级最高，稳定可靠）
        if self._is_source_enabled('price_sources', 'sina'):
            try:
                url1 = f'http://hq.sinajs.cn/list={market_code}{fund_code}'
                headers1 = {
                    'Referer': 'http://finance.sina.com.cn',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                response1 = self.session.get(url1, headers=headers1, timeout=5)
                if response1.status_code == 200:
                    content = response1.text
                    import re
                    match = re.search(r'="([^"]+)"', content)
                    if match:
                        parts = match.group(1).split(',')
                        if len(parts) >= 3:
                            price = float(parts[3])  # 当前价格
                            # 计算涨跌幅：(当前价格 - 昨日收盘价) / 昨日收盘价
                            prev_close = float(parts[2]) if len(parts) > 2 and parts[2] else price
                            change_pct = ((price - prev_close) / prev_close) if prev_close > 0 else 0
                            if price > 0:
                                prices.append({
                                    'code': fund_code,
                                    'price': price,
                                    'change_pct': change_pct,  # 已经是小数形式（如0.05表示5%）
                                    'volume': float(parts[6]) if len(parts) > 6 else 0,
                                    'amount': float(parts[9]) if len(parts) > 9 else 0,
                                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    'source': 'sina'
                                })
            except Exception as e:
                print(f"新浪财经获取失败 {fund_code}: {e}")
        
        # 方法2：腾讯财经（备用）
        if self._is_source_enabled('price_sources', 'tencent'):
            try:
                url2 = f'http://qt.gtimg.cn/q={market_code}{fund_code}'
                headers2 = {
                    'Referer': 'http://qq.com',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                response2 = self.session.get(url2, headers=headers2, timeout=5)
                if response2.status_code == 200:
                    content = response2.text
                    import re
                    match = re.search(r'="([^"]+)"', content)
                    if match:
                        parts = match.group(1).split('~')
                        if len(parts) >= 5:
                            price = float(parts[3])  # 当前价格
                            # 腾讯财经：parts[4]通常是昨日收盘价，计算涨跌幅
                            prev_close = float(parts[4]) if len(parts) > 4 and parts[4] else price
                            change_pct = ((price - prev_close) / prev_close) if prev_close > 0 else 0
                            if price > 0:
                                prices.append({
                                    'code': fund_code,
                                    'price': price,
                                    'change_pct': change_pct,  # 已经是小数形式（如0.05表示5%）
                                    'volume': float(parts[6]) if len(parts) > 6 else 0,
                                    'amount': float(parts[37]) if len(parts) > 37 else 0,
                                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    'source': 'tencent'
                                })
            except Exception as e:
                print(f"腾讯财经获取失败 {fund_code}: {e}")
        
        # 数据验证和选择
        if prices:
            # 移除异常价格（价格<=0或明显异常）
            valid_prices = [p for p in prices if p['price'] > 0 and 0.01 < p['price'] < 100]
            
            if valid_prices:
                # 按优先级排序（sina > tencent）
                source_priority = {'sina': 1, 'tencent': 2}
                valid_prices.sort(key=lambda x: source_priority.get(x.get('source', ''), 999))
                
                # 返回优先级最高的有效价格，标记置信度为高（通过了价格范围校验）
                result = {k: v for k, v in valid_prices[0].items() if k != 'source'}
                result['price_confidence'] = 'high'
                return result
        
        return None
    
    def get_fund_nav(self, fund_code: str) -> Optional[Dict]:
        """
        获取LOF基金场外净值
        数据源优先级：实时API > SSE Excel文件
        实时数据源：东方财富基金净值API、天天基金网API、akshare
        备用数据源：SSE Excel文件（标准数据源，但更新较慢）
        
        Args:
            fund_code: 基金代码（6位数字）
            
        Returns:
            包含净值信息的字典，如果失败返回None
        """
        navs = []  # 收集多个数据源的净值
        
        # 方法1：东方财富基金净值API（最实时，优先级最高）
        if self._is_source_enabled('nav_sources', 'eastmoney_api'):
            try:
                import time
                import json
                url = 'http://api.fund.eastmoney.com/f10/lsjz'
                params = {
                    'callback': 'jQuery',
                    'fundCode': fund_code,
                    'pageIndex': 1,
                    'pageSize': 1,
                    'startDate': '',
                    'endDate': '',
                    '_': int(time.time() * 1000)
                }
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Referer': 'http://fundf10.eastmoney.com/'
                }
                response = self.session.get(url, params=params, headers=headers, timeout=10)
                if response.status_code == 200:
                    content = response.text
                    import re
                    json_match = re.search(r'jQuery\((.+)\)', content)
                    if json_match:
                        try:
                            data = json.loads(json_match.group(1))
                            if data.get('Data') and data['Data'].get('LSJZList'):
                                lsjz = data['Data']['LSJZList'][0]
                                nav = float(lsjz.get('DWJZ', 0))
                                if nav > 0:
                                    navs.append({
                                        'code': fund_code,
                                        'nav': nav,
                                        'date': lsjz.get('FSRQ', ''),
                                        'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'source': 'eastmoney_api',
                                        'priority': 1
                                    })
                        except (json.JSONDecodeError, ValueError, KeyError, IndexError):
                            pass
            except Exception as e:
                print(f"东方财富净值API获取失败 {fund_code}: {e}")
        
        # 方法2：akshare获取基金净值（如果可用，实时性较好）
        if AKSHARE_AVAILABLE and self._is_source_enabled('nav_sources', 'akshare'):
            try:
                import akshare as ak
                # 使用akshare获取基金净值（获取最新净值）
                # 注意：akshare的API可能需要不同的参数，这里使用通用方法
                try:
                    # 尝试使用fund_open_fund_info_em获取净值走势
                    fund_nav = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
                    if fund_nav is not None and not fund_nav.empty:
                        # 按日期列排序后取最后一行，避免因数据源顺序不同取到旧净值
                        date_col = next((c for c in ['净值日期', '日期', 'FSRQ', 'date'] if c in fund_nav.columns), None)
                        if date_col:
                            try:
                                fund_nav = fund_nav.sort_values(date_col)
                            except Exception:
                                pass
                        latest_nav = fund_nav.iloc[-1]
                        # 尝试不同的列名
                        nav_value = 0
                        nav_date = ''
                        for col in ['净值', '单位净值', 'DWJZ', 'net_value']:
                            if col in latest_nav:
                                try:
                                    nav_value = float(latest_nav[col])
                                    break
                                except:
                                    continue
                        for col in ['净值日期', '日期', 'FSRQ', 'date']:
                            if col in latest_nav:
                                try:
                                    nav_date = str(latest_nav[col])
                                    break
                                except:
                                    continue
                        
                        if nav_value > 0:
                            navs.append({
                                'code': fund_code,
                                'nav': nav_value,
                                'date': nav_date,
                                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'source': 'akshare',
                                'priority': 2
                            })
                except Exception as e2:
                    # 如果fund_open_fund_info_em失败，尝试其他方法
                    print(f"akshare获取净值失败（方法1） {fund_code}: {e2}")
            except Exception as e:
                print(f"akshare获取净值失败 {fund_code}: {e}")
        
        # 方法3：从天天基金网API获取净值（实时性较好）
        if self._is_source_enabled('nav_sources', 'eastmoney_fundf10'):
            try:
                url = f'http://fundf10.eastmoney.com/F10DataApi.aspx?type=lsjz&code={fund_code}&page=1&per=1'
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Referer': 'http://fundf10.eastmoney.com/'
                }
                response = self.session.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    content = response.text
                    import re
                    # 解析HTML表格，查找最新净值
                    table_match = re.search(r'<table[^>]*>(.*?)</table>', content, re.DOTALL | re.IGNORECASE)
                    if table_match:
                        table_content = table_match.group(1)
                        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_content, re.DOTALL | re.IGNORECASE)
                        if rows and len(rows) > 1:  # 第一行是表头，第二行是数据
                            data_row = rows[1]
                            cells = re.findall(r'<td[^>]*>(.*?)</td>', data_row, re.DOTALL | re.IGNORECASE)
                            if len(cells) >= 2:
                                try:
                                    nav_value = float(cells[1].strip())
                                    nav_date = cells[0].strip() if len(cells) > 0 else ''
                                    if nav_value > 0:
                                        navs.append({
                                            'code': fund_code,
                                            'nav': nav_value,
                                            'date': nav_date,
                                            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                            'source': 'eastmoney_fundf10',
                                            'priority': 3
                                        })
                                except (ValueError, TypeError, IndexError):
                                    pass
            except Exception as e:
                print(f"从天天基金网获取净值失败 {fund_code}: {e}")
        
        # 方法4：从东方财富基金详情页获取净值（备用）
        if self._is_source_enabled('nav_sources', 'eastmoney_web'):
            try:
                url = f'https://fund.eastmoney.com/{fund_code}.html'
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Referer': 'https://fund.eastmoney.com/'
                }
                response = self.session.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    content = response.text
                    import re
                    # 尝试从页面提取净值
                    nav_patterns = [
                        r'"dwjz":"(\d+\.\d+)"',  # JSON数据中的净值（优先级最高）
                        r'单位净值[：:]\s*(\d+\.\d+)',
                        r'最新净值[：:]\s*(\d+\.\d+)',
                        r'fundNav["\']?\s*[>：:]\s*(\d+\.\d+)',
                    ]
                    for pattern in nav_patterns:
                        match = re.search(pattern, content)
                        if match:
                            try:
                                nav_value = float(match.group(1))
                                if nav_value > 0:
                                    # 尝试提取净值日期
                                    date_patterns = [
                                        r'净值日期[：:]\s*(\d{4}-\d{2}-\d{2})',
                                        r'"jzrq":"(\d{4}-\d{2}-\d{2})"',  # JSON数据中的日期
                                        r'(\d{4}-\d{2}-\d{2})',
                                    ]
                                    nav_date = ''
                                    for date_pattern in date_patterns:
                                        date_match = re.search(date_pattern, content)
                                        if date_match:
                                            nav_date = date_match.group(1)
                                            break
                                    
                                    navs.append({
                                        'code': fund_code,
                                        'nav': nav_value,
                                        'date': nav_date,
                                        'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'source': 'eastmoney_web',
                                        'priority': 4
                                    })
                                    break  # 找到净值后退出
                            except (ValueError, TypeError):
                                continue
            except Exception as e:
                print(f"从东方财富获取净值失败 {fund_code}: {e}")
        
        # 方法5：从SSE Excel文件读取（备用，更新较慢）
        if navs:  # 如果已经有实时数据，优先返回实时数据
            # 按优先级排序，返回优先级最高的
            navs.sort(key=lambda x: x.get('priority', 999))
            return {k: v for k, v in navs[0].items() if k not in ['priority', 'source']}
        
        # 如果实时数据源都失败，尝试从SSE Excel文件读取（备用，更新较慢）
        if self._is_source_enabled('nav_sources', 'sse_excel') and PANDAS_AVAILABLE:
            nav_file = os.path.join(_MODULE_DIR, 'data', 'LOF最新净值列表.xlsx')
            if os.path.exists(nav_file):
                try:
                    df = pd.read_excel(nav_file)
                    
                    # 查找列名：基金代码、基金名称、单位净值、净值日期
                    code_col = None
                    nav_col = None
                    date_col = None
                    
                    for col in df.columns:
                        col_str = str(col)
                        if any(kw in col_str for kw in ['基金代码', '代码', 'code']):
                            code_col = col
                        if any(kw in col_str for kw in ['单位净值', '净值', 'nav', 'NAV']):
                            nav_col = col
                        if any(kw in col_str for kw in ['净值日期', '日期', 'date', 'Date']):
                            date_col = col
                    
                    if code_col and nav_col:
                        # 查找匹配的基金代码
                        fund_row = df[df[code_col].astype(str).str.strip() == str(fund_code).strip()]
                        if not fund_row.empty:
                            nav_value = float(fund_row.iloc[0][nav_col])
                            nav_date = ''
                            if date_col:
                                nav_date = str(fund_row.iloc[0][date_col])
                            
                            if nav_value > 0:
                                return {
                                    'code': fund_code,
                                    'nav': nav_value,
                                    'date': nav_date,
                                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    'source': 'sse_excel'
                                }
                except Exception as e:
                    print(f"从SSE Excel读取净值失败 {fund_code}: {e}")
        
        # 如果所有数据源都失败，返回None
        return None
    
    def get_fund_info(self, fund_code: str) -> Optional[Dict]:
        """
        获取基金完整信息（价格+净值）
        数据源标准：以SSE为标准（基金代码、名称、净值、净值日期）
        补充数据源：场内价格使用东方财富套利API，限购信息使用东方财富套利API
        支持缓存机制
        
        Args:
            fund_code: 基金代码
            
        Returns:
            包含价格和净值的完整信息
        """
        
        # 尝试从缓存获取
        cache_mgr = get_cache_manager()
        if cache_mgr:
            cached_info = cache_mgr.get('fund_info', fund_code)
            if cached_info is not None:
                return cached_info
        
        # 缓存未命中，从数据源获取
        # 数据源标准：以SSE为标准
        # 1. 优先从SSE获取净值（标准数据源）
        nav_info = self.get_fund_nav(fund_code)
        
        # 2. 获取价格（补充数据源：新浪、腾讯）
        price_info = self.get_fund_price(fund_code)
        
        # 如果既没有SSE净值也没有价格，判定为数据缺失
        if not nav_info and not price_info:
            return None
        
        # 数据验证和合并
        if price_info and nav_info:
            # 数据合理性验证
            price = price_info.get('price', 0)
            nav = nav_info.get('nav', 0)
            nav_date_str = nav_info.get('date', '')
            
            # 检查净值日期是否过旧（超过30天没有更新，可能已清盘）或未来日期（数据异常）
            nav_date_too_old = False
            if nav_date_str:
                try:
                    nav_date = datetime.strptime(nav_date_str, '%Y-%m-%d')
                    days_old = (datetime.now() - nav_date).days
                    # 如果净值日期过旧（超过30天）或者是未来日期（数据异常），标记为已清盘
                    if days_old > 30 or days_old < 0:
                        nav_date_too_old = True
                except:
                    pass
            
            # 检查价格和净值是否合理（价差不应超过50%）
            if price > 0 and nav > 0:
                diff_pct = abs(price - nav) / nav
                
                # 如果价差超过50%且净值日期过旧，判定为已清盘
                if diff_pct > 0.5 and nav_date_too_old:
                    return None
                
                if diff_pct > 0.5:  # 价差超过50%可能数据有误
                    print(f"警告：基金 {fund_code} 价差异常 ({diff_pct*100:.2f}%)，价格: {price}, 净值: {nav}")
                    # 仍然返回，但标记为可疑
                    result = {
                        **price_info,
                        'nav': nav_info['nav'],
                        'nav_date': nav_info['date'],
                        'data_warning': True,
                        'price_diff_pct': diff_pct * 100
                    }
                    # 暂时跳过限购信息获取，避免阻塞批量请求（限购信息可以在后续异步获取）
                    # 使用空 dict，避免覆盖数据库中已有的正确申购状态
                    result['purchase_limit'] = {}

                    # 保存到缓存
                    cache_mgr = get_cache_manager()
                    if cache_mgr:
                        cache_mgr.set('fund_info', result, fund_code)

                    return result

            result = {
                **price_info,
                'nav': nav_info['nav'],
                'nav_date': nav_info['date']
            }
            # 暂时跳过限购信息获取，避免阻塞批量请求（限购信息可以在后续异步获取）
            # 使用空 dict，避免覆盖数据库中已有的正确申购状态
            result['purchase_limit'] = {}
            
            # 保存到缓存
            cache_mgr = get_cache_manager()
            if cache_mgr:
                cache_mgr.set('fund_info', result, fund_code)
            
            return result
        
        # 如果只有价格或只有净值，检查是否可能是已清盘基金
        if price_info and not nav_info:
            # 有价格但没有净值，可能是清盘前的最后价格
            # 检查价格数据是否可信（如果价格数据源数量少，可能不可信）
            price_confidence = price_info.get('price_confidence', 'unknown')
            if price_confidence == 'low':
                return None
        
        if nav_info and not price_info:
            # 有净值但没有价格，检查净值日期
            nav_date_str = nav_info.get('date', '')
            nav_date_too_old = False
            if nav_date_str:
                try:
                    nav_date = datetime.strptime(nav_date_str, '%Y-%m-%d')
                    days_old = (datetime.now() - nav_date).days
                    # 如果净值日期过旧（超过30天）或者是未来日期（数据异常），判定为已清盘
                    if days_old > 30 or days_old < 0:
                        nav_date_too_old = True
                        return None
                except:
                    pass
            
            # 如果净值日期正常，返回只有净值的数据（价格设为0，表示暂无价格数据）
            if not nav_date_too_old:
                result = {
                    'code': fund_code,
                    'price': 0,  # 暂无价格数据
                    'nav': nav_info['nav'],
                    'nav_date': nav_info['date'],
                    'change_pct': 0,
                    'volume': 0,
                    'amount': 0,
                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'price_missing': True  # 标记价格缺失
                }
                result['purchase_limit'] = {}  # 空 dict，避免覆盖数据库中已有的正确申购状态

                # 保存到缓存
                cache_mgr = get_cache_manager()
                if cache_mgr:
                    cache_mgr.set('fund_info', result, fund_code)

                return result

        # 如果只有价格没有净值，返回基础数据（允许只有价格的情况）
        if price_info and not nav_info:
            price_confidence = price_info.get('price_confidence', 'unknown')
            # 只有价格置信度低时才返回None，否则返回只有价格的数据
            if price_confidence == 'low':
                return None
            
            # 返回只有价格的数据（净值设为0，表示暂无净值数据）
            result = {
                **price_info,
                'nav': 0,  # 暂无净值数据
                'nav_date': '',
                'nav_missing': True  # 标记净值缺失
            }
            result['purchase_limit'] = {}  # 空 dict，避免覆盖数据库中已有的正确申购状态

            # 保存到缓存
            cache_mgr = get_cache_manager()
            if cache_mgr:
                cache_mgr.set('fund_info', result, fund_code)

            return result

        # 如果既没有价格也没有净值，返回None
        return None
    
    def get_all_funds_arbitrage_data(self, fund_codes: List[str] = None) -> Dict[str, Dict]:
        """
        批量获取所有LOF基金的套利数据（价格+净值+溢价率）
        一次性获取所有基金数据，避免逐个请求
        
        Args:
            fund_codes: 基金代码列表，如果为None则获取所有基金
            
        Returns:
            字典，key为基金代码，value为基金数据
        """
        result = {}
        try:
            url = 'https://zqhdplus.eastmoney.com/api/fundArbitrage/getFundArbitrageList'
            # 尝试不传fundCode，获取所有基金数据
            params = {
                'pageIndex': 1,
                'pageSize': 500,  # 增大pageSize，尝试获取更多数据
            }
            
            # 如果指定了基金代码，只请求一次，然后过滤
            # 否则请求所有数据
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('Data') and data['Data'].get('List'):
                    fund_set = set(fund_codes) if fund_codes else None
                    
                    for item in data['Data']['List']:
                        fund_code = item.get('FundCode', '')
                        if not fund_code:
                            continue
                            
                        # 如果指定了基金代码列表，只处理列表中的基金
                        if fund_set and fund_code not in fund_set:
                            continue
                            
                        market_price = float(item.get('MarketPrice', 0))
                        net_value = float(item.get('NetValue', 0))
                        nav_date_str = item.get('NetValueDate', '')
                        
                        # 检查净值日期是否过旧（超过30天没有更新，可能已清盘）或未来日期（数据异常）
                        nav_date_too_old = False
                        if nav_date_str:
                            try:
                                nav_date = datetime.strptime(nav_date_str, '%Y-%m-%d')
                                days_old = (datetime.now() - nav_date).days
                                # 如果净值日期过旧（超过30天）或者是未来日期（数据异常），标记为已清盘
                                if days_old > 30 or days_old < 0:
                                    nav_date_too_old = True
                            except:
                                pass
                        
                        # 只处理价格和净值都有效，且净值日期不过旧的基金
                        if market_price > 0 and net_value > 0 and not nav_date_too_old:
                            result[fund_code] = {
                                'code': fund_code,
                                'price': market_price,
                                'nav': net_value,
                                'premium_rate': float(item.get('PremiumRate', 0)),
                                'change_pct': float(item.get('ChangePercent', 0)) / 100,
                                'volume': item.get('Volume', 0),
                                'amount': item.get('Amount', 0),
                                'nav_date': nav_date_str,
                                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'source': 'arbitrage_api'
                            }
        except Exception as e:
            print(f"批量套利API获取失败: {e}")
        
        return result
    
    # 已移除：get_fund_arbitrage_data方法
    # 原因：东方财富套利API返回404，不再使用
    # 价格数据源改为：新浪财经、腾讯财经
    # 净值数据源改为：仅使用SSE Excel（标准数据源）
    
    def get_fund_purchase_limit(self, fund_code: str) -> Optional[Dict]:
        """
        获取基金申购限购信息（使用天天基金网数据源）
        
        Args:
            fund_code: 基金代码
            
        Returns:
            包含限购信息的字典: {'is_limited': bool, 'limit_amount': float, 'limit_unit': str, 'limit_desc': str, 'purchase_status': str}
            purchase_status: '暂停申购' | '限购' | '开放申购'
            如果无法获取限购信息，返回None
        """
        # 方法1：从天天基金网获取限购信息（最准确的数据源）
        try:
            url = f'http://fundf10.eastmoney.com/jjfl_{fund_code}.html'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'http://fundf10.eastmoney.com/'
            }
            response = self.session.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                content = response.text
                import re
                
                # 标记是否检测到"开放申购"状态（用于后续判断）
                found_open_status = False
                
                # 优先检查申购状态（更准确，避免误判）
                if '申购状态' in content:
                    # 检查表格中的申购状态字段
                    purchase_status_patterns = [
                        r'申购状态[^<]*</td>\s*<td[^>]*>([^<]*)</td>',
                        r'<td[^>]*>申购状态</td>\s*<td[^>]*>([^<]*)</td>',
                        r'申购状态[：:]\s*([^<\n]*)',
                    ]
                    for pattern in purchase_status_patterns:
                        status_match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
                        if status_match:
                            status_text = status_match.group(1).strip()
                            # 检查是否暂停申购
                            if '暂停申购' in status_text:
                                print(f"基金 {fund_code} 天天基金网申购状态: 暂停申购 (状态文本: {status_text})")
                                return {
                                    'is_limited': True,
                                    'limit_amount': 0,
                                    'limit_unit': '元',
                                    'limit_desc': '暂停申购',
                                    'purchase_status': '暂停申购'
                                }
                            # 检查是否开放申购
                            elif '开放申购' in status_text or '正常申购' in status_text or '可申购' in status_text or '无限制' in status_text:
                                print(f"基金 {fund_code} 天天基金网申购状态: 开放申购 (状态文本: {status_text})")
                                # 标记为开放申购，但继续检查是否有限购金额
                                found_open_status = True
                                # 注意：这里不立即返回，继续检查限购金额（因为可能同时有限购）
                                break
                
                # 检查限购金额（无论是否检测到"开放申购"状态，都要检查限购金额）
                if '限购' in content or '申购限额' in content or '申购状态' in content:
                    
                    # 优先方法1：从表格中提取"单日累计申购限额"（最准确，这是真正的限购）
                    # 表格格式：<td class="th w110">单日累计申购限额</td><td class="w135">100.00元</td>
                    # 或者：<td>单日累计申购限额</td><td>100.00元</td>
                    table_patterns = [
                        r'单日累计申购限额[^<]*</td>\s*<td[^>]*>(\d+(?:\.\d+)?)\s*元',  # 标准表格格式
                        r'<td[^>]*>单日累计申购限额</td>\s*<td[^>]*>(\d+(?:\.\d+)?)\s*元',  # 另一种表格格式
                    ]
                    for pattern in table_patterns:
                        table_match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
                        if table_match:
                            try:
                                limit_amount = float(table_match.group(1))
                                if limit_amount > 0:
                                    return {
                                        'is_limited': True,
                                        'limit_amount': limit_amount,
                                        'limit_unit': '元',
                                        'limit_desc': f'限购 {limit_amount:.2f} 元' if limit_amount < 100 else f'限购 {limit_amount:.0f} 元',
                                        'purchase_status': '限购'
                                    }
                            except (ValueError, TypeError):
                                continue
                    
                    # 方法2：提取"单日累计申购限额"（文本形式，非表格）
                    text_pattern = r'单日累计申购限额[：:]\s*(\d+(?:\.\d+)?)\s*元'
                    text_match = re.search(text_pattern, content, re.IGNORECASE)
                    if text_match:
                        try:
                            limit_amount = float(text_match.group(1))
                            if limit_amount > 0:
                                return {
                                    'is_limited': True,
                                    'limit_amount': limit_amount,
                                    'limit_unit': '元',
                                    'limit_desc': f'限购 {limit_amount:.2f} 元' if limit_amount < 100 else f'限购 {limit_amount:.0f} 元',
                                    'purchase_status': '限购'
                                }
                        except (ValueError, TypeError):
                            pass
                    
                    # 方法3：如果找不到"单日累计申购限额"，尝试查找"申购限额"（但要排除"申购起点"、"单笔申购限额"等）
                    # 使用更精确的匹配，确保不匹配到"申购起点"、"追加起点"、"单笔申购限额"等
                    # 查找表格中"申购限额"字段（排除包含"起点"、"单笔"、"追加"的行）
                    table_rows = re.findall(r'<tr[^>]*>.*?</tr>', content, re.DOTALL | re.IGNORECASE)
                    for row in table_rows:
                        # 优先检查"单日累计申购限额"
                        if '单日累计申购限额' in row:
                            # 提取这一行中的金额（优先"元"单位）
                            amount_match = re.search(r'<td[^>]*>(\d+(?:\.\d+)?)\s*元', row, re.IGNORECASE)
                            if amount_match:
                                try:
                                    limit_amount = float(amount_match.group(1))
                                    if limit_amount > 0:
                                        return {
                                            'is_limited': True,
                                            'limit_amount': limit_amount,
                                            'limit_unit': '元',
                                            'limit_desc': f'限购 {limit_amount:.2f} 元' if limit_amount < 100 else f'限购 {limit_amount:.0f} 元',
                                            'purchase_status': '限购'
                                        }
                                except (ValueError, TypeError):
                                    pass
                            # 如果没有"元"单位，尝试"万"单位
                            amount_match_wan = re.search(r'<td[^>]*>(\d+(?:\.\d+)?)\s*万', row, re.IGNORECASE)
                            if amount_match_wan:
                                try:
                                    limit_amount_wan = float(amount_match_wan.group(1))
                                    limit_amount = limit_amount_wan * 10000
                                    if limit_amount > 0:
                                        return {
                                            'is_limited': True,
                                            'limit_amount': limit_amount,
                                            'limit_unit': '元',
                                            'limit_desc': f'限购 {limit_amount_wan:.0f} 万元',
                                            'purchase_status': '限购'
                                        }
                                except (ValueError, TypeError):
                                    pass
                        # 检查这一行是否包含"申购限额"但不包含"起点"、"单笔"、"追加"
                        elif '申购限额' in row and '起点' not in row and '单笔' not in row and '追加' not in row and '累计' not in row:
                            # 提取这一行中的金额（只匹配"元"单位，避免误匹配"万"）
                            amount_match = re.search(r'<td[^>]*>(\d+(?:\.\d+)?)\s*元', row, re.IGNORECASE)
                            if amount_match:
                                try:
                                    limit_amount = float(amount_match.group(1))
                                    if limit_amount > 0:
                                        return {
                                            'is_limited': True,
                                            'limit_amount': limit_amount,
                                            'limit_unit': '元',
                                            'limit_desc': f'限购 {limit_amount:.2f} 元' if limit_amount < 100 else f'限购 {limit_amount:.0f} 元',
                                            'purchase_status': '限购'
                                        }
                                except (ValueError, TypeError):
                                    continue
                    
                    # 如果前面找到了"开放申购"状态但没找到限购金额
                    # 先尝试从 AKShare 缓存补充限购金额（AKShare 包含单日累计限定额度）
                    if found_open_status:
                        akshare_limit = self._get_akshare_limit(fund_code)
                        if akshare_limit:
                            print(f"基金 {fund_code} AKShare补充限购信息: {akshare_limit.get('limit_desc')}")
                            return akshare_limit
                        print(f"基金 {fund_code} 天天基金网: 检测到开放申购状态，未找到限购金额，返回开放申购")
                        return {
                            'is_limited': False,
                            'limit_amount': None,
                            'limit_unit': None,
                            'limit_desc': '开放申购',
                            'purchase_status': '开放申购'
                        }
                    
                    # 如果找到限购关键词但没提取到金额，检查是否暂停申购
                    # 更精确地检查"暂停申购"，避免误判"暂停赎回"等
                    # 注意：已经在前面优先检查了申购状态，这里作为备用检查
                    if '暂停申购' in content and '暂停赎回' not in content:
                        # 再次确认是暂停申购而不是暂停赎回
                        print(f"基金 {fund_code} 天天基金网检测到暂停申购（备用检查）")
                        return {
                            'is_limited': True,
                            'limit_amount': 0,
                            'limit_unit': '元',
                            'limit_desc': '暂停申购',
                            'purchase_status': '暂停申购'
                        }
        except Exception as e:
            print(f"天天基金网获取限购信息失败 {fund_code}: {e}")
        
        # 方法2：从东方财富基金详情页获取（优先检查交易状态，这个页面的交易状态信息更准确）
        try:
            url = f'https://fund.eastmoney.com/{fund_code}.html'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://fund.eastmoney.com/'
            }
            response = self.session.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                content = response.text
                import re
                
                # 优先检查交易状态（这个页面的交易状态信息更准确）
                # 查找交易状态相关的信息
                status_patterns = [
                    # 表格格式：<td>交易状态</td><td>暂停申购</td>
                    r'<td[^>]*>交易状态</td>\s*<td[^>]*>([^<]*)</td>',
                    r'交易状态[^<]*</td>\s*<td[^>]*>([^<]*)</td>',
                    # 文本格式：交易状态：暂停申购
                    r'交易状态[：:]\s*([^<\n]*)',
                    # 其他可能的格式
                    r'<td[^>]*>申购状态</td>\s*<td[^>]*>([^<]*)</td>',
                    r'申购状态[^<]*</td>\s*<td[^>]*>([^<]*)</td>',
                    r'申购状态[：:]\s*([^<\n]*)',
                ]
                
                for pattern in status_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE | re.DOTALL)
                    if matches:
                        status_text = matches[0].strip()
                        if status_text:
                            # 检查是否暂停申购
                            if '暂停申购' in status_text or ('暂停' in status_text and '赎回' not in status_text):
                                print(f"基金 {fund_code} 东方财富基金详情页交易状态: {status_text} -> 暂停申购")
                                return {
                                    'is_limited': True,
                                    'limit_amount': 0,
                                    'limit_unit': '元',
                                    'limit_desc': '暂停申购',
                                    'purchase_status': '暂停申购'
                                }
                            # 检查是否开放申购
                            elif '开放申购' in status_text or '正常申购' in status_text or '可申购' in status_text:
                                print(f"基金 {fund_code} 东方财富基金详情页交易状态: {status_text} -> 开放申购")
                                # 继续检查是否有限购金额
                                break
                
                # 如果找到限购或申购限额信息
                if '限购' in content or '申购限额' in content:
                    patterns = [
                        r'单日.*?(\d+(?:\.\d+)?)\s*万',
                        r'限购.*?(\d+(?:\.\d+)?)\s*万',
                        r'申购限额.*?(\d+(?:\.\d+)?)\s*万',
                        # 也尝试匹配"元"单位
                        r'单日.*?(\d+(?:\.\d+)?)\s*元',
                        r'限购.*?(\d+(?:\.\d+)?)\s*元',
                        r'申购限额.*?(\d+(?:\.\d+)?)\s*元',
                    ]
                    for pattern in patterns:
                        matches = re.findall(pattern, content)
                        if matches:
                            try:
                                amount = float(matches[0])
                                # 如果是"万"单位，转换为"元"
                                if '万' in pattern:
                                    limit_amount = amount * 10000
                                    limit_desc = f'限购 {amount:.0f} 万元'
                                else:
                                    limit_amount = amount
                                    limit_desc = f'限购 {amount:.0f} 元' if amount < 100 else f'限购 {amount:.0f} 元'
                                
                                if limit_amount > 0:
                                    print(f"基金 {fund_code} 东方财富基金详情页限购金额: {limit_amount} 元")
                                    return {
                                        'is_limited': True,
                                        'limit_amount': limit_amount,
                                        'limit_unit': '元',
                                        'limit_desc': limit_desc,
                                        'purchase_status': '限购'
                                    }
                            except (ValueError, TypeError):
                                continue
        except Exception as e:
            print(f"东方财富基金详情页获取限购信息失败 {fund_code}: {e}")
        
        # 方法3：使用akshare获取基金限购信息（备用，数据可能不准确）
        if AKSHARE_AVAILABLE:
                try:
                    
                    # 尝试使用akshare的基金基本信息接口
                    # 注意：基金代码可能需要添加后缀，如 'of' (场外) 或 'sh'/'sz' (场内)
                    fund_code_full = f"{fund_code}.OF"  # 场外基金
                    
                    # 尝试使用akshare的基金申购赎回信息（包含限购信息）
                    try:
                        
                        # fund_purchase_em()不接受参数，返回所有基金的申购状态
                        # 使用缓存，避免重复下载（缓存5分钟）
                        import time as time_module
                        current_time = time_module.time()
                        if (self._akshare_purchase_cache is None or 
                            self._akshare_purchase_cache_time is None or 
                            current_time - self._akshare_purchase_cache_time > 300):  # 5分钟缓存
                            
                            try:
                                print(f"正在从akshare下载基金申购数据...")
                                self._akshare_purchase_cache = ak.fund_purchase_em()
                                self._akshare_purchase_cache_time = current_time
                                
                                print(f"akshare数据下载完成，共 {len(self._akshare_purchase_cache)} 条记录")
                            except Exception as e:
                                print(f"akshare数据下载失败: {e}")
                                # 如果下载失败，缓存设为空，跳过akshare方法
                                self._akshare_purchase_cache = None
                                raise  # 继续使用其他方法获取限购信息

                        all_funds_df = self._akshare_purchase_cache
                        if all_funds_df is not None and not all_funds_df.empty:
                            # 从所有基金中筛选出指定基金
                            # 基金代码通常在第二列（索引1）
                            cols = list(all_funds_df.columns)
                            fund_code_col = cols[1]  # 基金代码列
                            fund_record = all_funds_df[all_funds_df[fund_code_col].astype(str).str.contains(fund_code)]
                            
                            if not fund_record.empty:
                                fund_row = fund_record.iloc[0]
                                
                                
                                # 查找限购相关列（akshare返回的列名是中文）
                                cols = list(fund_record.columns)
                                
                                # 优先查找"单日累计限定额度"列（akShare返回的列名，索引通常是倒数第二列）
                                limit_col = None
                                
                                # 方法1：按列名匹配
                                for col in cols:
                                    col_str = str(col)
                                    # 查找包含"单日累计限定额度"的列（不包含"原始值"）
                                    if '单日累计限定额度' in col_str and '原始值' not in col_str:
                                        limit_col = col
                                        break
                                
                                # 方法2：如果没找到，查找其他限购相关列
                                if limit_col is None:
                                    limit_keywords = ['单日累计限定额度', '限额', '限购', '单日累计', '单笔限额', '最大申购']
                                    for col in cols:
                                        col_str = str(col)
                                        if any(keyword in col_str for keyword in limit_keywords) and '原始值' not in col_str:
                                            limit_col = col
                                            break
                                
                                # 方法3：如果还是没找到，尝试倒数第二列（通常是限购列）
                                if limit_col is None and len(cols) >= 2:
                                    # 倒数第二列通常是"单日累计限定额度"
                                    limit_col = cols[-2]
                                
                                # 如果找到了限购列，解析限购金额
                                if limit_col is not None:
                                    try:
                                        limit_value = fund_row[limit_col]
                                        # 检查值是否有效
                                        if limit_value is not None:
                                            # 使用pandas的notna检查
                                            import pandas as pd
                                            if pd.notna(limit_value):
                                                limit_amount = float(limit_value)
                                                
                                                # akShare返回的"单日累计限定额度"通常是以"元"为单位
                                                # 直接使用原始值，不做单位转换（避免误将100元转换为100万元）
                                                
                                                # 过滤异常大的值（可能是错误数据，如100000000000.0）
                                                if limit_amount > 1e10:  # 大于100亿，可能是错误数据
                                                    limit_amount = None
                                                
                                                # 如果值在合理范围内（1元到1亿元），直接使用
                                                if limit_amount and 1 <= limit_amount <= 100000000:  # 1元到1亿元之间
                                                    return {
                                                        'is_limited': True,
                                                        'limit_amount': limit_amount,
                                                        'limit_unit': '元',
                                                        'limit_desc': f'限购 {limit_amount/10000:.0f} 万元' if limit_amount >= 10000 else f'限购 {limit_amount:.0f} 元',
                                                        'purchase_status': '限购'
                                                    }
                                    except (ValueError, TypeError) as e:
                                        print(f"解析限购金额失败 {fund_code}: {e}")
                                
                                # 如果没找到限购信息，检查申购状态
                                purchase_status_col = None
                                for col in cols:
                                    if '申购状态' in str(col):
                                        purchase_status_col = col
                                        break
                                
                                if purchase_status_col:
                                    purchase_status = fund_row[purchase_status_col]
                                    purchase_status_str = str(purchase_status).strip() if purchase_status else ''
                                    
                                    # 优先检查暂停申购（最明确的状态）
                                    if purchase_status_str and '暂停申购' in purchase_status_str:
                                        print(f"基金 {fund_code} akshare申购状态: {purchase_status_str} -> 暂停申购")
                                        return {
                                            'is_limited': True,
                                            'limit_amount': 0,
                                            'limit_unit': '元',
                                            'limit_desc': '暂停申购',
                                            'purchase_status': '暂停申购'
                                        }
                                    # 检查开放申购相关状态
                                    elif purchase_status_str and ('开放申购' in purchase_status_str or '正常申购' in purchase_status_str or '可申购' in purchase_status_str or '开放' in purchase_status_str):
                                        print(f"基金 {fund_code} akshare申购状态: {purchase_status_str} -> 开放申购")
                                        return {
                                            'is_limited': False,
                                            'limit_amount': None,
                                            'limit_unit': None,
                                            'limit_desc': '开放申购',
                                            'purchase_status': '开放申购'
                                        }
                                    # 如果申购状态存在但不是已知状态，记录日志但不返回（继续使用其他数据源）
                                    elif purchase_status_str:
                                        print(f"基金 {fund_code} akshare申购状态未知: {purchase_status_str}，继续使用其他数据源")
                                        # 不返回，继续使用其他数据源（天天基金网等）
                            else:
                                pass  # 未找到对应基金记录，继续使用其他数据源
                    except Exception as e:
                        pass  # 内层异常已被外层捕获
                except Exception as e:
                    print(f"akshare获取限购信息失败 {fund_code}: {e}")
        
        # 如果akShare不可用或获取失败，返回开放申购（默认状态）
        return {
            'is_limited': False,
            'limit_amount': None,
            'limit_unit': None,
            'limit_desc': '开放申购',
            'purchase_status': '开放申购'
        }
    
    def _warm_akshare_cache(self):
        """预热 AKShare 申购状态缓存（下载全量数据）。
        由 background_updater 在每轮 update_all_purchase_limits() 开始时调用。
        缓存有效期 5 分钟内不重复下载。
        """
        try:
            if not AKSHARE_AVAILABLE:
                return
            import time as _t
            if (self._akshare_purchase_cache is not None and
                    not self._akshare_purchase_cache.empty and
                    self._akshare_purchase_cache_time is not None and
                    _t.time() - self._akshare_purchase_cache_time < 300):
                return  # 缓存仍在有效期内，无需重新下载
            print("预热 AKShare 基金申购状态缓存...")
            import akshare as ak
            df = ak.fund_purchase_em()
            if df is not None and not df.empty:
                self._akshare_purchase_cache = df
                self._akshare_purchase_cache_time = _t.time()
                print(f"AKShare 申购状态缓存预热完成，共 {len(df)} 条记录")
            else:
                print("AKShare 返回空数据，缓存未更新")
        except Exception as e:
            print(f"AKShare 缓存预热失败: {e}")

    def _get_akshare_limit(self, fund_code: str) -> Optional[Dict]:
        """从 AKShare 缓存中查找基金的限购金额。
        只使用已有的缓存，不触发新的网络下载（避免阻塞更新循环）。
        缓存由 _warm_akshare_cache() 统一负责预热。
        如果找到有效限购金额则返回 '限购' 字典，否则返回 None。
        """
        try:
            if not AKSHARE_AVAILABLE:
                return None
            import time as _t
            # 只读缓存，不下载
            df = self._akshare_purchase_cache
            if df is None or df.empty:
                return None
            if (self._akshare_purchase_cache_time is None or
                    _t.time() - self._akshare_purchase_cache_time > 300):
                return None  # 缓存已过期，等待下次预热

            df = self._akshare_purchase_cache
            if df is None or df.empty:
                return None

            cols = list(df.columns)
            fund_code_col = cols[1]
            records = df[df[fund_code_col].astype(str).str.contains(fund_code)]
            if records.empty:
                return None

            row = records.iloc[0]

            # 查找"单日累计限定额度"列
            limit_col = None
            for col in cols:
                if '单日累计限定额度' in str(col) and '原始值' not in str(col):
                    limit_col = col
                    break
            if limit_col is None:
                for col in cols:
                    col_str = str(col)
                    if any(kw in col_str for kw in ['限额', '限购', '单日累计', '单笔限额', '最大申购']) and '原始值' not in col_str:
                        limit_col = col
                        break
            if limit_col is None and len(cols) >= 2:
                limit_col = cols[-2]

            if limit_col is not None:
                import pandas as pd
                limit_value = row[limit_col]
                if limit_value is not None and pd.notna(limit_value):
                    try:
                        limit_amount = float(limit_value)
                        if 1 <= limit_amount <= 1e8:  # 1元到1亿元之间为合理限购范围
                            desc = (f'限购 {limit_amount/10000:.0f} 万元'
                                    if limit_amount >= 10000
                                    else f'限购 {limit_amount:.0f} 元')
                            return {
                                'is_limited': True,
                                'limit_amount': limit_amount,
                                'limit_unit': '元',
                                'limit_desc': desc,
                                'purchase_status': '限购'
                            }
                    except (ValueError, TypeError):
                        pass
            return None
        except Exception:
            return None

    def _parse_purchase_limit(self, limit_value, field_name: str) -> Dict:
        """解析限购信息"""
        try:
            if limit_value == 0 or limit_value == '0' or limit_value == '':
                return {
                    'is_limited': False,
                    'limit_amount': None,
                    'limit_unit': None,
                    'limit_desc': '不限购'
                }
            
            # 尝试转换为数值
            if isinstance(limit_value, (int, float)):
                amount = float(limit_value)
                if amount > 0:
                    return {
                        'is_limited': True,
                        'limit_amount': amount,
                        'limit_unit': '元',
                        'limit_desc': f'限购 {amount:,.0f} 元'
                    }
            
            # 如果是字符串，尝试解析
            if isinstance(limit_value, str):
                return self._parse_purchase_limit_from_text(limit_value, '元')
                
        except Exception as e:
            print(f"解析限购信息失败: {e}")
        
        return {
            'is_limited': False,
            'limit_amount': None,
            'limit_unit': None,
            'limit_desc': '不限购'
        }
    
    def _parse_purchase_limit_from_text(self, amount, unit: str) -> Dict:
        """从文本解析限购信息"""
        try:
            # 如果amount是字符串，先尝试转换
            if isinstance(amount, str):
                amount_str = amount.replace(',', '').replace('，', '').strip()
                try:
                    amount = float(amount_str)
                except ValueError:
                    return {
                        'is_limited': False,
                        'limit_amount': None,
                        'limit_unit': None,
                        'limit_desc': '不限购'
                    }
            
            # 转换为浮点数
            amount = float(amount)
            
            # 单位转换
            multiplier = 1
            if '万' in unit or '萬' in unit:
                multiplier = 10000
            elif '千' in unit:
                multiplier = 1000
            
            final_amount = amount * multiplier
            
            if final_amount > 0:
                if final_amount >= 10000:
                    desc = f'限购 {final_amount/10000:.2f} 万元'
                else:
                    desc = f'限购 {final_amount:.2f} 元'
                return {
                    'is_limited': True,
                    'limit_amount': final_amount,
                    'limit_unit': '元',
                    'limit_desc': desc
                }
        except Exception as e:
            print(f"解析限购文本失败: {e}")
        
        return {
            'is_limited': False,
            'limit_amount': None,
            'limit_unit': None,
            'limit_desc': '不限购'
        }

class MockDataFetcher:
    """模拟数据获取器（用于测试）"""
    
    def __init__(self):
        import random
        self.random = random
    
    def get_fund_price(self, fund_code: str) -> Optional[Dict]:
        """模拟价格数据"""
        base_price = 1.0 + self.random.uniform(-0.2, 0.2)
        return {
            'code': fund_code,
            'price': round(base_price, 3),
            'change_pct': round(self.random.uniform(-0.05, 0.05), 4),
            'volume': self.random.randint(1000000, 10000000),
            'amount': self.random.randint(10000000, 100000000),
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def get_fund_nav(self, fund_code: str) -> Optional[Dict]:
        """模拟净值数据"""
        base_nav = 1.0 + self.random.uniform(-0.2, 0.2)
        return {
            'code': fund_code,
            'nav': round(base_nav, 3),
            'date': datetime.now().strftime('%Y-%m-%d'),
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def get_fund_info(self, fund_code: str) -> Optional[Dict]:
        price_info = self.get_fund_price(fund_code)
        nav_info = self.get_fund_nav(fund_code)
        
        if price_info and nav_info:
            return {
                **price_info,
                'nav': nav_info['nav'],
                'nav_date': nav_info['date']
            }
        return None
