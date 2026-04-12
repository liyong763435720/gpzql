"""
数据更新服务
"""
import json
import logging
import os
import threading
import pandas as pd
from datetime import datetime, timedelta
from typing import Callable, Optional
import time
import traceback
from app.database import Database
from app.data_fetcher import DataFetcher
from app.config import Config

_logger = logging.getLogger(__name__)

_CHECKPOINT_FILE = 'update_checkpoint.json'


class DataUpdater:
    def __init__(self, db: Database, config: Config):
        self.db = db
        self.config = config
        self.fetcher = DataFetcher(config)
        self.data_source = config.get('data_source', 'tushare')
        self.progress_callback: Optional[Callable] = None
        # 控制事件：_pause_event 被清除时暂停，_stop_event 被设置时停止
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()  # 初始为"运行中"状态

    def stop(self):
        """请求停止（处理完当前股票后停止）"""
        self._stop_event.set()
        self._pause_event.set()  # 解除暂停阻塞

    def pause(self):
        """暂停（处理完当前股票后暂停）"""
        self._pause_event.clear()

    def resume(self):
        """从暂停处恢复"""
        self._pause_event.set()

    def _check_control(self) -> bool:
        """检查暂停/停止状态。暂停时阻塞，返回 True 表示需要停止。"""
        self._pause_event.wait()  # 暂停时阻塞直到 resume()
        return self._stop_event.is_set()

    def set_progress_callback(self, callback: Callable):
        """设置进度回调函数"""
        self.progress_callback = callback

    def _update_progress(self, current: int, total: int, message: str = ""):
        """更新进度"""
        if self.progress_callback:
            try:
                self.progress_callback(current, total, message)
            except Exception as e:
                _logger.warning("Progress callback error: %s", e)

    # ── 断点管理 ──────────────────────────────────────────────────────────────

    def save_checkpoint(self, market: Optional[str],
                        completed_codes: set, total: int):
        data = {
            'market': market,
            'completed_codes': list(completed_codes),
            'total': total,
            'saved_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        try:
            with open(_CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            _logger.warning("Failed to save checkpoint: %s", e)

    @staticmethod
    def load_checkpoint() -> Optional[dict]:
        try:
            if os.path.exists(_CHECKPOINT_FILE):
                with open(_CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception:
            pass
        return None

    @staticmethod
    def clear_checkpoint():
        try:
            if os.path.exists(_CHECKPOINT_FILE):
                os.remove(_CHECKPOINT_FILE)
        except Exception:
            pass
    
    def test_update_data(self, market: str = None, limit: int = 10):
        """测试更新数据（只更新前N条股票，用于快速测试）
        
        Args:
            market: 市场类型 'A', 'HK', 'US' 或 None（全部）
            limit: 更新的股票数量（默认10条）
        """
        try:
            # 1. 更新股票列表
            market_name = {'A': 'A股', 'HK': '港股', 'US': '美股'}.get(market, '全部市场')
            self._update_progress(0, 100, f"正在获取{market_name}股票列表（测试模式）...")
            stocks_df = self.fetcher.get_stock_list(market=market)
            if not stocks_df.empty:
                self.db.save_stocks(stocks_df)
                self._update_progress(10, 100, f"已获取 {len(stocks_df)} 只{market_name}股票")
            else:
                error_msg = f"{market_name}股票列表获取失败"
                _logger.error(error_msg)
                self._update_progress(10, 100, error_msg)
                return False

            # 2. 只更新前N条股票
            test_stocks_df = stocks_df.head(limit)
            total_stocks = len(test_stocks_df)
            processed = 0
            
            self._update_progress(10, 100, f"开始测试更新前{total_stocks}条股票...")
            
            for idx, row in test_stocks_df.iterrows():
                # 每只股票处理前检查暂停/停止
                if self._check_control():
                    progress = 10 + int((processed / total_stocks) * 80) if total_stocks > 0 else 10
                    self._update_progress(progress, 100, f"⏹ 已停止 [{processed}/{total_stocks}]")
                    return False

                ts_code = row['ts_code']
                list_date = row['list_date']
                
                # 确定起始日期（最近1年）
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
                
                if list_date and len(list_date) == 8:
                    start_date = max(list_date, start_date)
                
                try:
                    stock_market = self.fetcher.detect_market(ts_code)
                    data_source = self.fetcher.get_data_source_for_market(stock_market)
                    kline_df = self.fetcher.get_monthly_kline(ts_code, start_date, end_date)

                    if not kline_df.empty:
                        kline_df = self.fetcher.calculate_pct_chg(kline_df)
                        currency_map = {'A': 'CNY', 'HK': 'HKD', 'US': 'USD'}
                        currency = currency_map.get(stock_market, 'CNY')
                        self.db.save_monthly_kline(kline_df, data_source=data_source, market=stock_market, currency=currency)

                    processed += 1
                    progress = 10 + int((processed / total_stocks) * 80)
                    self._update_progress(progress, 100, f"测试更新 {row['name']} ({ts_code})... [{processed}/{total_stocks}]")

                    # 按市场调整延迟
                    if stock_market in ['HK', 'US']:
                        time.sleep(3)
                    elif data_source == 'akshare':
                        time.sleep(1.0)
                    else:
                        time.sleep(0.5)
                except Exception as e:
                    error_msg = str(e)
                    _logger.error("Error updating %s: %s", ts_code, error_msg, exc_info=True)
                    # 更新进度，显示错误信息
                    processed += 1
                    progress = 10 + int((processed / total_stocks) * 80)
                    self._update_progress(progress, 100, f"测试更新 {row['name']} ({ts_code}) 时出错: {error_msg[:50]}... [{processed}/{total_stocks}]")
                    continue

            self._update_progress(100, 100, f"测试更新完成！已更新{processed}条股票")
            return True

        except Exception as e:
            error_msg = str(e)
            _logger.error("Error in test_update_data: %s", error_msg, exc_info=True)
            self._update_progress(100, 100, f"测试更新失败: {error_msg}")
            return False
    
    def update_all_data(self, start_year: int = 2000, rebuild: bool = False,
                        market: str = None, resume_checkpoint: bool = False):
        """批量更新数据

        Args:
            start_year: 起始年份
            rebuild: 是否全量重建
                - True: 全量重建，先删除当前数据源的所有数据，然后重新获取
                - False: 增量更新，只添加缺失的数据（默认）
            market: 市场类型 'A', 'HK', 'US' 或 None（全部）
        """
        try:
            # 1. 更新股票列表
            market_name = {'A': 'A股', 'HK': '港股', 'US': '美股'}.get(market, '全部市场')
            self._update_progress(0, 100, f"正在获取{market_name}股票列表...")
            stocks_df = self.fetcher.get_stock_list(market=market)
            if not stocks_df.empty:
                self.db.save_stocks(stocks_df)
                self._update_progress(10, 100, f"已获取 {len(stocks_df)} 只{market_name}股票")
            else:
                error_msg = f"{market_name}股票列表获取失败。提示：如果使用baostock数据源，需要先有其他数据源（akshare或tushare）的股票列表，或者确保已安装akshare库。"
                _logger.error(error_msg)
                self._update_progress(10, 100, error_msg)
                return False

            # 2. 如果是全量重建，先删除当前市场的所有数据
            if rebuild:
                if market:
                    market_name = {'A': 'A股', 'HK': '港股', 'US': '美股'}.get(market, '')
                    self._update_progress(10, 100, f"正在删除 {market_name} 的旧数据...")
                    deleted_count = self.db.delete_monthly_kline_by_market(market)
                    self._update_progress(10, 100, f"已删除 {deleted_count} 条旧数据，开始重新获取...")
                else:
                    # 全部市场：逐一删除各市场数据
                    self._update_progress(10, 100, "正在删除所有市场的旧数据...")
                    total_deleted = 0
                    for m in ['A', 'HK', 'US']:
                        total_deleted += self.db.delete_monthly_kline_by_market(m)
                    self._update_progress(10, 100, f"已删除 {total_deleted} 条旧数据，开始重新获取...")

            # 3. 更新月K线数据
            end_date = datetime.now().strftime('%Y%m%d')
            total_stocks = len(stocks_df)
            mode_text = "全量重建" if rebuild else "增量更新"

            # 断点续更：加载已完成的股票列表（仅增量更新支持）
            completed_codes_set: set = set()
            if rebuild:
                # 全量重建始终清除旧断点，防止残留断点污染后续增量续更
                self.clear_checkpoint()
            elif resume_checkpoint:
                cp = self.load_checkpoint()
                if cp and cp.get('market') == market:
                    completed_codes_set = set(cp.get('completed_codes', []))
                    _logger.info("Resuming from checkpoint: %d codes already done", len(completed_codes_set))
                else:
                    self.clear_checkpoint()
            else:
                self.clear_checkpoint()

            processed = len(completed_codes_set)

            # 按市场分组处理：港股/美股用批量接口，A股串行
            a_stocks = stocks_df[stocks_df['market'] == 'A'] if 'market' in stocks_df.columns else stocks_df
            hk_stocks = stocks_df[stocks_df['market'] == 'HK'] if 'market' in stocks_df.columns else pd.DataFrame()
            us_stocks = stocks_df[stocks_df['market'] == 'US'] if 'market' in stocks_df.columns else pd.DataFrame()

            # 如果market列不在stocks_df里，退回到逐一检测
            if 'market' not in stocks_df.columns:
                a_stocks = stocks_df
                hk_stocks = pd.DataFrame()
                us_stocks = pd.DataFrame()

            # --- 港股/美股：批量下载 ---
            for market_label, market_stocks, currency_code in [
                ('HK', hk_stocks, 'HKD'),
                ('US', us_stocks, 'USD'),
            ]:
                if market_stocks.empty:
                    continue

                data_source = self.fetcher.get_data_source_for_market(market_label)
                market_name_cn = {'HK': '港股', 'US': '美股'}[market_label]
                self._update_progress(
                    10 + int((processed / total_stocks) * 80), 100,
                    f"开始批量更新{market_name_cn}({len(market_stocks)}只) [{mode_text}]..."
                )

                # 当月第一天（当月没有完整月K线，跳过起始在当月的股票）
                current_month_start = datetime.now().strftime('%Y%m') + '01'

                # 全量重建：所有股票统一从 start_year 开始，一次性批量下载
                # 增量更新：每只股票按自己的最新日期决定起始，按起始日期分组
                from collections import defaultdict
                if rebuild:
                    all_codes = [c for c in market_stocks['ts_code'] if c not in completed_codes_set]
                    batch_start = f"{start_year}0101"
                    start_groups = {batch_start: all_codes} if all_codes else {}
                else:
                    code_start_map = {}
                    for _, row in market_stocks.iterrows():
                        ts_code = row['ts_code']
                        if ts_code in completed_codes_set:
                            continue  # 断点续更：跳过已完成
                        list_date = row.get('list_date')
                        if not list_date or not isinstance(list_date, str) or len(list_date) != 8:
                            list_date = f"{start_year}0101"
                        s = max(list_date, f"{start_year}0101")

                        latest = self.db.get_latest_trade_date(ts_code, data_source=data_source)
                        if latest:
                            s = (pd.to_datetime(latest, format='%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')

                        if s >= current_month_start:
                            processed += 1
                            completed_codes_set.add(ts_code)
                            continue
                        code_start_map[ts_code] = s

                    if not code_start_map:
                        processed += len(market_stocks) - len([c for c in market_stocks['ts_code'] if c in completed_codes_set])
                        continue

                    start_groups = defaultdict(list)
                    for ts_code, s in code_start_map.items():
                        start_groups[s].append(ts_code)

                CHUNK = 50
                for s, codes_batch in start_groups.items():
                    for chunk_start in range(0, len(codes_batch), CHUNK):
                        chunk = codes_batch[chunk_start: chunk_start + CHUNK]
                        progress = 10 + int((processed / total_stocks) * 80)
                        self._update_progress(progress, 100,
                            f"批量下载{market_name_cn} [{processed+1}-{processed+len(chunk)}/{total_stocks}] [{mode_text}]...")
                        try:
                            if data_source == 'yfinance':
                                batch_results = self.fetcher.get_monthly_kline_batch(chunk, s, end_date)
                            else:
                                # 非yfinance数据源（alpha_vantage/tushare等）串行逐只获取
                                batch_results = {}
                                for _tc in chunk:
                                    try:
                                        batch_results[_tc] = self.fetcher.get_monthly_kline(_tc, s, end_date)
                                    except Exception as _e2:
                                        _logger.warning("Serial fetch failed for %s: %s", _tc, _e2)
                                        batch_results[_tc] = pd.DataFrame()
                        except Exception as e:
                            _logger.error("Batch download failed for %s group starting %s: %s", market_label, s, e)
                            batch_results = {c: pd.DataFrame() for c in chunk}

                        for ts_code in chunk:
                            kline_df = batch_results.get(ts_code, pd.DataFrame())
                            saved_ok = False
                            try:
                                if not kline_df.empty:
                                    if not rebuild:
                                        prev_close = self.db.get_latest_close(ts_code, data_source=data_source)
                                    else:
                                        prev_close = None
                                    kline_df = self.fetcher.calculate_pct_chg(kline_df, prev_close=prev_close)
                                    self.db.save_monthly_kline(
                                        kline_df, data_source=data_source,
                                        market=market_label, currency=currency_code
                                    )
                                    saved_ok = True
                            except Exception as e:
                                _logger.error("Error saving %s: %s", ts_code, e)

                            processed += 1
                            # 只有成功保存数据（或数据本就为空且无异常）时才标记完成
                            # 失败的股票不进 completed_codes_set，断点续更时会重试
                            if saved_ok or kline_df.empty:
                                completed_codes_set.add(ts_code)
                            progress = 10 + int((processed / total_stocks) * 80)
                            self._update_progress(progress, 100,
                                f"批量更新{market_name_cn} {ts_code}... [{processed}/{total_stocks}] [{mode_text}]")

                        # 每个 chunk 后：保存断点（防崩溃丢进度）并检查暂停/停止
                        self.save_checkpoint(market, completed_codes_set, total_stocks)
                        if self._check_control():
                            self._update_progress(progress, 100,
                                f"⏹ 已停止 [{processed}/{total_stocks}]，断点已保存，下次更新可从此处继续")
                            return False

            # --- A股：串行更新 ---
            for idx, row in a_stocks.iterrows():
                ts_code = row['ts_code']

                # 断点续更：跳过已完成的股票
                if ts_code in completed_codes_set:
                    continue

                list_date = row.get('list_date')
                if not list_date or not isinstance(list_date, str) or len(list_date) != 8:
                    list_date = f"{start_year}0101"
                start_date = max(list_date, f"{start_year}0101")

                try:
                    if not rebuild:
                        stock_market = self.fetcher.detect_market(ts_code)
                        data_source = self.fetcher.get_data_source_for_market(stock_market)
                        latest_date = self.db.get_latest_trade_date(ts_code, data_source=data_source)
                        if latest_date:
                            start_date = (pd.to_datetime(latest_date, format='%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')

                    if start_date >= end_date:
                        processed += 1
                        completed_codes_set.add(ts_code)
                        if self._check_control():
                            self.save_checkpoint(market, completed_codes_set, total_stocks)
                            progress = 10 + int((processed / total_stocks) * 80)
                            self._update_progress(progress, 100,
                                f"⏹ 已停止 [{processed}/{total_stocks}]，断点已保存，下次更新可从此处继续")
                            return False
                        continue

                    try:
                        stock_market = self.fetcher.detect_market(ts_code)
                        data_source = self.fetcher.get_data_source_for_market(stock_market)
                        kline_df = self.fetcher.get_monthly_kline(ts_code, start_date, end_date)
                    except Exception as fetch_error:
                        error_msg = str(fetch_error)
                        error_str_lower = error_msg.lower()
                        _logger.warning("Error fetching data for %s: %s", ts_code, error_msg)
                        is_rate_limit = ('rate limit' in error_str_lower or
                                         'too many requests' in error_str_lower or
                                         'YFRateLimitError' in error_msg)
                        processed += 1
                        progress = 10 + int((processed / total_stocks) * 80)
                        if is_rate_limit:
                            self._update_progress(progress, 100,
                                f"API限制，跳过 {row['name']} ({ts_code}) [{processed}/{total_stocks}]")
                            time.sleep(2)
                        else:
                            self._update_progress(progress, 100,
                                f"获取失败 {row['name']} ({ts_code}): {error_msg[:50]}... [{processed}/{total_stocks}]")
                            time.sleep(0.5)
                        if self._check_control():
                            self.save_checkpoint(market, completed_codes_set, total_stocks)
                            self._update_progress(progress, 100,
                                f"⏹ 已停止 [{processed}/{total_stocks}]，断点已保存，下次更新可从此处继续")
                            return False
                        continue

                    if not kline_df.empty:
                        stock_market = self.fetcher.detect_market(ts_code)
                        data_source = self.fetcher.get_data_source_for_market(stock_market)
                        if not rebuild:
                            prev_close = self.db.get_latest_close(ts_code, data_source=data_source)
                        else:
                            prev_close = None
                        kline_df = self.fetcher.calculate_pct_chg(kline_df, prev_close=prev_close)
                        currency_map = {'A': 'CNY', 'HK': 'HKD', 'US': 'USD'}
                        currency = currency_map.get(stock_market, 'CNY')
                        self.db.save_monthly_kline(kline_df, data_source=data_source,
                                                   market=stock_market, currency=currency)

                    processed += 1
                    completed_codes_set.add(ts_code)
                    progress = 10 + int((processed / total_stocks) * 80)
                    self._update_progress(progress, 100,
                        f"更新 {row['name']} ({ts_code})... [{processed}/{total_stocks}] [{mode_text}]")

                    # 每 100 只保存一次断点
                    if processed % 100 == 0:
                        self.save_checkpoint(market, completed_codes_set, total_stocks)

                    if data_source == 'akshare':
                        time.sleep(1.0)
                    else:
                        time.sleep(0.2)
                except Exception as e:
                    error_msg = str(e)
                    _logger.error("Error updating %s: %s", ts_code, error_msg, exc_info=True)
                    processed += 1
                    # 出错的股票不加入 completed_codes_set，下次断点续更可以重试
                    progress = 10 + int((processed / total_stocks) * 80)
                    self._update_progress(progress, 100,
                        f"更新出错 {row['name']} ({ts_code}): {error_msg[:50]}... [{processed}/{total_stocks}]")
                    continue

                # 每只股票处理完后检查暂停/停止
                if self._check_control():
                    self.save_checkpoint(market, completed_codes_set, total_stocks)
                    self._update_progress(progress, 100,
                        f"⏹ 已停止 [{processed}/{total_stocks}]，断点已保存，下次更新可从此处继续")
                    return False

            # 4. 更新行业分类（限定与K线更新相同的市场范围）
            self._update_progress(90, 100, "正在更新行业分类...")
            self._update_industry_classification(market=market)

            self.clear_checkpoint()
            mode_text = "全量重建" if rebuild else "增量更新"
            self._update_progress(100, 100, f"数据更新完成！[{mode_text}]")
            return True

        except Exception as e:
            error_msg = str(e)
            _logger.error("Error in update_all_data: %s", error_msg, exc_info=True)
            self._update_progress(100, 100, f"数据更新失败: {error_msg}")
            return False
    
    def update_industry_only(self, market: str = None) -> bool:
        """独立更新行业分类（不更新K线数据）

        Args:
            market: 'A'/'HK'/'US' 或 None（全部）
        """
        try:
            market_name = {'A': 'A股', 'HK': '港股', 'US': '美股'}.get(market, '全部市场')
            self._update_progress(0, 100, f"开始更新{market_name}行业分类...")
            self._update_industry_classification(market=market)
            self._update_progress(100, 100, f"{market_name}行业分类更新完成")
            return True
        except Exception as e:
            self._update_progress(100, 100, f"行业分类更新失败: {e}")
            return False

    def _update_industry_classification(self, market: str = None):
        """更新行业分类

        Args:
            market: 限定市场 'A'/'HK'/'US'，None 表示全部
        """
        try:
            # 从股票基本信息中获取行业分类（stocks 表的 industry 字段）
            if market in (None, 'A', 'HK', 'US'):
                stocks_df = self.db.get_stocks(exclude_delisted=True, market=market)
            else:
                stocks_df = self.db.get_stocks(exclude_delisted=True)

            self._update_progress(5, 100, "正在从股票基本信息写入行业分类...")
            records = [
                (row['ts_code'], row['industry'], 'L1', '', row.get('market', ''))
                for _, row in stocks_df.iterrows()
                if pd.notna(row.get('industry')) and row['industry']
            ]
            self.db.save_industry_batch(records, 'sw')

            if self._check_control():
                return

            # A股：尝试通过 tushare 获取更详细的申万/中信分类
            if market in (None, 'A'):
                self._update_progress(20, 100, "正在获取A股申万行业分类...")
                try:
                    sw_industries = self.fetcher.get_industry_classification('sw')
                    if sw_industries:
                        sw_records = [
                            (ts_code, industry_name, 'L1', '', 'A')
                            for industry_name, stock_codes in sw_industries.items()
                            for ts_code in stock_codes
                        ]
                        self.db.save_industry_batch(sw_records, 'sw')
                except Exception:
                    pass

                if self._check_control():
                    return

                self._update_progress(40, 100, "正在获取A股中信行业分类...")
                try:
                    citics_industries = self.fetcher.get_industry_classification('citics')
                    if citics_industries:
                        citics_records = [
                            (ts_code, industry_name, 'L1', '', 'A')
                            for industry_name, stock_codes in citics_industries.items()
                            for ts_code in stock_codes
                        ]
                        self.db.save_industry_batch(citics_records, 'citics')
                except Exception:
                    pass

                if self._check_control():
                    return

            # 港股：通过 yfinance 获取行业分类
            if market in (None, 'HK'):
                hk_source = self.fetcher.get_data_source_for_market('HK')
                if hk_source == 'yfinance':
                    try:
                        hk_stocks = self.db.get_stocks(exclude_delisted=True, market='HK')
                        if not hk_stocks.empty:
                            total_hk = len(hk_stocks)
                            self._update_progress(50, 100, f"正在通过yfinance获取 {total_hk} 只港股行业分类...")

                            def hk_progress(c, t, m):
                                pct = 50 + int(c / t * 25) if t > 0 else 50
                                self._update_progress(pct, 100, m)

                            hk_industries = self.fetcher.get_industry_by_yfinance(
                                hk_stocks['ts_code'].tolist(), 'HK',
                                progress_callback=hk_progress,
                                stop_check=self._check_control
                            )
                            if self._check_control():
                                return
                            hk_records = [
                                (ts_code, industry_name, 'L1', '', 'HK')
                                for industry_name, stock_codes in hk_industries.items()
                                for ts_code in stock_codes
                            ]
                            self.db.save_industry_batch(hk_records, 'sw')
                            self._update_progress(75, 100, f"港股行业分类完成，共 {len(hk_industries)} 个行业")
                    except Exception as e:
                        _logger.error("港股行业分类更新失败: %s", e)

            if self._check_control():
                return

            # 美股：通过 yfinance 获取行业分类
            if market in (None, 'US'):
                us_source = self.fetcher.get_data_source_for_market('US')
                if us_source == 'yfinance':
                    try:
                        us_stocks = self.db.get_stocks(exclude_delisted=True, market='US')
                        if not us_stocks.empty:
                            total_us = len(us_stocks)
                            self._update_progress(75, 100, f"正在通过yfinance获取 {total_us} 只美股行业分类...")

                            def us_progress(c, t, m):
                                pct = 75 + int(c / t * 20) if t > 0 else 75
                                self._update_progress(pct, 100, m)

                            us_industries = self.fetcher.get_industry_by_yfinance(
                                us_stocks['ts_code'].tolist(), 'US',
                                progress_callback=us_progress,
                                stop_check=self._check_control
                            )
                            if self._check_control():
                                return
                            us_records = [
                                (ts_code, industry_name, 'L1', '', 'US')
                                for industry_name, stock_codes in us_industries.items()
                                for ts_code in stock_codes
                            ]
                            self.db.save_industry_batch(us_records, 'sw')
                            self._update_progress(95, 100, f"美股行业分类完成，共 {len(us_industries)} 个行业")
                    except Exception as e:
                        _logger.error("美股行业分类更新失败: %s", e)

        except Exception as e:
            _logger.error("Error updating industry classification: %s", e, exc_info=True)

