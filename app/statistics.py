"""
统计计算模块
"""
import pandas as pd
from typing import Dict, List, Optional, Tuple
from app.database import Database
from app.data_cleaner import filter_relisting_months


class Statistics:
    def __init__(self, db: Database):
        self.db = db
        self._fetcher = None  # 懒加载，避免每次调用都重建

    def _get_fetcher(self):
        if self._fetcher is None:
            from app.data_fetcher import DataFetcher
            from app.config import Config
            self._fetcher = DataFetcher(Config())
        return self._fetcher
    
    def calculate_stock_month_statistics(self, ts_code: str, month: int,
                                        start_year: int = None, end_year: int = None,
                                        data_source: str = None,
                                        exclude_relisting: bool = False) -> Dict:
        """
        计算单只股票在指定月份的历史统计
        
        Args:
            ts_code: 股票代码
            month: 月份（1-12）
            start_year: 起始年份
            end_year: 结束年份
            data_source: 数据源（可选，如果不指定则使用配置的数据源）
        
        Returns:
            统计结果字典
        """
        if data_source is None:
            fetcher = self._get_fetcher()
            stock_market = fetcher.detect_market(ts_code)
            data_source = fetcher.get_data_source_for_market(stock_market)

        # 使用指定数据源查询，fallback 到全部数据源并记录实际来源
        df = self.db.get_monthly_kline(ts_code=ts_code, month=month,
                                      start_year=start_year, end_year=end_year,
                                      data_source=data_source)
        actual_data_source = data_source
        if df.empty and data_source:
            df = self.db.get_monthly_kline(ts_code=ts_code, month=month,
                                          start_year=start_year, end_year=end_year)
            actual_data_source = 'mixed'  # 实际使用了多数据源混合

        _empty = {
            'ts_code': ts_code, 'month': month,
            'total_count': 0, 'up_count': 0, 'down_count': 0, 'flat_count': 0,
            'avg_up_pct': 0, 'avg_down_pct': 0,
            'up_probability': 0, 'down_probability': 0, 'flat_probability': 0,
            'actual_data_source': actual_data_source,
        }
        if df.empty:
            return _empty

        df = df[df['pct_chg'].notna()]
        if df.empty:
            return _empty

        if exclude_relisting:
            df = filter_relisting_months(df)
            if df.empty:
                return _empty

        up_df   = df[df['pct_chg'] > 0]
        down_df = df[df['pct_chg'] < 0]
        flat_df = df[df['pct_chg'] == 0]

        total_count = len(df)
        up_count    = len(up_df)
        down_count  = len(down_df)
        flat_count  = len(flat_df)

        avg_up_pct   = up_df['pct_chg'].mean()         if up_count   > 0 else 0
        avg_down_pct = abs(down_df['pct_chg'].mean())  if down_count > 0 else 0

        up_probability   = up_count   / total_count * 100
        down_probability = down_count / total_count * 100
        flat_probability = flat_count / total_count * 100

        return {
            'ts_code': ts_code,
            'month': month,
            'total_count': total_count,
            'up_count': up_count,
            'down_count': down_count,
            'flat_count': flat_count,
            'avg_up_pct': round(avg_up_pct, 2),
            'avg_down_pct': round(avg_down_pct, 2),
            'up_probability': round(up_probability, 2),
            'down_probability': round(down_probability, 2),
            'flat_probability': round(flat_probability, 2),
            'actual_data_source': actual_data_source,
        }
    
    def calculate_month_filter_statistics(self, month: int, start_year: int,
                                         end_year: int, top_n: int = 20,
                                         data_source: str = None, min_count: int = 0,
                                         market: str = None,
                                         exclude_relisting: bool = False) -> List[Dict]:
        """
        计算月榜单（按上涨概率排序前N支）
        优化：一次批量查询替代N次单股查询
        """
        # 批量获取K线数据（一次查询）
        kline_df = self.db.get_kline_bulk_for_month(month, start_year, end_year, data_source)
        # 若指定数据源无数据则兜底查全部源
        if kline_df.empty and data_source:
            kline_df = self.db.get_kline_bulk_for_month(month, start_year, end_year)

        kline_df = kline_df[kline_df['pct_chg'].notna()]
        if kline_df.empty:
            return []

        if exclude_relisting:
            kline_df = filter_relisting_months(kline_df)
            if kline_df.empty:
                return []

        # 股票基础信息（一次查询）
        stocks_df = self.db.get_stocks(exclude_delisted=True, market=market)
        if market:
            valid_codes = set(stocks_df['ts_code'])
            kline_df = kline_df[kline_df['ts_code'].isin(valid_codes)]

        stock_info = stocks_df.set_index('ts_code')

        results = []
        for ts_code, group in kline_df.groupby('ts_code'):
            if ts_code not in stock_info.index:
                continue
            up_rows   = group[group['pct_chg'] > 0]
            down_rows = group[group['pct_chg'] < 0]
            flat_rows = group[group['pct_chg'] == 0]
            total      = len(group)
            up_count   = len(up_rows)
            down_count = len(down_rows)
            flat_count = len(flat_rows)
            if total == 0:
                continue
            if min_count > 0 and total < min_count:  # 用 total 而非 up+down，平盘也算有效记录
                continue
            row = stock_info.loc[ts_code]
            ds_series = group['data_source'].dropna()
            results.append({
                'ts_code': ts_code,
                'symbol': row.get('symbol', ts_code),
                'name': row.get('name', ''),
                'month': month,
                'total_count': total,
                'up_count': up_count,
                'down_count': down_count,
                'flat_count': flat_count,
                'avg_up_pct': round(float(up_rows['pct_chg'].mean()), 2) if up_count > 0 else 0,
                'avg_down_pct': round(float(abs(down_rows['pct_chg'].mean())), 2) if down_count > 0 else 0,
                'up_probability': round(up_count / total * 100, 2),
                'down_probability': round(down_count / total * 100, 2),
                'flat_probability': round(flat_count / total * 100, 2),
                'data_source': ds_series.mode().iloc[0] if not ds_series.empty else (data_source or '')
            })

        results.sort(key=lambda x: x['up_probability'], reverse=True)
        return results[:top_n]
    
    def calculate_industry_statistics(self, month: int, start_year: int, end_year: int,
                                     industry_type: str = 'sw', data_source: str = None,
                                     market: str = None,
                                     exclude_relisting: bool = False) -> List[Dict]:
        """
        计算行业统计（各行业在指定月份的上涨概率）

        Args:
            month: 月份（1-12）
            start_year: 起始年份
            end_year: 结束年份
            industry_type: 行业分类类型（sw/citics）
            data_source: 数据源（可选，如果不指定则使用配置的数据源）
            market: 市场筛选（A/HK/US，None表示全部）

        Returns:
            行业统计列表（按上涨概率降序）
        """
        # 批量获取K线数据（一次查询）
        kline_df = self.db.get_kline_bulk_for_month(month, start_year, end_year, data_source)
        if kline_df.empty and data_source:
            kline_df = self.db.get_kline_bulk_for_month(month, start_year, end_year)

        kline_df = kline_df[kline_df['pct_chg'].notna()]
        if kline_df.empty:
            return []

        if exclude_relisting:
            kline_df = filter_relisting_months(kline_df)
            if kline_df.empty:
                return []

        # 市场筛选
        if market:
            market_stocks = set(self.db.get_stocks(exclude_delisted=True, market=market)['ts_code'])
            kline_df = kline_df[kline_df['ts_code'].isin(market_stocks)]

        # 行业股票映射（一次查询，按市场过滤）
        industry_map = self.db.get_all_industry_stock_mapping(industry_type, market=market)
        if industry_map.empty:
            return []

        # 合并K线与行业映射，在内存中聚合
        merged = kline_df.merge(industry_map, on='ts_code', how='inner')
        if merged.empty:
            return []

        results = []
        for industry_name, group in merged.groupby('industry_name'):
            up_rows = group[group['pct_chg'] > 0]
            down_rows = group[group['pct_chg'] < 0]
            flat_rows = group[group['pct_chg'] == 0]
            total = len(group)
            up_count = len(up_rows)
            down_count = len(down_rows)
            flat_count = len(flat_rows)
            if total == 0:
                continue
            results.append({
                'industry_name': industry_name,
                'stock_count': int(group['ts_code'].nunique()),
                'total_count': total,
                'up_count': up_count,
                'down_count': down_count,
                'flat_count': flat_count,
                'avg_up_pct': round(float(up_rows['pct_chg'].mean()), 2) if up_count > 0 else 0,
                'avg_down_pct': round(float(abs(down_rows['pct_chg'].mean())), 2) if down_count > 0 else 0,
                'up_probability': round(up_count / total * 100, 2),
                'down_probability': round(down_count / total * 100, 2),
                'flat_probability': round(flat_count / total * 100, 2),
            })

        results.sort(key=lambda x: x['up_probability'], reverse=True)
        return results

    def calculate_industry_enhanced_stats(self, month: int, start_year: int, end_year: int,
                                          industry_type: str = 'sw', data_source: str = None,
                                          market: str = None,
                                          exclude_relisting: bool = False) -> List[Dict]:
        """
        行业增强统计：以"年"为单位聚合行业平均涨跌，计算期望收益率、跑赢大盘概率、近5年一致性。

        Returns:
            每个行业的增强统计列表，默认按期望收益率降序
        """
        kline_df = self.db.get_kline_bulk_for_month(month, start_year, end_year, data_source)
        if kline_df.empty and data_source:
            kline_df = self.db.get_kline_bulk_for_month(month, start_year, end_year)

        kline_df = kline_df[kline_df['pct_chg'].notna()]
        if kline_df.empty:
            return []

        if exclude_relisting:
            kline_df = filter_relisting_months(kline_df)
            if kline_df.empty:
                return []

        if market:
            market_stocks = set(self.db.get_stocks(exclude_delisted=True, market=market)['ts_code'])
            kline_df = kline_df[kline_df['ts_code'].isin(market_stocks)]

        industry_map = self.db.get_all_industry_stock_mapping(industry_type, market=market)
        if industry_map.empty:
            return []

        merged = kline_df.merge(industry_map, on='ts_code', how='inner')
        if merged.empty:
            return []

        # 每年大盘平均涨幅（等权）作为基准
        market_avg_by_year = kline_df.groupby('year')['pct_chg'].mean()

        recent_start = end_year - 4  # 近5年：end_year-4 ~ end_year

        results = []
        for industry_name, group in merged.groupby('industry_name'):
            # 每年行业平均涨幅
            year_avg = group.groupby('year')['pct_chg'].mean()
            if year_avg.empty:
                continue

            rets = year_avg.values.tolist()
            years = year_avg.index.tolist()
            total_years = len(rets)

            up_rets   = [r for r in rets if r > 0]
            down_rets = [r for r in rets if r < 0]   # 平盘年份不计入下跌

            up_probability  = round(len(up_rets) / total_years * 100, 2)
            avg_up_return   = round(sum(up_rets)   / len(up_rets),   2) if up_rets   else 0.0
            avg_down_return = round(sum(down_rets) / len(down_rets), 2) if down_rets else 0.0
            expected_return = round(sum(rets) / total_years, 2)

            # 跑赢大盘：该年行业均涨 > 大盘均涨
            beat = sum(
                1 for yr, r in zip(years, rets)
                if yr in market_avg_by_year.index and r > market_avg_by_year[yr]
            )
            excess_market_prob = round(beat / total_years * 100, 2)

            # 近5年上涨率
            recent_rets = [r for yr, r in zip(years, rets) if yr >= recent_start]
            if recent_rets:
                recent_up_prob = round(
                    sum(1 for r in recent_rets if r > 0) / len(recent_rets) * 100, 2
                )
                # 一致性：近5年 vs 全历史偏差越小越稳定（100=完全一致）
                consistency = round(100 - abs(recent_up_prob - up_probability), 2)
            else:
                recent_up_prob = None
                consistency = None

            results.append({
                'industry_name':    industry_name,
                'stock_count':      int(group['ts_code'].nunique()),
                'total_years':      total_years,
                'expected_return':  expected_return,
                'up_probability':   up_probability,
                'avg_up_return':    avg_up_return,
                'avg_down_return':  avg_down_return,
                'excess_market_prob': excess_market_prob,
                'recent_up_prob':   recent_up_prob,
                'consistency':      consistency,
            })

        results.sort(key=lambda x: x['expected_return'], reverse=True)
        return results

    def calculate_month_filter_enhanced_stats(self, month: int, start_year: int,
                                               end_year: int, top_n: int = 50,
                                               data_source: str = None, min_years: int = 3,
                                               market: str = None,
                                               exclude_relisting: bool = False) -> List[Dict]:
        """
        月榜单增强统计：以"年"为单位聚合个股表现，计算期望收益率、跑赢大盘概率、近5年一致性。

        Returns:
            每只股票的增强统计列表，默认按期望收益率降序，top_n<=0 返回全部
        """
        kline_df = self.db.get_kline_bulk_for_month(month, start_year, end_year, data_source)
        if kline_df.empty and data_source:
            kline_df = self.db.get_kline_bulk_for_month(month, start_year, end_year)

        kline_df = kline_df[kline_df['pct_chg'].notna()]
        if kline_df.empty:
            return []

        if exclude_relisting:
            kline_df = filter_relisting_months(kline_df)
            if kline_df.empty:
                return []

        stocks_df = self.db.get_stocks(exclude_delisted=True, market=market)
        if market:
            valid_codes = set(stocks_df['ts_code'])
            kline_df = kline_df[kline_df['ts_code'].isin(valid_codes)]

        stock_info = stocks_df.set_index('ts_code')

        # 每年大盘等权均值作为基准
        market_avg_by_year = kline_df.groupby('year')['pct_chg'].mean()
        recent_start = end_year - 4  # 近5年

        results = []
        for ts_code, group in kline_df.groupby('ts_code'):
            if ts_code not in stock_info.index:
                continue

            year_ret = group.groupby('year')['pct_chg'].mean()
            rets  = year_ret.values.tolist()
            years = year_ret.index.tolist()
            total_years = len(rets)

            if total_years < min_years:
                continue

            up_rets   = [r for r in rets if r > 0]
            down_rets = [r for r in rets if r < 0]   # 平盘年份不计入下跌

            up_probability  = round(len(up_rets) / total_years * 100, 2)
            avg_up_return   = round(sum(up_rets)   / len(up_rets),   2) if up_rets   else 0.0
            avg_down_return = round(sum(down_rets) / len(down_rets), 2) if down_rets else 0.0
            expected_return = round(sum(rets) / total_years, 2)
            max_up   = round(max(rets), 2)
            max_down = round(min(rets), 2)

            beat = sum(
                1 for yr, r in zip(years, rets)
                if yr in market_avg_by_year.index and r > market_avg_by_year[yr]
            )
            excess_market_prob = round(beat / total_years * 100, 2)

            recent_rets = [r for yr, r in zip(years, rets) if yr >= recent_start]
            if recent_rets:
                recent_up_prob = round(
                    sum(1 for r in recent_rets if r > 0) / len(recent_rets) * 100, 2
                )
                consistency = round(100 - abs(recent_up_prob - up_probability), 2)
            else:
                recent_up_prob = None
                consistency = None

            row = stock_info.loc[ts_code]
            results.append({
                'ts_code':          ts_code,
                'symbol':           row.get('symbol', ts_code),
                'name':             row.get('name', ''),
                'total_years':      total_years,
                'expected_return':  expected_return,
                'up_probability':   up_probability,
                'avg_up_return':    avg_up_return,
                'avg_down_return':  avg_down_return,
                'max_up':           max_up,
                'max_down':         max_down,
                'excess_market_prob': excess_market_prob,
                'recent_up_prob':   recent_up_prob,
                'consistency':      consistency,
            })

        results.sort(key=lambda x: x['expected_return'], reverse=True)
        return results[:top_n] if top_n > 0 else results

    def calculate_industry_top_stocks(self, industry_name: str, month: int,
                                     start_year: int, end_year: int,
                                     industry_type: str = 'sw', top_n: int = 20,
                                     data_source: str = None, market: str = None,
                                     exclude_relisting: bool = False) -> List[Dict]:
        """
        计算行业中上涨概率最高的前N支股票

        Args:
            industry_name: 行业名称
            month: 月份（1-12）
            start_year: 起始年份
            end_year: 结束年份
            industry_type: 行业分类类型（sw/citics）
            top_n: 返回前N支股票
            data_source: 数据源（可选，如果不指定则使用配置的数据源）
            market: 市场筛选（A/HK/US，None表示全部）

        Returns:
            股票统计列表（按上涨概率降序）
        """
        # 获取行业下的股票（按市场过滤）
        stock_codes = self.db.get_industry_stocks(industry_name, industry_type, market=market)
        if not stock_codes:
            return []

        # 股票信息（带市场筛选）
        stocks_df = self.db.get_stocks(exclude_delisted=True, market=market)
        if market:
            valid_codes = set(stocks_df['ts_code'])
            stock_codes = [c for c in stock_codes if c in valid_codes]
        if not stock_codes:
            return []

        # 批量获取该行业股票的K线数据（一次查询）
        kline_df = self.db.get_kline_bulk_for_month(month, start_year, end_year,
                                                     data_source, stock_codes=stock_codes)
        if kline_df.empty and data_source:
            kline_df = self.db.get_kline_bulk_for_month(month, start_year, end_year,
                                                         stock_codes=stock_codes)

        kline_df = kline_df[kline_df['pct_chg'].notna()]
        if kline_df.empty:
            return []

        if exclude_relisting:
            kline_df = filter_relisting_months(kline_df)
            if kline_df.empty:
                return []

        stock_info = stocks_df.set_index('ts_code')

        results = []
        for ts_code, group in kline_df.groupby('ts_code'):
            if ts_code not in stock_info.index:
                continue
            up_rows   = group[group['pct_chg'] > 0]
            down_rows = group[group['pct_chg'] < 0]
            flat_rows = group[group['pct_chg'] == 0]
            total      = len(group)
            up_count   = len(up_rows)
            down_count = len(down_rows)
            flat_count = len(flat_rows)
            if total == 0:
                continue
            row = stock_info.loc[ts_code]
            ds_series = group['data_source'].dropna()
            results.append({
                'ts_code': ts_code,
                'symbol': row.get('symbol', ts_code),
                'name': row.get('name', ''),
                'month': month,
                'total_count': total,
                'up_count': up_count,
                'down_count': down_count,
                'flat_count': flat_count,
                'avg_up_pct': round(float(up_rows['pct_chg'].mean()), 2) if up_count > 0 else 0,
                'avg_down_pct': round(float(abs(down_rows['pct_chg'].mean())), 2) if down_count > 0 else 0,
                'up_probability': round(up_count / total * 100, 2),
                'down_probability': round(down_count / total * 100, 2),
                'flat_probability': round(flat_count / total * 100, 2),
                'data_source': ds_series.mode().iloc[0] if not ds_series.empty else (data_source or '')
            })

        results.sort(key=lambda x: x['up_probability'], reverse=True)
        return results[:top_n]

