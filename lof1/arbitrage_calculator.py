# -*- coding: utf-8 -*-
"""
LOF基金套利计算模块
计算套利机会和收益
"""

from typing import Dict, Optional
from config import TRADE_FEES, ARBITRAGE_THRESHOLD

class ArbitrageCalculator:
    """LOF基金套利计算器"""
    
    def __init__(self, threshold=None, fees=None):
        """
        初始化计算器
        
        Args:
            threshold: 套利阈值配置，如果为None则使用默认值
            fees: 交易费用配置，如果为None则使用默认值
        """
        self.fees = fees if fees else TRADE_FEES
        self.threshold = threshold if threshold else ARBITRAGE_THRESHOLD
    
    def calculate_arbitrage(self, fund_info: Dict) -> Optional[Dict]:
        """
        计算套利机会
        
        Args:
            fund_info: 包含price和nav的基金信息
            
        Returns:
            套利分析结果，如果没有套利机会返回None
        """
        if not fund_info:
            return None
        
        price = fund_info.get('price', 0)
        nav = fund_info.get('nav', 0)
        price_missing = fund_info.get('price_missing', False)
        nav_missing = fund_info.get('nav_missing', False)
        
        # 如果价格和净值都缺失，返回None
        if price <= 0 and nav <= 0:
            return None
        
        # 如果只有价格没有净值，返回基础数据
        if price > 0 and nav <= 0:
            return {
                'fund_code': fund_info.get('code', ''),
                'price': price,
                'nav': 0,
                'nav_date': fund_info.get('nav_date', ''),  # 净值日期
                'price_diff': 0,
                'price_diff_pct': 0,
                'change_pct': round(fund_info.get('change_pct', 0) * 100, 2),  # 日内涨幅（百分比）
                'arbitrage_type': '暂无净值数据',
                'operation': '净值数据缺失',
                'total_cost_rate': 0,
                'profit_rate': 0,
                'net_profit_10k': 0,
                'has_opportunity': False,
                'update_time': fund_info.get('update_time', ''),
                'nav_missing': True
            }
        
        # 如果价格缺失（price=0且标记了price_missing），返回基础数据
        if price <= 0 and nav > 0:
            if price_missing:
                return {
                    'fund_code': fund_info.get('code', ''),
                    'price': 0,
                    'nav': nav,
                    'nav_date': fund_info.get('nav_date', ''),  # 净值日期
                    'price_diff': 0,
                    'price_diff_pct': 0,
                    'change_pct': round(fund_info.get('change_pct', 0) * 100, 2),  # 日内涨幅（百分比）
                    'arbitrage_type': '暂无价格数据',
                    'operation': '价格数据缺失',
                    'total_cost_rate': 0,
                    'profit_rate': 0,
                    'net_profit_10k': 0,
                    'has_opportunity': False,
                    'update_time': fund_info.get('update_time', ''),
                    'price_missing': True
                }
            else:
                return None
        
        # 如果价格和净值都有效，继续计算套利
        if price <= 0 or nav <= 0:
            return None
        
        # 计算价差
        price_diff = price - nav
        price_diff_pct = (price_diff / nav) * 100
        
        # 判断套利方向
        is_premium = price > nav  # 溢价：场内价格 > 净值
        
        # 计算套利成本和收益
        if is_premium:
            # 溢价套利：场外申购（按净值）→ 场内卖出（按价格）
            # 操作顺序：1. 在场外申购基金份额（净值nav），2. 转到场内卖出（价格price）
            subscribe_cost = self.fees['subscribe_fee']
            sell_cost = self.fees['sell_commission'] + self.fees['stamp_tax']
            total_cost = subscribe_cost + sell_cost
            
            # 假设投入10000元计算
            investment = 10000
            # 1. 在场外申购：投入investment，扣除申购费后，按净值申购得份额
            subscribe_shares = investment * (1 - subscribe_cost) / nav
            # 2. 在场内卖出：持有份额，按价格卖出，扣除卖出费用
            final_value = subscribe_shares * price * (1 - sell_cost)
            # 净收益
            net_profit = final_value - investment
            profit_rate = (net_profit / investment) * 100
            
            
            arbitrage_type = '溢价套利'
            operation = '场外申购 → 场内卖出'
        else:
            # 折价套利：场内买入（按价格）→ 场外赎回（按净值）
            # 操作顺序：1. 在场内买入基金份额（价格price），2. 转到场外赎回（净值nav）
            redeem_cost = self.fees['redeem_fee']
            buy_cost = self.fees['buy_commission']
            total_cost = redeem_cost + buy_cost
            
            # 假设投入10000元计算
            investment = 10000
            # 1. 在场内买入：投入investment，扣除买入费用后，按价格买入得份额
            buy_shares = investment * (1 - buy_cost) / price
            # 2. 在场外赎回：持有份额，按净值赎回，扣除赎回费用
            final_value = buy_shares * nav * (1 - redeem_cost)
            # 净收益
            net_profit = final_value - investment
            profit_rate = (net_profit / investment) * 100
            
            
            arbitrage_type = '折价套利'
            operation = '场内买入 → 场外赎回'
        
        # 判断是否有套利机会（只根据收益率判断，不检查价差）
        # profit_rate 已经是百分比数值（例如 0.5 表示 0.5%）
        # min_profit_rate 是小数（例如 0.005 表示 0.5%），需要乘以 100 转换为百分比数值
        # 对于折价套利和溢价套利，都使用相同的阈值判断逻辑
        min_profit_rate_percent = self.threshold.get('min_profit_rate', 0.005) * 100
        has_opportunity = profit_rate >= min_profit_rate_percent
        
        # T+N 时间成本估算
        # 溢价套利：申购确认 T+1，到账 T+2，场内转场外约 3 个交易日
        # 折价套利：买入 T+1 确认，转场外赎回需 7-10 个交易日（含赎回到账）
        holding_days = 3 if is_premium else 10
        # 年化收益率 = 单次收益率 / 持仓天数 × 252（年化交易日）
        annualized_rate = round((profit_rate / 100) / holding_days * 252 * 100, 1)

        return {
            'fund_code': fund_info.get('code', ''),
            'price': price,
            'nav': nav,
            'nav_date': fund_info.get('nav_date', ''),  # 净值日期
            'price_diff': round(price_diff, 4),
            'price_diff_pct': round(price_diff_pct, 2),
            'change_pct': round(fund_info.get('change_pct', 0) * 100, 2),  # 日内涨幅（百分比）
            'arbitrage_type': arbitrage_type,
            'operation': operation,
            'total_cost_rate': round(total_cost * 100, 2),
            'profit_rate': round(profit_rate, 2),
            'net_profit_10k': round(net_profit, 2),  # 投入1万元的净收益
            'has_opportunity': has_opportunity,
            'holding_days': holding_days,           # 预计持仓交易日数
            'annualized_rate': annualized_rate,     # 年化收益率（%）
            'update_time': fund_info.get('update_time', ''),
        }
    
    def calculate_batch(self, funds_info: list) -> list:
        """
        批量计算套利机会
        
        Args:
            funds_info: 基金信息列表
            
        Returns:
            套利分析结果列表
        """
        results = []
        for fund_info in funds_info:
            result = self.calculate_arbitrage(fund_info)
            if result:
                results.append(result)
        return results
    
    def filter_opportunities(self, results: list) -> list:
        """
        过滤出有套利机会的结果
        
        Args:
            results: 套利分析结果列表
            
        Returns:
            有套利机会的结果列表
        """
        return [r for r in results if r.get('has_opportunity', False)]
    
    def sort_by_profit(self, results: list, reverse: bool = True) -> list:
        """
        按收益率排序

        Args:
            results: 套利分析结果列表
            reverse: 是否降序排列

        Returns:
            排序后的结果列表
        """
        return sorted(results, key=lambda x: x.get('profit_rate', 0), reverse=reverse)

    def calculate_with_custom_fees(self, fund_info: Dict, custom_fees: Dict,
                                    amount: float = 10000) -> Optional[Dict]:
        """
        使用自定义费率计算套利收益，并与默认费率对比

        Args:
            fund_info: 包含 price 和 nav 的基金信息
            custom_fees: 自定义费率字典（仅传需要覆盖的字段）
                         例：{"subscribe_fee": 0.001, "buy_commission": 0.00025}
            amount: 模拟投入金额（元），默认 10000

        Returns:
            包含自定义费率结果和默认费率对比的字典，失败返回 None
        """
        if not fund_info:
            return None

        price = fund_info.get('price', 0)
        nav = fund_info.get('nav', 0)
        if price <= 0 or nav <= 0:
            return None

        def _calc(fees: Dict, inv: float) -> Optional[Dict]:
            """内部：用给定费率和金额计算一次"""
            is_premium = price > nav
            if is_premium:
                sub_fee = fees.get('subscribe_fee', TRADE_FEES['subscribe_fee'])
                sell_cost = fees.get('sell_commission', TRADE_FEES['sell_commission']) + \
                            fees.get('stamp_tax', TRADE_FEES['stamp_tax'])
                total_cost = sub_fee + sell_cost
                shares = inv * (1 - sub_fee) / nav
                final_val = shares * price * (1 - sell_cost)
            else:
                buy_cost = fees.get('buy_commission', TRADE_FEES['buy_commission'])
                redeem_fee = fees.get('redeem_fee', TRADE_FEES['redeem_fee'])
                total_cost = buy_cost + redeem_fee
                shares = inv * (1 - buy_cost) / price
                final_val = shares * nav * (1 - redeem_fee)

            net_profit = final_val - inv
            profit_rate = (net_profit / inv) * 100
            holding_days = 3 if is_premium else 10
            annualized_rate = round((profit_rate / 100) / holding_days * 252 * 100, 1)
            return {
                'profit': round(net_profit, 2),
                'profit_rate': round(profit_rate, 2),
                'total_cost_rate': round(total_cost * 100, 2),
                'final_value': round(final_val, 2),
                'holding_days': holding_days,
                'annualized_rate': annualized_rate,
            }

        # 合并自定义费率（只覆盖传入的字段）
        merged_fees = dict(self.fees)
        merged_fees.update({k: v for k, v in custom_fees.items() if v is not None})

        custom_result = _calc(merged_fees, amount)
        default_result = _calc(self.fees, amount)

        if custom_result is None or default_result is None:
            return None

        is_premium = price > nav
        return {
            'fund_code': fund_info.get('code', ''),
            'price': price,
            'nav': nav,
            'nav_date': fund_info.get('nav_date', ''),
            'price_diff_pct': round((price - nav) / nav * 100, 2),
            'arbitrage_type': 'premium' if is_premium else 'discount',
            'arbitrage_type_cn': '溢价套利' if is_premium else '折价套利',
            'operation': ('场外申购 → 场内卖出' if is_premium else '场内买入 → 场外赎回'),
            'amount': amount,
            'custom_fees': merged_fees,
            'result': custom_result,
            'default_result': default_result,
            'fee_saving': round(custom_result['profit'] - default_result['profit'], 2),
        }
