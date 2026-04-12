# -*- coding: utf-8 -*-
"""
LOF基金套利工具主程序
"""

import time
import sys
from typing import List, Dict
from data_fetcher import LOFDataFetcher, MockDataFetcher
from arbitrage_calculator import ArbitrageCalculator
from config import LOF_FUNDS, DATA_SOURCE


class LOFArbitrageTool:
    """LOF基金套利工具主类"""
    
    def __init__(self, use_mock_data: bool = False):
        """
        初始化工具
        
        Args:
            use_mock_data: 是否使用模拟数据（用于测试）
        """
        if use_mock_data:
            self.data_fetcher = MockDataFetcher()
            print("使用模拟数据模式")
        else:
            self.data_fetcher = LOFDataFetcher()
        self.calculator = ArbitrageCalculator()
        self.funds = LOF_FUNDS
    
    def monitor_single_fund(self, fund_code: str, fund_name: str = ''):
        """
        监控单个基金的套利机会
        
        Args:
            fund_code: 基金代码
            fund_name: 基金名称
        """
        print(f"\n{'='*60}")
        print(f"监控基金: {fund_code} {fund_name}")
        print(f"{'='*60}")
        
        fund_info = self.data_fetcher.get_fund_info(fund_code)
        if not fund_info:
            print(f"❌ 无法获取基金 {fund_code} 的数据")
            return
        
        print(f"\n📊 实时数据:")
        print(f"  场内价格: {fund_info['price']:.4f} 元")
        print(f"  场外净值: {fund_info['nav']:.4f} 元")
        print(f"  净值日期: {fund_info.get('nav_date', 'N/A')}")
        print(f"  更新时间: {fund_info.get('update_time', 'N/A')}")
        
        result = self.calculator.calculate_arbitrage(fund_info)
        if result:
            self._print_arbitrage_result(result)
        else:
            print("\n❌ 无法计算套利机会")
    
    def monitor_multiple_funds(self, fund_codes: List[str] = None):
        """
        监控多个基金的套利机会
        
        Args:
            fund_codes: 基金代码列表，如果为None则监控配置中的所有基金
        """
        if fund_codes is None:
            fund_codes = list(self.funds.keys())
        
        print(f"\n{'='*60}")
        print(f"监控 {len(fund_codes)} 只LOF基金")
        print(f"{'='*60}\n")
        
        results = []
        for fund_code in fund_codes:
            fund_name = self.funds.get(fund_code, '')
            print(f"正在获取 {fund_code} {fund_name}...", end=' ')
            
            fund_info = self.data_fetcher.get_fund_info(fund_code)
            if fund_info:
                result = self.calculator.calculate_arbitrage(fund_info)
                if result:
                    results.append(result)
                print("✓")
            else:
                print("✗")
            
            time.sleep(0.5)  # 避免请求过快
        
        if results:
            print(f"\n{'='*60}")
            print("套利分析结果")
            print(f"{'='*60}\n")
            
            # 按收益率排序
            sorted_results = self.calculator.sort_by_profit(results)
            
            # 显示所有结果
            for i, result in enumerate(sorted_results, 1):
                print(f"\n[{i}] {result['fund_code']} {self.funds.get(result['fund_code'], '')}")
                self._print_arbitrage_result(result, indent=2)
            
            # 筛选有套利机会的
            opportunities = self.calculator.filter_opportunities(sorted_results)
            if opportunities:
                print(f"\n{'='*60}")
                print(f"✨ 发现 {len(opportunities)} 个套利机会！")
                print(f"{'='*60}\n")
                for i, opp in enumerate(opportunities, 1):
                    print(f"[机会 {i}] {opp['fund_code']} - {opp['arbitrage_type']}")
                    print(f"  收益率: {opp['profit_rate']:.2f}%")
                    print(f"  操作: {opp['operation']}")
                    print(f"  投入1万元净收益: {opp['net_profit_10k']:.2f} 元\n")
            else:
                print(f"\n⚠️  当前没有符合条件的套利机会")
        else:
            print("\n❌ 未能获取任何基金数据")
    
    def _print_arbitrage_result(self, result: Dict, indent: int = 0):
        """
        打印套利结果
        
        Args:
            result: 套利分析结果
            indent: 缩进空格数
        """
        prefix = ' ' * indent
        print(f"{prefix}价差: {result['price_diff']:.4f} 元 ({result['price_diff_pct']:+.2f}%)")
        print(f"{prefix}套利类型: {result['arbitrage_type']}")
        print(f"{prefix}操作方式: {result['operation']}")
        print(f"{prefix}总成本率: {result['total_cost_rate']:.2f}%")
        print(f"{prefix}预期收益率: {result['profit_rate']:.2f}%")
        print(f"{prefix}投入1万元净收益: {result['net_profit_10k']:.2f} 元")
        
        if result['has_opportunity']:
            print(f"{prefix}✅ 有套利机会！")
        else:
            print(f"{prefix}❌ 套利机会不足")
    
    def continuous_monitor(self, fund_codes: List[str] = None, interval: int = None):
        """
        持续监控套利机会
        
        Args:
            fund_codes: 基金代码列表
            interval: 监控间隔（秒），如果为None则使用配置中的值
        """
        if interval is None:
            interval = DATA_SOURCE['update_interval']
        
        if fund_codes is None:
            fund_codes = list(self.funds.keys())
        
        print(f"\n{'='*60}")
        print(f"开始持续监控 {len(fund_codes)} 只基金")
        print(f"监控间隔: {interval} 秒")
        print(f"按 Ctrl+C 停止监控")
        print(f"{'='*60}\n")
        
        try:
            while True:
                self.monitor_multiple_funds(fund_codes)
                print(f"\n⏰ 等待 {interval} 秒后更新...")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n\n监控已停止")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='LOF基金套利工具')
    parser.add_argument('-c', '--code', type=str, help='监控单个基金代码')
    parser.add_argument('-f', '--funds', nargs='+', help='监控多个基金代码')
    parser.add_argument('-m', '--monitor', action='store_true', help='持续监控模式')
    parser.add_argument('--mock', action='store_true', help='使用模拟数据（测试模式）')
    
    args = parser.parse_args()
    
    tool = LOFArbitrageTool(use_mock_data=args.mock)
    
    if args.code:
        # 监控单个基金
        fund_name = LOF_FUNDS.get(args.code, '')
        tool.monitor_single_fund(args.code, fund_name)
    elif args.funds:
        # 监控指定基金列表
        if args.monitor:
            tool.continuous_monitor(args.funds)
        else:
            tool.monitor_multiple_funds(args.funds)
    elif args.monitor:
        # 持续监控所有基金
        tool.continuous_monitor()
    else:
        # 默认：监控所有基金一次
        tool.monitor_multiple_funds()


if __name__ == '__main__':
    main()
