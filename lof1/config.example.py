# -*- coding: utf-8 -*-
"""
LOF基金套利工具配置文件模板

首次部署时，请复制此文件为 config.py 并配置：
1. 设置 Tushare token（如果使用Tushare数据源）
2. 根据需要调整其他配置项

或者使用环境变量设置 TUSHARE_TOKEN（推荐）
"""

# 交易费用配置（单位：百分比）
TRADE_FEES = {
    'buy_commission': 0.0003,      # 买入佣金 0.03%
    'sell_commission': 0.0003,     # 卖出佣金 0.03%
    'subscribe_fee': 0.015,         # 申购费率 1.5%（通常为1.5%）
    'redeem_fee': 0.005,            # 赎回费率 0.5%（通常为0.5%）
    'stamp_tax': 0.001,             # 印花税 0.1%（卖出时收取）
}

# 套利阈值配置
ARBITRAGE_THRESHOLD = {
    'min_profit_rate': 0.005,      # 最小套利收益率 0.5%
    'min_price_diff': 0.01,        # 最小价差 0.01元
}

# 数据源配置
DATA_SOURCE = {
    'update_interval': 60,          # 数据更新间隔（秒）
    
    # 价格数据源配置（可启用多个，按优先级使用）
    'price_sources': {
        'eastmoney_stock': {'enabled': True, 'priority': 1, 'name': '东方财富股票API'},
        'eastmoney_arbitrage': {'enabled': True, 'priority': 2, 'name': '东方财富套利API'},
        'sina': {'enabled': True, 'priority': 3, 'name': '新浪财经'},
        'tencent': {'enabled': True, 'priority': 4, 'name': '腾讯财经'},
        'netease': {'enabled': True, 'priority': 5, 'name': '网易财经'},
    },
    
    # 净值数据源配置
    'nav_sources': {
        'eastmoney_api': {'enabled': True, 'priority': 1, 'name': '东方财富净值API'},
        'eastmoney_arbitrage': {'enabled': True, 'priority': 2, 'name': '东方财富套利API'},
        'eastmoney_js': {'enabled': True, 'priority': 3, 'name': '东方财富JS文件'},
        '1234567': {'enabled': True, 'priority': 4, 'name': '天天基金网'},
    },
    
    # 基金列表数据源配置
    'fund_list_sources': {
        'tushare': {'enabled': True, 'priority': 1, 'name': 'Tushare', 'token': 'your_tushare_token_here'},
        'eastmoney': {'enabled': True, 'priority': 2, 'name': '东方财富'},
    },
    
    # 中文名称数据源配置
    'name_sources': {
        'tushare': {'enabled': True, 'priority': 1, 'name': 'Tushare'},
        'eastmoney_api': {'enabled': True, 'priority': 2, 'name': '东方财富API'},
        'eastmoney_js': {'enabled': True, 'priority': 3, 'name': '东方财富JS文件'},
    },
    
    # 限购信息数据源配置
    'purchase_limit_sources': {
        'akshare': {'enabled': True, 'priority': 1, 'name': 'AkShare'},
        'eastmoney_api': {'enabled': True, 'priority': 2, 'name': '东方财富API'},
        'eastmoney_scrape': {'enabled': True, 'priority': 3, 'name': '东方财富网页爬取'},
    },
    
    # 向后兼容的旧配置
    'use_tushare': True,
    'tushare_token': 'your_tushare_token_here',  # 请替换为您的真实Tushare token，或使用环境变量 TUSHARE_TOKEN
}

# 常用LOF基金代码（真实LOF基金列表）
# 注意：基金列表可以通过API动态获取，这里提供常用基金作为默认列表
LOF_FUNDS = {
    # 热门指数型LOF
    '161725': '招商中证白酒指数(LOF)A',
    '161028': '富国中证新能源汽车指数(LOF)',
    '160632': '鹏华酒分级',
    '160225': '国泰国证新能源汽车指数(LOF)',
    '160633': '鹏华中证一带一路主题指数(LOF)',
    '161607': '融通巨潮100指数(LOF)A',
    '161610': '融通领先成长混合(LOF)',
    '161631': '融通人工智能指数(LOF)A',
    '161726': '招商国证生物医药指数(LOF)A',
    '161727': '招商中证银行指数(LOF)A',
    '161728': '招商中证煤炭等权指数(LOF)A',
    '161729': '招商中证证券公司指数(LOF)A',
    
    # 热门混合型LOF
    '163402': '兴全趋势投资混合(LOF)',
    '161005': '富国天惠精选成长混合(LOF)A',
    '163406': '兴全合润混合(LOF)',
    '163407': '兴全沪深300指数(LOF)A',
    '163409': '兴全绿色投资混合(LOF)',
    '163412': '兴全轻资产投资混合(LOF)',
    '163415': '兴全商业模式优选混合(LOF)',
    '163417': '兴全合宜混合(LOF)A',
    '161017': '富国中证500指数增强(LOF)A',
    '161019': '富国中证军工指数(LOF)A',
    '161022': '富国创业板指数(LOF)A',
    '161025': '富国中证移动互联网指数(LOF)A',
    '161026': '富国中证国有企业改革指数(LOF)A',
    '161027': '富国中证全指证券公司指数(LOF)A',
    '161029': '富国中证银行指数(LOF)A',
    '161030': '富国中证煤炭指数(LOF)A',
    '161031': '富国中证工业4.0指数(LOF)A',
    '161032': '富国中证体育产业指数(LOF)A',
    '161033': '富国中证智能汽车指数(LOF)A',
    '161035': '富国中证医药主题指数(LOF)A',
    '161036': '富国中证高端制造指数(LOF)A',
    '161037': '富国中证消费50指数(LOF)A',
    
    # 其他热门LOF
    '160119': '南方中证500ETF联接(LOF)A',
    '160125': '南方香港优选股票(LOF)',
    '160127': '南方新兴消费增长股票(LOF)',
    '160133': '南方天元新产业股票(LOF)',
    '160135': '南方中证高铁产业指数(LOF)',
    '160136': '南方中证互联网指数(LOF)',
}
