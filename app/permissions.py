"""
权限定义和管理
"""
from typing import List, Dict

# 权限定义
PERMISSIONS = {
    'stock_analysis_single': {
        'code': 'stock_analysis_single',
        'name': '单股分析（单月统计）',
        'description': '可以查询单只股票的单月统计数据'
    },
    'stock_analysis_multi': {
        'code': 'stock_analysis_multi',
        'name': '单股分析（多月统计）',
        'description': '可以查询单只股票的多月统计数据'
    },
    'month_filter': {
        'code': 'month_filter',
        'name': '月榜单',
        'description': '可以使用月榜单筛选股票数据'
    },
    'month_enhanced': {
        'code': 'month_enhanced',
        'name': '月榜单增强',
        'description': '可以使用月榜单增强（期望收益率、跑赢大盘概率、近5年一致性等）'
    },
    'industry_statistics': {
        'code': 'industry_statistics',
        'name': '行业分析（行业统计）',
        'description': '可以查询行业统计数据'
    },
    'industry_enhanced': {
        'code': 'industry_enhanced',
        'name': '行业增强分析',
        'description': '可以使用行业增强分析（期望收益率、跑赢大盘概率、近5年一致性等）'
    },
    'industry_top_stocks': {
        'code': 'industry_top_stocks',
        'name': '行业分析（行业前N支股票）',
        'description': '可以查询行业前N支股票'
    },
    'source_compare': {
        'code': 'source_compare',
        'name': '数据校对',
        'description': '可以对比不同数据源的数据'
    },
    'export_excel': {
        'code': 'export_excel',
        'name': 'Excel导出',
        'description': '可以导出查询结果为Excel文件'
    },
    'data_management': {
        'code': 'data_management',
        'name': '数据管理',
        'description': '可以查看数据状态、更新数据（全量/增量）'
    },
    'lof_arbitrage': {
        'code': 'lof_arbitrage',
        'name': 'LOF基金套利',
        'description': '可以使用LOF基金套利工具（查看行情、记录套利、设置提醒等）'
    }
}

# 所有权限代码列表
ALL_PERMISSIONS = list(PERMISSIONS.keys())

# 套餐定义
PLANS = {
    'free': {
        'code': 'free',
        'name': '免费版',
        'description': '基础股票单月统计分析',
        'price_monthly': 0,
        'price_quarterly': 0,
        'price_yearly': 0,
        'permissions': [
            'stock_analysis_single',
        ]
    },
    'basic': {
        'code': 'basic',
        'name': '基础版',
        'description': '单股多月分析、月榜单、行业统计',
        'price_monthly': 39,
        'price_quarterly': 99,
        'price_yearly': 359,
        'permissions': [
            'stock_analysis_single',
            'stock_analysis_multi',
            'month_filter',
            'industry_statistics',
            'industry_top_stocks',
        ]
    },
    'pro': {
        'code': 'pro',
        'name': '专业版',
        'description': '全功能：增强分析、Excel导出、LOF套利',
        'price_monthly': 79,
        'price_quarterly': 199,
        'price_yearly': 699,
        'permissions': [
            'stock_analysis_single',
            'stock_analysis_multi',
            'month_filter',
            'month_enhanced',
            'industry_statistics',
            'industry_enhanced',
            'industry_top_stocks',
            'source_compare',
            'export_excel',
            'lof_arbitrage',
        ]
    },
}

# 点数充值包定义（amount 单位：分）
CREDIT_PACKAGES = [
    {
        'id': 'starter',
        'name': '体验包',
        'credits': 35,
        'amount': 1900,
        'tag': '',
    },
    {
        'id': 'standard',
        'name': '标准包',
        'credits': 100,
        'amount': 4900,
        'tag': '最划算',
    },
    {
        'id': 'large',
        'name': '大额包',
        'credits': 260,
        'amount': 9900,
        'tag': '',
    },
]

# 点数日解锁功能定价（单位：点数）
CREDIT_UNLOCK_COSTS = {
    'stock_analysis_multi': 5,    # 单股多月分析
    'month_filter': 5,            # 月榜单
    'industry_statistics': 5,     # 行业统计
    'source_compare': 5,          # 数据校对
    'month_enhanced': 8,          # 月榜单增强
    'industry_enhanced': 8,       # 行业增强分析
    'lof_arbitrage': 12,          # LOF套利分析
    # export_excel 不进点数体系，仅专业版订阅可用
}

# 套餐层级顺序（用于判断升降级）
PLAN_ORDER = ['free', 'basic', 'pro']


def get_plan(code: str) -> Dict:
    """获取套餐信息"""
    return PLANS.get(code, {})


def get_plan_permissions(code: str) -> List[str]:
    """获取套餐对应的权限列表"""
    return PLANS.get(code, {}).get('permissions', [])


def get_all_plans() -> List[Dict]:
    """获取所有套餐列表（按层级排序）"""
    return [PLANS[code] for code in PLAN_ORDER]


def get_permission_name(code: str) -> str:
    """获取权限名称"""
    return PERMISSIONS.get(code, {}).get('name', code)


def get_all_permissions() -> List[Dict]:
    """获取所有权限列表"""
    return [PERMISSIONS[code] for code in ALL_PERMISSIONS]

