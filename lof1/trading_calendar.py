# -*- coding: utf-8 -*-
"""
交易日历与时间风控模块
提供节假日判断、T+N 估算、套利时间风险提示
"""

from datetime import date, datetime, timedelta
from typing import List, Dict, Optional

# ── 中国 A 股法定节假日（2025-2026）──────────────────────────
# 格式：(月, 日)，不区分年份；跨年段单独列出
_HOLIDAYS_2025 = {
    date(2025, 1, 1),                                       # 元旦
    date(2025, 1, 28), date(2025, 1, 29), date(2025, 1, 30),
    date(2025, 1, 31), date(2025, 2, 3), date(2025, 2, 4), # 春节
    date(2025, 4, 4), date(2025, 4, 5), date(2025, 4, 6),  # 清明
    date(2025, 5, 1), date(2025, 5, 2), date(2025, 5, 5),  # 劳动节
    date(2025, 5, 31), date(2025, 6, 2),                   # 端午
    date(2025, 10, 1), date(2025, 10, 2), date(2025, 10, 3),
    date(2025, 10, 6), date(2025, 10, 7), date(2025, 10, 8), # 国庆/中秋
}

_HOLIDAYS_2026 = {
    date(2026, 1, 1),                                       # 元旦
    date(2026, 2, 17), date(2026, 2, 18), date(2026, 2, 19),
    date(2026, 2, 20), date(2026, 2, 23), date(2026, 2, 24), # 春节
    date(2026, 4, 6),                                       # 清明
    date(2026, 5, 1), date(2026, 5, 4), date(2026, 5, 5),  # 劳动节
    date(2026, 6, 19),                                      # 端午
    date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5),
    date(2026, 10, 6), date(2026, 10, 7), date(2026, 10, 8), # 国庆
}

_ALL_HOLIDAYS = _HOLIDAYS_2025 | _HOLIDAYS_2026

# 节假日名称映射（用于展示）
_HOLIDAY_NAMES: Dict[date, str] = {
    date(2025, 1, 1): '元旦', date(2025, 1, 28): '春节', date(2025, 1, 29): '春节',
    date(2025, 1, 30): '春节', date(2025, 1, 31): '春节', date(2025, 2, 3): '春节',
    date(2025, 2, 4): '春节', date(2025, 4, 4): '清明节', date(2025, 4, 5): '清明节',
    date(2025, 4, 6): '清明节', date(2025, 5, 1): '劳动节', date(2025, 5, 2): '劳动节',
    date(2025, 5, 5): '劳动节', date(2025, 5, 31): '端午节', date(2025, 6, 2): '端午节',
    date(2025, 10, 1): '国庆节', date(2025, 10, 2): '国庆节', date(2025, 10, 3): '国庆节',
    date(2025, 10, 6): '国庆节', date(2025, 10, 7): '国庆节', date(2025, 10, 8): '国庆节',
    date(2026, 1, 1): '元旦', date(2026, 2, 17): '春节', date(2026, 2, 18): '春节',
    date(2026, 2, 19): '春节', date(2026, 2, 20): '春节', date(2026, 2, 23): '春节',
    date(2026, 2, 24): '春节', date(2026, 4, 6): '清明节',
    date(2026, 5, 1): '劳动节', date(2026, 5, 4): '劳动节', date(2026, 5, 5): '劳动节',
    date(2026, 6, 19): '端午节',
    date(2026, 10, 1): '国庆节', date(2026, 10, 2): '国庆节', date(2026, 10, 5): '国庆节',
    date(2026, 10, 6): '国庆节', date(2026, 10, 7): '国庆节', date(2026, 10, 8): '国庆节',
}


def is_trading_day(d: date) -> bool:
    """判断某天是否为 A 股交易日（非周末 & 非法定节假日）"""
    if d.weekday() >= 5:   # 周六=5, 周日=6
        return False
    return d not in _ALL_HOLIDAYS


def get_next_n_trading_days(start: date, n: int) -> List[date]:
    """从 start 日期开始（不含当天），返回后续 n 个交易日"""
    result = []
    d = start + timedelta(days=1)
    while len(result) < n:
        if is_trading_day(d):
            result.append(d)
        d += timedelta(days=1)
    return result


def get_holidays_in_range(start: date, end: date) -> List[Dict]:
    """返回 [start, end] 区间内的节假日列表"""
    result = []
    d = start
    while d <= end:
        if d in _ALL_HOLIDAYS:
            result.append({
                'date': d.strftime('%Y-%m-%d'),
                'name': _HOLIDAY_NAMES.get(d, '节假日'),
            })
        d += timedelta(days=1)
    return result


def get_risk_tips(arbitrage_type: str, start_date: Optional[date] = None) -> Dict:
    """
    根据套利类型和起始日期，生成时间风控提示。

    Args:
        arbitrage_type: 'premium'（溢价）或 'discount'（折价）
        start_date: 套利开始日期，默认今天

    Returns:
        {
            'holding_days': int,         # 预计持仓交易日数
            'expected_end_date': str,    # 预计操作完成的最晚日期
            'holidays_in_range': list,   # 期间节假日列表
            'holiday_count': int,        # 节假日天数（含周末）
            'risks': list[str],          # 风险提示文本列表
            'risk_level': str,           # 'low' / 'medium' / 'high'
        }
    """
    if start_date is None:
        start_date = date.today()

    # 预计持仓（交易日）
    # 溢价：申购 T+1 确认 → 场内转场外最快 T+2，加 1 天缓冲 = 3 个交易日
    # 折价：场内买入 T+1 确认 → 场外赎回 T+7（按最快估算）= 10 个交易日
    holding_trading_days = 3 if arbitrage_type == 'premium' else 10

    # 计算预计结束的自然日
    end_trading_days = get_next_n_trading_days(start_date, holding_trading_days)
    expected_end = end_trading_days[-1] if end_trading_days else start_date

    # 期间节假日
    holidays = get_holidays_in_range(start_date, expected_end)
    # 期间实际日历天数
    calendar_days = (expected_end - start_date).days

    risks = []
    risk_level = 'low'

    # 风险1：期间有节假日
    if holidays:
        holiday_names = list({h['name'] for h in holidays})
        risks.append(
            f"持仓期间包含{'/'.join(holiday_names)}，实际到账时间可能延后，"
            f"净值变动风险加大，建议提高收益率要求。"
        )
        risk_level = 'medium'

    # 风险2：长假前（距离下一个长假 ≤ 5 个交易日）
    upcoming = []
    check_date = start_date
    for _ in range(30):
        check_date += timedelta(days=1)
        if check_date in _ALL_HOLIDAYS and _HOLIDAY_NAMES.get(check_date, '') not in [h['name'] for h in upcoming]:
            upcoming.append({'date': check_date, 'name': _HOLIDAY_NAMES.get(check_date, '节假日')})
    if upcoming:
        next_holiday = upcoming[0]
        trading_days_to_holiday = sum(
            1 for d in get_next_n_trading_days(start_date, 10)
            if d < next_holiday['date']
        )
        if trading_days_to_holiday <= 5:
            risks.append(
                f"距{next_holiday['name']}仅约 {trading_days_to_holiday} 个交易日，"
                f"节假日期间净值可能大幅偏移，存在溢价反转风险。"
            )
            risk_level = 'high' if risk_level != 'high' else 'high'

    # 风险3：折价套利持仓期长，净值波动风险
    if arbitrage_type == 'discount' and calendar_days > 14:
        risks.append(
            f"折价套利赎回周期约 {calendar_days} 个自然日，"
            f"期间净值波动可能侵蚀套利收益，请关注基金持仓风险。"
        )
        risk_level = 'medium' if risk_level == 'low' else risk_level

    # 风险4：周末叠加
    weekend_count = sum(1 for i in range(calendar_days + 1)
                        if (start_date + timedelta(days=i)).weekday() >= 5)
    if weekend_count >= 4:
        risks.append(f"持仓期间含 {weekend_count} 天周末，实际等待时间较长。")

    if not risks:
        risks.append("当前时间窗口无特殊风险，可正常操作。")

    return {
        'holding_trading_days': holding_trading_days,
        'holding_calendar_days': calendar_days,
        'expected_end_date': expected_end.strftime('%Y-%m-%d'),
        'holidays_in_range': holidays,
        'holiday_count': len(holidays),
        'risks': risks,
        'risk_level': risk_level,
    }
