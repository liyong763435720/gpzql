"""复牌异常月过滤工具：识别并剔除月K线中的复牌/重新上市首月数据"""
import pandas as pd


def filter_relisting_months(df: pd.DataFrame, gap_threshold_months: int = 13) -> pd.DataFrame:
    """
    从月K线 DataFrame 中剔除"复牌首月"记录。
    判断标准：同一只股票相邻两条月K线记录的月份间距超过 gap_threshold_months 个月，
    则后一条视为复牌/重新上市首月，予以剔除。

    使用月份级别（year * 12 + month）计算间距，避免对 trade_date 列的依赖，
    同时兼容 get_monthly_kline（有 trade_date）和 get_kline_bulk_for_month（无 trade_date）。

    正常相邻同月记录间距 = 12 个月。
    默认阈值 13 个月：即缺席超过 1 个完整月历年才视为复牌异常。

    Args:
        df: 月K线 DataFrame，须包含 ts_code、year、month 列
        gap_threshold_months: 判定为复牌首月的最小月份间距，默认 13

    Returns:
        过滤后的 DataFrame（已重置索引）
    """
    required = {'ts_code', 'year', 'month'}
    if df.empty or not required.issubset(df.columns):
        return df

    df = df.copy()
    df['_ym'] = df['year'].astype(int) * 12 + df['month'].astype(int)
    df = df.sort_values(['ts_code', '_ym'])
    df['_prev_ym'] = df.groupby('ts_code')['_ym'].shift(1)
    df['_gap_months'] = df['_ym'] - df['_prev_ym']

    # 保留：首条记录（gap 为 NaN）或间隔在阈值以内
    mask = df['_gap_months'].isna() | (df['_gap_months'] <= gap_threshold_months)
    result = df[mask].drop(columns=['_ym', '_prev_ym', '_gap_months'])
    return result.reset_index(drop=True)
