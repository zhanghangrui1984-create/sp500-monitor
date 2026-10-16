# ══════════════════════════════════════════════════════════════════════════
# 标普500因子历史记录器
# ══════════════════════════════════════════════════════════════════════════
#
# 每次 main 跑完后调用 log_factors(snapshot),把当天一行写入:
#   data/sp500_factors_history.csv
#
# 文件格式(列顺序固定):
#   日期 / 中间量 / 60 因子(0/1)/ 10 触发器(triggered + pct)
#
# 行为:
#   - 文件不存在 → 创建
#   - 当天已有行 → 覆盖更新
#   - 列变了(新增因子等) → 自动加列,旧行用空补
# ══════════════════════════════════════════════════════════════════════════

import os
import pandas as pd
import numpy as np

CSV_PATH = "data/sp500_factors_history.csv"

# 中间量列(snapshot 顶层字段)及对应 csv 列名(中文表头便于 Excel 查看)
INTERMEDIATE_COLS = [
    ('sp500',          'SP500'),
    ('sp_dd_pct',      'ATH回撤%'),
    ('ma200w',         'MA200W'),
    ('sigma_dev_200',  'Sσ_200'),
    ('vix',            'VIX'),
    ('V_c',            'V_c'),
    ('V_c_sigma',      'V_c_σ'),
    ('y_spread',       'Y利差'),
    ('fed_rate',       'Fed利率%'),
    ('real_rate',      '实际利率%'),
    ('hy_spread',      'HY利差%'),
    ('hy_c21',         'HY_c21'),
    ('nfci',           'NFCI'),
    ('n_c',            'N_c'),
    ('n_c_mu',         'N_c_μ'),
    ('n_c_sigma',      'N_c_σ'),
    ('forward_pe',     'Forward_PE'),
    ('pe_avg',         'PE_avg'),
    ('erp',            'ERP%'),
    ('cpi_y',          'CPI同比%'),
    ('oil_price',      'OIL(WTI)'),
    ('oil_c21',        'OIL_c21%'),
    ('oil_pct5y',      'OIL_pct5y%'),
    ('walcl_13w_chg',  'WALCL_13w变化T'),
]

# 60 因子的标准顺序(按物理类别分组,便于 Excel 横向查看)
FACTOR_ORDER = [
    # 股价/技术(16 个)
    'W+200',
    'Sσ_200 ≥ +1σ', 'Sσ_200 ≥ +2σ', 'Sσ_200 ≥ +3σ',
    'Sσ_200 ≤ -1σ', 'Sσ_200 ≤ -2σ', 'Sσ_200 ≤ -3σ',
    'S_t < S_200',
    'S- ≥ 5%', 'S- ≥ 10%', 'S- ≥ 20%', 'S- ≥ 30%',
    'S+1', 'S-1', 'S+6', 'S-6', 'S-+',
    # VIX(11 个)
    'V ≤ V_c', 'V ≤ V_c-1σ',
    'V ≥ V_c+1σ', 'V ≥ V_c+2σ', 'V ≥ V_c+3σ',
    'max(V,21d) ≥ V_c+2σ',
    'V < MA(V,21)', 'V ≤ MA(V,21)-1σ_21d',
    'V ≥ MA(V,21)+1σ_21d', 'V ≥ MA(V,21)+2σ_21d', 'V ≥ MA(V,21)+3σ_21d',
    # NFCI(6 个)
    'N > 0', 'N < 0',
    'N+ ≥ μ+1σ', 'N+ ≥ μ+2σ', 'N- ≥ μ+1σ', 'N- ≥ μ+2σ',
    # Fed / Y(4 个)
    'F+', 'F-', 'Y+', 'Y-',
    # WALCL(2 个)
    'WALCL+ ≥ 500hm', 'WALCL+ ≥ 1000hm',
    # HY(4 个)
    'HY_t > 8%', 'HY_c21 > 0', 'HY_c21 ≤ 0', 'HY_t < HY_m21',
    # 估值(5 个)
    'P+', 'P-', 'ERP > 3%', 'ERP < 1.5%', 'ERP < 0',
    # CPI(3 个)
    'CPI_y < 0%', 'CPI_y ≥ 5%', 'CPI_y ≥ 7%',
    # EPS(4 个)
    'E+', 'E+2', 'E-', 'E-2',
    # OIL(3 个)
    'OIL_c21 ≥ μ+1σ', 'OIL_c21 ≥ μ+2σ', 'OIL_pct5y > 85%',
]

# 触发器(10 个)
TRIGGER_IDS = [
    'T1_2000离场', 'T2_2007离场', 'T3_2015离场', 'T4_2022离场',
    'T5_2002入场', 'T6_2009入场', 'T7_2020入场', 'T8_2022入场',
    'T9_白银坑1组', 'T10_白银坑2组',
]


def _build_row(snapshot):
    """从 snapshot 构造当天的一行 dict"""
    row = {'日期': snapshot.get('date')}

    # 中间量
    for snap_key, col_name in INTERMEDIATE_COLS:
        row[col_name] = snapshot.get(snap_key)

    # 60 因子(0/1)
    factors = snapshot.get('factors', {})
    for f in FACTOR_ORDER:
        v = factors.get(f)
        if v is True:
            row[f] = 1
        elif v is False:
            row[f] = 0
        else:
            row[f] = ''  # None → 空

    # 10 触发器
    triggers = snapshot.get('triggers', {})
    for tid in TRIGGER_IDS:
        t = triggers.get(tid, {})
        triggered = t.get('triggered')
        pct       = t.get('satisfied_pct')
        sat       = t.get('satisfied_count', 0)
        total     = t.get('total_must', 0)

        # 触发标记列
        if triggered is True:
            row[f'{tid}_triggered'] = 1
        elif triggered is False:
            row[f'{tid}_triggered'] = 0
        else:
            row[f'{tid}_triggered'] = ''

        # 达成率列
        if pct is not None:
            row[f'{tid}_pct'] = round(pct * 100, 1)
        else:
            row[f'{tid}_pct'] = ''

        # 达成 / 总数列
        row[f'{tid}_达成'] = f"{sat}/{total}" if total > 0 else ''

    return row


def _build_column_order():
    """完整的列顺序"""
    cols = ['日期']
    cols += [c for _, c in INTERMEDIATE_COLS]
    cols += FACTOR_ORDER
    for tid in TRIGGER_IDS:
        cols += [f'{tid}_triggered', f'{tid}_pct', f'{tid}_达成']
    return cols


def log_factors(snapshot, csv_path=None):
    """
    把当天的 snapshot 写入 factors_history.csv。
    幂等:同一天重复跑会覆盖当天那一行,而不是追加重复行。
    """
    path = csv_path or CSV_PATH
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)

    today  = snapshot.get('date')
    if not today:
        print("  [factors_logger] ⚠️ snapshot 没有 date,跳过")
        return

    new_row = _build_row(snapshot)
    col_order = _build_column_order()

    # 读现有 csv
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, dtype=str)  # 全字符串读,避免类型转换
        except Exception as e:
            print(f"  [factors_logger] ⚠️ 读现有 csv 失败,新建: {e}")
            df = pd.DataFrame(columns=col_order)
    else:
        df = pd.DataFrame(columns=col_order)

    # 删除当天旧行(若存在)
    if len(df) > 0 and '日期' in df.columns:
        df = df[df['日期'] != today].copy()

    # 追加新行(转字符串避免格式不一致)
    new_row_str = {k: ('' if v is None else str(v) if not isinstance(v, float) or not pd.isna(v) else '')
                   for k, v in new_row.items()}
    df = pd.concat([df, pd.DataFrame([new_row_str])], ignore_index=True)

    # 按日期排序,且确保列顺序
    df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
    df = df.sort_values('日期').reset_index(drop=True)
    df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')

    # 列顺序:已知列在前(按 col_order),新出现的列在最后
    known   = [c for c in col_order if c in df.columns]
    unknown = [c for c in df.columns if c not in col_order]
    df = df[known + unknown]

    # 写回
    df.to_csv(path, index=False, encoding='utf-8-sig')  # utf-8-sig 让 Excel 直接识别中文
    print(f"  [factors_logger] ✅ 已记录到 {path}({len(df)} 行,{today})")


if __name__ == '__main__':
    # 自测
    sample = {
        'date': '2026-05-12',
        'sp500': 7412.84, 'sp_dd_pct': 0.0, 'sigma_dev_200': 2.97,
        'vix': 18.91, 'V_c': 18.23, 'V_c_sigma': 3.28,
        'erp': 1.67, 'forward_pe': 27.77,
        'factors': {'W+200': True, 'S- ≥ 10%': False, 'S_t < S_200': False},
        'triggers': {
            'T2_2007离场': {'triggered': False, 'satisfied_pct': 0.71,
                            'satisfied_count': 5, 'total_must': 7},
            'T10_白银坑2组': {'triggered': False, 'satisfied_pct': 0.73,
                              'satisfied_count': 8, 'total_must': 11},
        }
    }
    log_factors(sample, csv_path='/tmp/test_factors.csv')
    import pandas as pd
    print(pd.read_csv('/tmp/test_factors.csv').iloc[0].head(20))
