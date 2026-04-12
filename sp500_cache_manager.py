# ══════════════════════════════════════════════
# 标普500监控系统 v10.1 — 数据库管理模块
# ══════════════════════════════════════════════

import pandas as pd
import numpy as np
import os
from datetime import datetime

DB_FILE = "D:\\sp500_monitor\\data\\sp500_realtime_db.csv"

def load_db():
    if not os.path.exists(DB_FILE):
        print(f"  [DB] 数据库不存在: {DB_FILE}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(DB_FILE, index_col='date', parse_dates=True)
        return df.sort_index()
    except Exception as e:
        print(f"  [DB] 读取失败: {e}")
        return pd.DataFrame()

def update_db(snapshot, sp_val=None, pe_val=None, eps_val=None):
    df = load_db()
    today = snapshot.get('date')
    if not today:
        return df

    today_ts = pd.Timestamp(today)
    row = {
        'sp500':       sp_val or snapshot.get('sp500'),
        'forward_pe':  pe_val or snapshot.get('forward_pe'),
        'forward_eps': eps_val,
        'erp':         snapshot.get('erp'),
        'vix':         snapshot.get('vix'),
        'nfci':        snapshot.get('nfci'),
        'hy_spread':   snapshot.get('hy_spread'),
        'y_spread':    snapshot.get('y_spread'),
        'fed_rate':    snapshot.get('fed_rate'),
        'real_rate':   snapshot.get('real_rate'),
        'cpi_yoy':     snapshot.get('cpi_yoy'),
        'mfg_yoy':     snapshot.get('mfg_yoy'),
    }

    new_row = pd.DataFrame([row], index=[today_ts])
    new_row.index.name = 'date'

    if today_ts in df.index:
        for col, val in row.items():
            if val is not None and not (isinstance(val, float) and np.isnan(val)):
                if col in df.columns:
                    df.loc[today_ts, col] = val
    else:
        df = pd.concat([df, new_row])

    df = df.sort_index()
    try:
        df.to_csv(DB_FILE)
        print(f"  [DB] 数据库已更新: {today}")
    except Exception as e:
        print(f"  [DB] 保存失败: {e}")
    return df

def get_eps_signals(db, current_sp, current_pe):
    """计算E+/E+2/E-/E-2（基于21天和42天比较）"""
    if db is None or len(db) == 0:
        return None, None, None, None

    # 优先用forward_eps列
    if 'forward_eps' in db.columns:
        eps_col = db['forward_eps'].dropna()
    elif 'forward_pe' in db.columns and 'sp500' in db.columns:
        valid = db.dropna(subset=['forward_pe', 'sp500'])
        if len(valid) == 0:
            return None, None, None, None
        eps_col = valid['sp500'] / valid['forward_pe']
    else:
        return None, None, None, None

    if len(eps_col) < 42:
        print(f"  [E] 历史数据{len(eps_col)}天，还需{max(0,42-len(eps_col))}天")
        return None, None, None, None

    eps_now = float(eps_col.iloc[-1])
    eps_21d = float(eps_col.iloc[-22]) if len(eps_col) >= 22 else None
    eps_42d = float(eps_col.iloc[-43]) if len(eps_col) >= 43 else None

    if eps_21d is None:
        return None, None, None, None

    e_plus  = bool(eps_now > eps_21d)
    e_minus = bool(eps_now < eps_21d)
    e_plus2  = bool(e_plus  and eps_42d and eps_21d > eps_42d)
    e_minus2 = bool(e_minus and eps_42d and eps_21d < eps_42d)

    print(f"  [E] EPS当前={eps_now:.2f}, 21d前={eps_21d:.2f}"
          + (f", 42d前={eps_42d:.2f}" if eps_42d else "")
          + f" → E+={e_plus} E+2={e_plus2} E-={e_minus} E-2={e_minus2}")
    return e_plus, e_plus2, e_minus, e_minus2

def backfill_eps(pe_current=None):
    df = load_db()
    if df.empty:
        return
    if 'forward_eps' not in df.columns:
        df['forward_eps'] = np.nan
    if 'sp500' not in df.columns or 'forward_pe' not in df.columns:
        return

    df['_pe_filled'] = df['forward_pe'].ffill().bfill()
    if pe_current:
        df['_pe_filled'] = df['_pe_filled'].fillna(pe_current)

    mask = df['forward_eps'].isna() & df['sp500'].notna() & df['_pe_filled'].notna()
    df.loc[mask, 'forward_eps'] = df.loc[mask, 'sp500'] / df.loc[mask, '_pe_filled']
    df.drop(columns=['_pe_filled'], inplace=True)

    try:
        df.to_csv(DB_FILE)
        print(f"  [DB] EPS回填完成：{mask.sum()}行")
    except Exception as e:
        print(f"  [DB] EPS回填失败: {e}")

def db_status():
    df = load_db()
    if len(df) == 0:
        print("  [DB] 数据库为空")
        return 0
    print(f"  [DB] {len(df)}行，{df.index[0].date()} ~ {df.index[-1].date()}")
    return len(df)
