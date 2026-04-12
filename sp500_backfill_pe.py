"""
标普500历史Forward PE回填脚本
从gurufocus/multpl抓取历史PE，写入DB，使P+/P-可以正常计算
一次性运行即可，之后每日自动更新
"""

import pandas as pd
import numpy as np
import requests
import yfinance as yf
import os
import sys

DB_FILE = "D:\\sp500_monitor\\data\\sp500_realtime_db.csv"
# 云端路径（直接运行此脚本时自动识别）
if not os.path.exists(os.path.dirname(DB_FILE)):
    DB_FILE = "data/sp500_realtime_db.csv"

def fetch_pe_from_multpl():
    """从multpl.com抓取S&P500历史Forward PE（月度，2000年至今）"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        url = "https://www.multpl.com/s-p-500-pe-ratio/table/by-month"
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            print(f"  [multpl] HTTP {resp.status_code}")
            return None

        tables = pd.read_html(resp.text)
        if not tables:
            return None

        df = tables[0]
        df.columns = ['date', 'pe']
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['pe']   = pd.to_numeric(df['pe'], errors='coerce')
        df = df.dropna().sort_values('date').reset_index(drop=True)

        if len(df) < 50:
            print(f"  [multpl] 数据太少: {len(df)}行")
            return None

        print(f"  [multpl] 成功: {len(df)}行，{df['date'].iloc[0].date()} ~ {df['date'].iloc[-1].date()}")
        print(f"  [multpl] PE范围: {df['pe'].min():.1f} ~ {df['pe'].max():.1f}")
        return df.set_index('date')['pe']

    except Exception as e:
        print(f"  [multpl] 失败: {e}")
        return None

def fetch_pe_from_gurufocus():
    """从gurufocus抓取S&P500历史PE（备用）"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        url = "https://www.gurufocus.com/term/peratio/SP500/PE-Ratio/SP-500"
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            return None

        tables = pd.read_html(resp.text)
        for t in tables:
            if len(t) > 20 and t.shape[1] >= 2:
                t.columns = [str(c) for c in t.columns]
                date_col = t.columns[0]
                pe_col   = t.columns[1]
                t[date_col] = pd.to_datetime(t[date_col], errors='coerce')
                t[pe_col]   = pd.to_numeric(t[pe_col], errors='coerce')
                t = t.dropna(subset=[date_col, pe_col])
                if len(t) > 20:
                    pe_s = t.set_index(date_col)[pe_col].sort_index()
                    print(f"  [gurufocus] 成功: {len(pe_s)}行")
                    return pe_s
        return None
    except Exception as e:
        print(f"  [gurufocus] 失败: {e}")
        return None

def backfill_pe_to_db(pe_monthly):
    """把月度PE前向填充到日度，更新DB"""
    if pe_monthly is None or len(pe_monthly) < 10:
        print("  ❌ PE数据不足，跳过")
        return

    # 前向填充到日度
    start = pe_monthly.index[0]
    end   = pe_monthly.index[-1]
    daily_idx = pd.date_range(start=start, end=end, freq='D')
    pe_daily  = pe_monthly.reindex(daily_idx).ffill()
    print(f"  PE日度序列: {len(pe_daily)}行，σ={pe_daily.std():.2f}")

    # 读取DB
    if not os.path.exists(DB_FILE):
        print(f"  ❌ DB不存在: {DB_FILE}")
        return

    db = pd.read_csv(DB_FILE, index_col='date', parse_dates=True)
    print(f"  DB现有: {len(db)}行")

    # 将PE写入DB（只更新forward_pe为空或与历史常数相同的行）
    pe_aligned = pe_daily.reindex(db.index, method='ffill')
    pe_aligned = pe_aligned.reindex(db.index)

    updated = 0
    if 'forward_pe' not in db.columns:
        db['forward_pe'] = np.nan

    # 判断当前DB的PE是否全为常数（初始化时的情况）
    pe_current = db['forward_pe'].dropna()
    is_constant = (pe_current.nunique() <= 2) if len(pe_current) > 0 else True

    if is_constant:
        print("  检测到PE为常数（初始化状态），开始全量回填...")
        for idx in db.index:
            if idx in pe_aligned.index and not pd.isna(pe_aligned[idx]):
                db.loc[idx, 'forward_pe'] = float(pe_aligned[idx])
                updated += 1
    else:
        print("  检测到PE已有历史数据，仅补空缺...")
        mask = db['forward_pe'].isna() & pe_aligned.notna()
        db.loc[mask, 'forward_pe'] = pe_aligned[mask]
        updated = mask.sum()

    # 同步更新forward_eps
    sp_col = 'sp500' if 'sp500' in db.columns else None
    if sp_col:
        mask_eps = db['forward_pe'].notna() & db[sp_col].notna()
        db.loc[mask_eps, 'forward_eps'] = db.loc[mask_eps, sp_col] / db.loc[mask_eps, 'forward_pe']
        print(f"  forward_eps同步更新: {mask_eps.sum()}行")

    db.to_csv(DB_FILE)
    print(f"  ✅ PE回填完成: {updated}行已更新")
    print(f"  PE统计: 均值={db['forward_pe'].mean():.1f}, σ={db['forward_pe'].std():.2f}, 不同值={db['forward_pe'].nunique()}个")

def run():
    print("=" * 55)
    print("标普500 历史Forward PE 回填工具")
    print("=" * 55)

    # 方法1：multpl.com
    print("\n[1/2] 尝试 multpl.com...")
    pe = fetch_pe_from_multpl()

    # 方法2：gurufocus备用
    if pe is None or len(pe) < 50:
        print("\n[2/2] 尝试 gurufocus...")
        pe = fetch_pe_from_gurufocus()

    if pe is None:
        print("\n❌ 所有数据源均失败，请检查网络")
        return

    print("\n回填到数据库...")
    backfill_pe_to_db(pe)

if __name__ == '__main__':
    run()
