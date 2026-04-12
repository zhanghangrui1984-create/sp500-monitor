# ══════════════════════════════════════════════
# 标普500监控系统 v10.1 — 数据库初始化脚本
# 本地运行一次，建立历史EPS数据库
# ══════════════════════════════════════════════

import pandas as pd
import numpy as np
import yfinance as yf
import requests
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

DB_FILE = "D:\\sp500_monitor\\data\\sp500_realtime_db.csv"

def fetch_sp500_history():
    print("  获取标普500历史数据（30年）...")
    try:
        hist = yf.download("^GSPC", start="1994-01-01",
                           auto_adjust=True, progress=False)
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)
        if hasattr(hist.index, 'tz') and hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        close = hist['Close'].squeeze()
        print(f"  ^GSPC: {len(close)}行，最新={float(close.iloc[-1]):.0f}")
        return close
    except Exception as e:
        print(f"  ❌ 失败: {e}")
        return None

def get_forward_pe():
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        url = "https://www.gurufocus.com/term/forwardpe/SPX/Forward-PE-Ratio/SP-500"
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            import re
            m = re.search(r'Forward PE.*?([\d]{1,2}\.[\d]+)', resp.text)
            if m:
                val = float(m.group(1))
                if 10 < val < 60:
                    print(f"  [gurufocus] SP500 PE={val:.2f}")
                    return val
    except: pass
    try:
        info = yf.Ticker("SPY").info
        val  = info.get('forwardPE') or info.get('trailingPE')
        if val and 10 < float(val) < 60:
            print(f"  [SPY] PE={float(val):.2f}")
            return float(val)
    except: pass
    print("  [PE] 使用默认值21.0")
    return 21.0

def build_database():
    print("=" * 60)
    print("标普500实时数据库初始化")
    print("=" * 60)

    print("\n[1/3] 获取标普500历史数据...")
    sp_series = fetch_sp500_history()
    if sp_series is None:
        print("❌ 数据获取失败，退出")
        return

    print("\n[2/3] 获取当前Forward PE...")
    pe_val = get_forward_pe()

    print("\n[3/3] 构建数据库...")
    db = pd.DataFrame({
        'sp500':       sp_series,
        'forward_pe':  pe_val,
        'forward_eps': sp_series / pe_val,
    })
    db.index.name = 'date'
    for col in ['erp','vix','nfci','hy_spread','y_spread',
                'fed_rate','real_rate','cpi_yoy','mfg_yoy']:
        db[col] = np.nan

    db = db.dropna(subset=['sp500']).sort_index()
    db.to_csv(DB_FILE)

    print(f"\n✅ 数据库已保存: {DB_FILE}")
    print(f"   共{len(db)}行，{db.index[0].date()} ~ {db.index[-1].date()}")

    # 验证E+/E+2
    eps = db['forward_eps'].dropna()
    if len(eps) >= 42:
        e_now = float(eps.iloc[-1])
        e_21  = float(eps.iloc[-22])
        e_42  = float(eps.iloc[-43])
        e_plus  = e_now > e_21
        e_plus2 = e_plus and e_21 > e_42
        print(f"\nE验证: EPS={e_now:.2f}, 21d前={e_21:.2f}, 42d前={e_42:.2f}")
        print(f"  E+={e_plus}, E+2={e_plus2} ✅")
    else:
        print(f"\n数据{len(eps)}行，E+2需要至少42行")

if __name__ == '__main__':
    build_database()
