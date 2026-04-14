# ══════════════════════════════════════════════
# 标普500监控系统 v10.3 — 数据抓取模块
# ══════════════════════════════════════════════

import yfinance as yf
import pandas as pd
import numpy as np
import requests
from fredapi import Fred
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

from config import FRED_API_KEY
fred = Fred(api_key=FRED_API_KEY)

def get_fred_series(series_id, periods=600):
    try:
        end   = datetime.today()
        start = end - timedelta(days=periods)
        data  = fred.get_series(series_id, observation_start=start)
        return data.dropna()
    except Exception as e:
        print(f"  [FRED] {series_id} 失败: {e}")
        return None

def yf_close(ticker, period="20y"):
    hist = yf.download(ticker, period=period, auto_adjust=True, progress=False)
    if hist is None or len(hist) == 0:
        return None
    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = hist.columns.get_level_values(0)
    if hasattr(hist.index, 'tz') and hist.index.tz is not None:
        hist.index = hist.index.tz_localize(None)
    close = hist['Close']
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    return close.squeeze() if len(close) > 10 else None

def get_sp500_history():
    print("  [yfinance] 抓取 ^GSPC...")
    try:
        close = yf_close("^GSPC", "30y")
        if close is not None:
            print(f"  [^GSPC] 最新={float(close.iloc[-1]):.0f} 共{len(close)}行")
            return close
    except Exception as e:
        print(f"  [^GSPC] 失败: {e}")
    # 备用：SPY×10
    try:
        close = yf_close("SPY", "30y")
        if close is not None:
            close = close * 10
            print(f"  [SPY×10] 最新≈{float(close.iloc[-1]):.0f} 共{len(close)}行")
            return close
    except Exception as e:
        print(f"  [SPY] 失败: {e}")
    return None

def get_vix_history():
    try:
        close = yf_close("^VIX", "5y")
        if close is not None:
            print(f"  [^VIX] 最新={float(close.iloc[-1]):.2f}")
            return close
    except Exception as e:
        print(f"  [^VIX] 失败: {e}")
    return None

def get_tlt_history():
    try:
        close = yf_close("TLT", "20y")
        if close is not None:
            print(f"  [TLT] 最新={float(close.iloc[-1]):.2f}")
            return close
    except Exception as e:
        print(f"  [TLT] 失败: {e}")
    return None

def get_forward_pe():
    # 方法1: gurufocus SP500
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
    except Exception as e:
        print(f"  [gurufocus] 失败: {e}")

    # 方法2: SPY
    try:
        info = yf.Ticker("SPY").info
        for key in ['forwardPE', 'trailingPE']:
            val = info.get(key)
            if val and 10 < float(val) < 60:
                print(f"  [SPY {key}] PE={float(val):.2f}")
                return float(val)
    except Exception as e:
        print(f"  [SPY PE] 失败: {e}")

    print("  [PE] 使用默认值21.0")
    return 21.0

def fetch_all_data():
    print("=" * 50)
    print(f"开始抓取：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    data = {}

    print("[1/13] 标普500历史数据...")
    data['sp500_series'] = get_sp500_history()

    print("[2/13] VIX恐慌指数...")
    data['vix_series'] = get_vix_history()

    print("[3/13] TLT（20年期国债ETF）...")
    data['tlt_series'] = get_tlt_history()

    print("[4/13] Y利差（T10Y2Y）...")
    data['y_series'] = get_fred_series('T10Y2Y', 500)

    print("[5/13] 联储利率（日度DFF）...")
    data['f_series'] = get_fred_series('DFF', 1500)

    print("[6/13] 实际利率...")
    data['r_series'] = get_fred_series('DFII10', 500)

    print("[7/13] HY信用利差...")
    data['hy_series'] = get_fred_series('BAMLH0A0HYM2', 500)

    print("[8/13] NFCI...")
    nfci = get_fred_series('NFCI', 600)
    if nfci is not None:
        nfci.index = nfci.index + timedelta(days=5)  # 发布延迟5天
    data['nfci_series'] = nfci

    print("[9/13] WALCL...")
    walcl = get_fred_series('WALCL', 400)
    if walcl is not None:
        walcl.index = walcl.index + timedelta(days=1)  # 延迟1天
    data['walcl_series'] = walcl

    print("[10/13] CPI...")
    data['cpi_series'] = get_fred_series('CPIAUCSL', 700)

    print("[11/13] MFG制造业新订单...")
    data['mfg_series'] = get_fred_series('NEWORDER', 400)

    print("[12/13] OIL原油价格（WTI）...")
    try:
        oil_s = yf_close("CL=F", "10y")
        if oil_s is None or len(oil_s) < 100:
            # 备用：USO ETF近似
            oil_s = yf_close("USO", "10y")
            if oil_s is not None:
                print(f"  [USO] 最新={float(oil_s.iloc[-1]):.2f}")
        else:
            print(f"  [CL=F] WTI最新={float(oil_s.iloc[-1]):.2f}")
        data['oil_series'] = oil_s
    except Exception as e:
        print(f"  [OIL] 失败: {e}")
        data['oil_series'] = None

    print("[13/13] Forward PE...")
    data['forward_pe'] = get_forward_pe()

    print("=" * 50)
    print("数据抓取完成")
    print("=" * 50)
    return data
