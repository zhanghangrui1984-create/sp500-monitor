# ══════════════════════════════════════════════════════════════════════════
# 标普500监控系统 v15 — 数据抓取模块(修复 OIL 数据延迟问题)
# ══════════════════════════════════════════════════════════════════════════
# 关键修复:
#   - OIL 历史用 FRED DCOILWTICO(1986+ 长历史,算 5 年百分位需要)
#   - OIL 当前价用 Yahoo CL=F **覆盖** FRED 滞后值
#     (FRED EIA 数据滞后 1-3 周,会让 OIL_pct5y 算错)
# ══════════════════════════════════════════════════════════════════════════

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


def get_fred_series(series_id, periods=600, observation_start=None):
    try:
        end = datetime.today()
        if observation_start is not None:
            start = observation_start
        else:
            start = end - timedelta(days=periods)
        data = fred.get_series(series_id, observation_start=start)
        return data.dropna()
    except Exception as e:
        print(f"  [FRED] {series_id} 失败: {e}")
        return None


def yf_close(ticker, period="20y", start=None):
    if start is not None:
        hist = yf.download(ticker, start=start, auto_adjust=True, progress=False)
    else:
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
        close = yf_close("^GSPC", "max")
        if close is not None:
            print(f"  [^GSPC] 最新={float(close.iloc[-1]):.0f} 共{len(close)}行 ({close.index[0].date()} ~)")
            return close
    except Exception as e:
        print(f"  [^GSPC] 失败: {e}")
    try:
        close = yf_close("SPY", "max")
        if close is not None:
            close = close * 10
            print(f"  [SPY×10] 最新≈{float(close.iloc[-1]):.0f} 共{len(close)}行")
            return close
    except Exception as e:
        print(f"  [SPY] 失败: {e}")
    return None


def get_vix_history():
    try:
        close = yf_close("^VIX", "max")
        if close is not None:
            print(f"  [^VIX] 最新={float(close.iloc[-1]):.2f} 共{len(close)}行 ({close.index[0].date()} ~)")
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


def get_oil_combined():
    """
    OIL 数据(★ 混合源,修复 FRED 滞后问题):
      历史: FRED DCOILWTICO(1986+ 长历史,算 5 年百分位需要)
      最新: Yahoo CL=F 覆盖 FRED 最近 30 天(FRED EIA 数据通常滞后 1-3 周)
    """
    oil_fred = None
    oil_yahoo = None

    try:
        oil_fred = get_fred_series('DCOILWTICO', observation_start='1986-01-01')
        if oil_fred is not None and len(oil_fred) > 1000:
            print(f"  [FRED DCOILWTICO] 共{len(oil_fred)}行 ({oil_fred.index[0].date()} ~ {oil_fred.index[-1].date()}) FRED最新值={float(oil_fred.iloc[-1]):.2f}")
    except Exception as e:
        print(f"  [FRED OIL] 失败: {e}")

    try:
        oil_yahoo = yf_close("CL=F", "1mo")
        if oil_yahoo is not None:
            print(f"  [Yahoo CL=F] 实时最新={float(oil_yahoo.iloc[-1]):.2f} ({oil_yahoo.index[-1].date()})")
    except Exception as e:
        print(f"  [Yahoo CL=F] 失败: {e}")

    if oil_fred is not None and oil_yahoo is not None:
        # FRED 截到 30 天前,Yahoo 用最近 30 天的实时数据覆盖
        fred_last_date = oil_fred.index[-1]
        cutoff = fred_last_date - pd.Timedelta(days=30)
        oil_old = oil_fred[oil_fred.index <= cutoff]
        yahoo_recent = oil_yahoo[oil_yahoo.index > cutoff]
        oil_combined = pd.concat([oil_old, yahoo_recent]).sort_index()
        oil_combined = oil_combined[~oil_combined.index.duplicated(keep='last')]
        print(f"  [OIL 合并] 共{len(oil_combined)}行,最新={float(oil_combined.iloc[-1]):.2f} ({oil_combined.index[-1].date()})")
        return oil_combined
    elif oil_fred is not None:
        print(f"  [OIL] 只有 FRED 数据(警告:可能滞后)")
        return oil_fred
    elif oil_yahoo is not None:
        print(f"  [OIL] 只有 Yahoo 数据(警告:历史短,百分位可能不准)")
        return oil_yahoo
    else:
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
    except Exception as e:
        print(f"  [gurufocus] 失败: {e}")

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
    print(f"开始抓取:{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    data = {}

    print("[1/13] 标普500历史数据(全历史)...")
    data['sp500_series'] = get_sp500_history()

    print("[2/13] VIX恐慌指数(全历史)...")
    data['vix_series'] = get_vix_history()

    print("[3/13] TLT(20年期国债ETF)...")
    data['tlt_series'] = get_tlt_history()

    print("[4/13] Y利差(T10Y2Y, 5y)...")
    data['y_series'] = get_fred_series('T10Y2Y', 1800)

    print("[5/13] 联储利率(日度DFF, 5y)...")
    data['f_series'] = get_fred_series('DFF', 1800)

    print("[6/13] 实际利率DFII10(5y)...")
    data['r_series'] = get_fred_series('DFII10', 1800)

    print("[7/13] HY信用利差(全历史 1996+)...")
    data['hy_series'] = get_fred_series('BAMLH0A0HYM2', observation_start='1996-12-01')

    print("[8/13] NFCI(全历史 1971+,用于 5y σ 体系)...")
    nfci = get_fred_series('NFCI', observation_start='1971-01-01')
    if nfci is not None:
        nfci.index = nfci.index + timedelta(days=5)
        print(f"  [NFCI] 共 {len(nfci)} 行,({nfci.index[0].date()} ~)")
    data['nfci_series'] = nfci

    print("[9/13] WALCL(2003+)...")
    walcl = get_fred_series('WALCL', observation_start='2003-01-01')
    if walcl is not None:
        walcl.index = walcl.index + timedelta(days=1)
        print(f"  [WALCL] 共 {len(walcl)} 行")
    data['walcl_series'] = walcl

    print("[10/13] CPI(2y,只需算同比)...")
    data['cpi_series'] = get_fred_series('CPIAUCSL', observation_start='1990-01-01')

    print("[11/13] MFG制造业新订单(2y)...")
    data['mfg_series'] = get_fred_series('NEWORDER', observation_start='2000-01-01')

    print("[12/13] OIL原油价格(★ 修复:FRED 长历史 + Yahoo 实时覆盖)...")
    data['oil_series'] = get_oil_combined()

    print("[13/13] Forward PE...")
    data['forward_pe'] = get_forward_pe()

    print("=" * 50)
    print("数据抓取完成")
    print("=" * 50)
    return data
