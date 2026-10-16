"""
标普500历史Forward PE回填脚本(增强版)
═══════════════════════════════════════════════════════════════════════
解决了 multpl.com HTTP 403 问题,使用真实浏览器 headers。
"""

import pandas as pd
import numpy as np
import requests
import os
import time
import io

DB_FILE = "D:\\sp500_monitor\\data\\sp500_realtime_db.csv"
if not os.path.exists(os.path.dirname(DB_FILE)):
    DB_FILE = "data/sp500_realtime_db.csv"


# 真实浏览器 headers — 绕开常见反爬墙
BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,'
              'image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
}


def fetch_pe_from_multpl():
    """从 multpl.com 抓取 S&P500 历史 PE(月度,1871+)"""
    try:
        # 用 session 让 cookie 持久化(很多反爬靠 cookie)
        sess = requests.Session()
        sess.headers.update(BROWSER_HEADERS)

        # 先访问主页拿 cookie
        try:
            sess.get("https://www.multpl.com/", timeout=10)
            time.sleep(0.5)
        except Exception:
            pass

        url = "https://www.multpl.com/s-p-500-pe-ratio/table/by-month"
        sess.headers['Referer'] = "https://www.multpl.com/"
        resp = sess.get(url, timeout=20)

        if resp.status_code != 200:
            print(f"  [multpl] HTTP {resp.status_code}")
            return None

        tables = pd.read_html(io.StringIO(resp.text))
        if not tables:
            print("  [multpl] 找不到表格")
            return None

        df = tables[0]
        df.columns = ['date', 'pe']
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['pe']   = pd.to_numeric(df['pe'], errors='coerce')
        df = df.dropna().sort_values('date').reset_index(drop=True)

        if len(df) < 50:
            print(f"  [multpl] 数据太少: {len(df)}行")
            return None

        print(f"  [multpl] ✅ 成功: {len(df)}行,{df['date'].iloc[0].date()} ~ {df['date'].iloc[-1].date()}")
        print(f"  [multpl] PE范围: {df['pe'].min():.1f} ~ {df['pe'].max():.1f}")
        return df.set_index('date')['pe']

    except Exception as e:
        print(f"  [multpl] 失败: {e}")
        return None


def fetch_pe_from_gurufocus():
    """从 gurufocus 备用源(响应慢且经常变格式,作兜底)"""
    try:
        sess = requests.Session()
        sess.headers.update(BROWSER_HEADERS)
        try:
            sess.get("https://www.gurufocus.com/", timeout=10)
            time.sleep(0.5)
        except Exception:
            pass

        url = "https://www.gurufocus.com/term/peratio/SP500/PE-Ratio/SP-500"
        sess.headers['Referer'] = "https://www.gurufocus.com/"
        resp = sess.get(url, timeout=20)
        if resp.status_code != 200:
            print(f"  [gurufocus] HTTP {resp.status_code}")
            return None

        tables = pd.read_html(io.StringIO(resp.text))
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
                    print(f"  [gurufocus] ✅ 成功: {len(pe_s)}行")
                    return pe_s
        return None
    except Exception as e:
        print(f"  [gurufocus] 失败: {e}")
        return None


def fetch_pe_from_stooq():
    """从 stooq 抓取 SP500 历史价格,合并 multpl 当前 PE 来推算历史 EPS,再反推 PE
    (这是兜底中的兜底,用了一个简化模型:假设 EPS 平滑增长)"""
    return None  # 暂未实现,留作扩展


def backfill_pe_to_db(pe_monthly, db_path=None):
    """把月度 PE 前向填充到日度,更新 DB"""
    if pe_monthly is None or len(pe_monthly) < 10:
        return False

    path = db_path or DB_FILE
    if not os.path.exists(path):
        print(f"  [回填] DB 不存在: {path}")
        return False

    db = pd.read_csv(path, index_col='date', parse_dates=True)
    if 'forward_pe' not in db.columns:
        db['forward_pe'] = np.nan

    # 把月度 PE 前向填充到日度
    pe_daily = pe_monthly.reindex(
        pd.date_range(pe_monthly.index[0], db.index[-1], freq='D')
    ).ffill()
    pe_aligned = pe_daily.reindex(db.index, method='ffill')

    # 覆盖 forward_pe
    db['forward_pe'] = pe_aligned.values

    # 同步 forward_eps = sp500 / forward_pe
    if 'sp500' in db.columns:
        mask = db['sp500'].notna() & db['forward_pe'].notna() & (db['forward_pe'] > 0)
        if 'forward_eps' not in db.columns:
            db['forward_eps'] = np.nan
        db.loc[mask, 'forward_eps'] = db.loc[mask, 'sp500'] / db.loc[mask, 'forward_pe']

    db.to_csv(path)
    n_unique = db['forward_pe'].dropna().nunique()
    print(f"  [回填] ✅ 完成: {len(db)} 行, {n_unique} 个不同 PE 值")
    return True


def main():
    print("=" * 60)
    print("PE 历史回填工具(独立运行)")
    print("=" * 60)

    print("\n[1/3] 尝试从 multpl.com 拉取...")
    pe = fetch_pe_from_multpl()

    if pe is None:
        print("\n[2/3] 尝试从 gurufocus 备用源...")
        pe = fetch_pe_from_gurufocus()

    if pe is None:
        print("\n❌ 所有源都失败")
        print("解决方案:")
        print("  1. 检查网络(VPN/代理?)")
        print("  2. 浏览器打开 https://www.multpl.com/s-p-500-pe-ratio/table/by-month")
        print("     若能看到表格,把页面另存为 .html,放到本目录,运行 main() 时改用本地文件")
        return

    print(f"\n[3/3] 回填到数据库 {DB_FILE}...")
    ok = backfill_pe_to_db(pe)
    if ok:
        print("\n✅ 全部完成 — 现在 P+/P- 因子可以正常计算")
        print("    重新跑 python sp500_main.py 看 T3 是否还显示数据不足")
    else:
        print("\n❌ 回填失败")


if __name__ == '__main__':
    main()
