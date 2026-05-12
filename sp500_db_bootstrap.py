# ══════════════════════════════════════════════════════════════════════════
# 标普500监控系统 — 数据库引导模块(本地/云端统一)
# ══════════════════════════════════════════════════════════════════════════
#
# 用途:每次启动时调用,确保数据库的"持久化历史数据"完整
#
# 设计原则:
#   - yfinance / FRED 的指标每次跑都全量重拉 → 不依赖 csv
#   - 真正需要持久化的只有 forward_pe 历史(来源:multpl.com / gurufocus)
#   - 每次启动 → 检查 csv → 缺什么补什么 → 增量,不重做
#
# 调用时机:在 sp500_main.py 和 sp500_main_cloud.py 的 fetch_all_data 之前
# ══════════════════════════════════════════════════════════════════════════

import os
import pandas as pd
import numpy as np


def bootstrap_database():
    """
    数据库引导:确保 csv 数据库的"持久化历史"完整。
    每次启动调用一次。

    步骤:
      1. 加载现有 csv(本地有/云端无)
      2. 用 yfinance 拉 SP500 全历史,确保 csv 有 sp500 列的完整历史
      3. 检测 forward_pe 历史:
         - 缺失(列不存在/全 NaN)
         - 不完整(只有最近几天)
         - 是常数(初始化用同一个 PE 填的)
         → 任一情况,从 multpl.com 拉历史 PE,前向填充补全
      4. 同步 forward_eps = sp500 / forward_pe
      5. 写回 csv

    返回:补全后的 db DataFrame
    """
    print("=" * 50)
    print("数据库引导:检查历史数据完整性")
    print("=" * 50)

    # 1. 加载/初始化 db
    from sp500_cache_manager import DB_FILE, load_db
    db = load_db()

    if db is None or len(db) == 0:
        print("  [bootstrap] csv 不存在或为空,首次启动")
        db = pd.DataFrame()
    else:
        print(f"  [bootstrap] csv 已存在,{len(db)} 行 ({db.index[0].date()} ~ {db.index[-1].date()})")

    # 2. 确保 SP500 历史完整
    db = _ensure_sp500_history(db)

    # 3. 确保 forward_pe 历史完整
    db = _ensure_pe_history(db)

    # 4. 同步 forward_eps
    db = _sync_eps(db)

    # 5. 写回 csv
    os.makedirs(os.path.dirname(DB_FILE) or '.', exist_ok=True)
    db.to_csv(DB_FILE)
    print(f"  [bootstrap] ✅ 数据库引导完成,共 {len(db)} 行,保存到 {DB_FILE}")
    print("=" * 50)
    return db


def _ensure_sp500_history(db):
    """确保 sp500 列有 1996+ 完整历史"""
    needs_refresh = (
        len(db) == 0
        or 'sp500' not in db.columns
        or db['sp500'].dropna().shape[0] < 1000  # 少于 1000 行说明不全
        or db.index[0] > pd.Timestamp('2000-01-01')  # 起点太晚
    )

    if not needs_refresh:
        print(f"  [bootstrap] SP500 历史已完整({db['sp500'].dropna().shape[0]} 行)")
        return db

    print("  [bootstrap] SP500 历史缺失/不全,从 yfinance 拉全历史...")
    try:
        import yfinance as yf
        import warnings
        warnings.filterwarnings('ignore')

        hist = yf.download("^GSPC", start="1994-01-01", auto_adjust=True, progress=False)
        if hist is None or len(hist) == 0:
            print("  [bootstrap] ⚠️ yfinance 失败,使用现有数据")
            return db
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)
        if hasattr(hist.index, 'tz') and hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        sp_series = hist['Close']
        if isinstance(sp_series, pd.DataFrame):
            sp_series = sp_series.iloc[:, 0]
        sp_series = sp_series.squeeze().dropna()

        print(f"  [bootstrap] yfinance 拉到 {len(sp_series)} 行 ({sp_series.index[0].date()} ~ {sp_series.index[-1].date()})")

        # 构建/合并到 db
        new_df = pd.DataFrame({'sp500': sp_series})
        new_df.index.name = 'date'

        if len(db) > 0:
            # 保留原 db 的其他列,只覆盖/补全 sp500
            for col in db.columns:
                if col != 'sp500':
                    new_df[col] = db[col]
            # 用新的 sp500 覆盖旧的(yfinance 更可靠)
            new_df['sp500'] = sp_series.reindex(new_df.index)

        db = new_df.sort_index()

        # 补齐其他常用空列
        for col in ['forward_pe', 'forward_eps', 'erp', 'vix', 'nfci',
                    'hy_spread', 'y_spread', 'fed_rate', 'real_rate',
                    'cpi_yoy', 'mfg_yoy']:
            if col not in db.columns:
                db[col] = np.nan

    except Exception as e:
        print(f"  [bootstrap] ⚠️ SP500 补抓失败: {e}")

    return db


def _ensure_pe_history(db):
    """
    确保 forward_pe 历史完整且有变化。
    云端首次启动或本地从未补 PE 时,PE 列全 NaN 或全是同一个值。
    """
    if 'forward_pe' not in db.columns:
        db['forward_pe'] = np.nan

    pe_series = db['forward_pe'].dropna()
    is_missing  = len(pe_series) < 100
    is_constant = (pe_series.nunique() <= 3) if len(pe_series) > 0 else True

    if not is_missing and not is_constant:
        print(f"  [bootstrap] forward_pe 历史已完整({len(pe_series)} 行,{pe_series.nunique()} 个不同值)")
        return db

    if is_missing:
        print(f"  [bootstrap] forward_pe 历史缺失(只有 {len(pe_series)} 个有效值),开始回填...")
    elif is_constant:
        print(f"  [bootstrap] forward_pe 历史是常数({pe_series.nunique()} 个不同值),开始回填真实历史...")

    # 调用 backfill 脚本里的函数
    try:
        from sp500_backfill_pe import fetch_pe_from_multpl, fetch_pe_from_gurufocus
        pe_hist = fetch_pe_from_multpl()
        if pe_hist is None or len(pe_hist) < 50:
            print("  [bootstrap] multpl 失败,尝试 gurufocus...")
            pe_hist = fetch_pe_from_gurufocus()
        if pe_hist is None or len(pe_hist) < 50:
            print("  [bootstrap] ⚠️ PE 历史回填失败,P+/P- 因子可能不可用")
            return db

        # 月度 PE 前向填充到日度
        pe_daily = pe_hist.reindex(pd.date_range(pe_hist.index[0], db.index[-1], freq='D')).ffill()
        pe_aligned = pe_daily.reindex(db.index, method='ffill')

        # 全量覆盖 forward_pe(因为我们认为 multpl 的数据更权威)
        db['forward_pe'] = pe_aligned.values
        pe_after = db['forward_pe'].dropna()
        print(f"  [bootstrap] ✅ PE 回填完成: {len(pe_after)} 行,均值={pe_after.mean():.2f},σ={pe_after.std():.2f},不同值={pe_after.nunique()} 个")

    except Exception as e:
        print(f"  [bootstrap] ⚠️ PE 回填异常: {e}")

    return db


def _sync_eps(db):
    """同步 forward_eps = sp500 / forward_pe"""
    if 'forward_eps' not in db.columns:
        db['forward_eps'] = np.nan

    if 'sp500' in db.columns and 'forward_pe' in db.columns:
        mask = db['sp500'].notna() & db['forward_pe'].notna() & (db['forward_pe'] > 0)
        db.loc[mask, 'forward_eps'] = db.loc[mask, 'sp500'] / db.loc[mask, 'forward_pe']
        n_eps = db['forward_eps'].notna().sum()
        print(f"  [bootstrap] forward_eps 同步: {n_eps} 行")
    return db


if __name__ == '__main__':
    bootstrap_database()
