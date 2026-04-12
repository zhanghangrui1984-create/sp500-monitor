# ══════════════════════════════════════════════
# 标普500监控系统 v10.1 — 云端主程序（GitHub Actions用）
# ══════════════════════════════════════════════

import os
import json
import traceback
from datetime import datetime

# 从环境变量注入密钥
config_content = f"""
FRED_API_KEY     = "{os.environ.get('FRED_API_KEY', '')}"
EMAIL_SENDER     = "{os.environ.get('EMAIL_ADDRESS', '')}"
EMAIL_RECEIVER   = "{os.environ.get('EMAIL_ADDRESS', '')}"
EMAIL_PASSWORD   = "{os.environ.get('EMAIL_PASSWORD', '')}"
CURRENT_POSITION = ""
ENTRY_DATE       = ""
ENTRY_SP         = 0
SC4_IMMUNE_UNTIL = ""
TLT_HOLDING      = False
LOG_DIR          = "logs"
DATA_DIR         = "data"
"""
with open('config.py', 'w') as f:
    f.write(config_content)

os.makedirs('logs', exist_ok=True)
os.makedirs('data', exist_ok=True)

from sp500_data_fetcher  import fetch_all_data
from sp500_signal_engine import compute_signals
from sp500_notifier      import send_email_with_attachment
from sp500_report_generator import generate_report
from sp500_cache_manager import (load_db, update_db, get_eps_signals,
                                  db_status, backfill_eps)

import sp500_cache_manager
sp500_cache_manager.DB_FILE = 'data/sp500_realtime_db.csv'

def save_log(snapshot):
    date_str = snapshot.get('date', datetime.today().strftime('%Y-%m-%d'))
    log_path = f"logs/{date_str}.json"
    clean = {k: v for k, v in snapshot.items()
             if isinstance(v, (bool, int, float, str)) or v is None}
    with open(log_path, 'w', encoding='utf-8') as f:
        json.dump(clean, f, ensure_ascii=False, indent=2)
    print(f"  ✅ 日志已保存：{log_path}")

def run():
    print()
    print("╔══════════════════════════════════════════════╗")
    print("║  标普500监控系统 v10.1 — GitHub Actions运行   ║")
    print(f"║  运行时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC       ║")
    print("╚══════════════════════════════════════════════╝")
    print()

    print("检查数据库...")
    db_status()

    try:
        data = fetch_all_data()

        db    = load_db()
        sp_val= float(data['sp500_series'].iloc[-1]) if data.get('sp500_series') is not None else None
        pe_val= data.get('forward_pe')

        # 若DB历史不足42行，用yfinance历史一次性初始化
        if len(db) < 42 and data.get('sp500_series') is not None:
            print("DB历史不足，正在初始化历史数据...")
            import pandas as pd, numpy as np
            sp_hist = data['sp500_series'].sort_index()
            pe_init = pe_val or 21.0
            hist_db = pd.DataFrame({
                'sp500':       sp_hist,
                'forward_pe':  pe_init,
                'forward_eps': sp_hist / pe_init,
            })
            hist_db.index.name = 'date'
            for col in ['erp','vix','nfci','hy_spread','y_spread',
                        'fed_rate','real_rate','cpi_yoy','mfg_yoy']:
                hist_db[col] = np.nan
            if len(db) > 0:
                hist_db = hist_db[~hist_db.index.isin(db.index)]
                hist_db = pd.concat([hist_db, db]).sort_index()
            hist_db.to_csv(sp500_cache_manager.DB_FILE)
            db = hist_db
            print(f"  历史DB初始化完成：{len(hist_db)}行")

        # 计算EPS信号
        e_plus, e_plus2, e_minus, e_minus2 = get_eps_signals(db, sp_val, pe_val)
        data['e_plus_from_cache']  = e_plus
        data['e_plus2_from_cache'] = e_plus2
        data['e_minus_from_cache'] = e_minus
        data['e_minus2_from_cache']= e_minus2

        print("\n计算模型信号中...")
        snapshot = compute_signals(data)

        # 计算今日EPS并写入DB
        eps_val = (sp_val / pe_val) if (sp_val and pe_val) else None
        update_db(snapshot, sp_val=sp_val, pe_val=pe_val, eps_val=eps_val)

        # 回填历史EPS
        backfill_eps(pe_val)

        # 回填历史PE（若DB中PE为常数则从multpl.com补充真实历史数据）
        try:
            from sp500_backfill_pe import fetch_pe_from_multpl, fetch_pe_from_gurufocus, backfill_pe_to_db
            import sp500_cache_manager as _scm
            _pe_db = pd.read_csv(_scm.DB_FILE, index_col='date', parse_dates=True)
            _pe_unique = _pe_db['forward_pe'].dropna().nunique() if 'forward_pe' in _pe_db.columns else 0
            if _pe_unique <= 3:  # PE为常数，需要回填
                print("检测到PE历史为常数，尝试回填真实PE数据...")
                _pe_hist = fetch_pe_from_multpl() or fetch_pe_from_gurufocus()
                if _pe_hist is not None:
                    backfill_pe_to_db(_pe_hist)
                    # 重新计算P+/P-
                    _pe_db2 = pd.read_csv(_scm.DB_FILE, index_col='date', parse_dates=True)
                    _pe_series = _pe_db2['forward_pe'].dropna()
                    _w = min(1260, len(_pe_series))
                    if _w >= 252 and _pe_series.std() > 0.5:
                        _avg = float(_pe_series.rolling(_w, min_periods=252).mean().iloc[-1])
                        _std = float(_pe_series.rolling(_w, min_periods=252).std().iloc[-1])
                        snapshot['P_plus']  = bool(pe_val > _avg + _std) if pe_val else None
                        snapshot['P_minus'] = bool(pe_val < _avg - _std) if pe_val else None
                        print(f"  P+/P- 更新: PE={pe_val:.1f} 均={_avg:.1f} σ={_std:.2f} P+={snapshot['P_plus']} P-={snapshot['P_minus']}")
        except Exception as _pe_e:
            print(f"  PE回填跳过: {_pe_e}")

        save_log(snapshot)

        # 生成docx报告
        print("生成详细报告...")
        report_path = generate_report(snapshot)

        # 发送邮件
        print("发送邮件通知（含附件）...")
        send_email_with_attachment(snapshot, report_path)

        print("\n✅ 云端运行完成。\n")

    except Exception as e:
        print(f"\n❌ 运行出错：{e}")
        traceback.print_exc()

if __name__ == '__main__':
    run()
