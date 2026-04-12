# ══════════════════════════════════════════════
# 标普500监控系统 v10.1 — 本地主程序
# ══════════════════════════════════════════════

import os
import json
import traceback
from datetime import datetime

from config import LOG_DIR, CURRENT_POSITION, ENTRY_SP
from sp500_data_fetcher  import fetch_all_data
from sp500_signal_engine import compute_signals
from sp500_notifier      import send_email
from sp500_cache_manager import (load_db, update_db, get_eps_signals,
                                  db_status, backfill_eps)

def ensure_dirs():
    os.makedirs(LOG_DIR, exist_ok=True)

def save_log(snapshot):
    date_str = snapshot.get('date', datetime.today().strftime('%Y-%m-%d'))
    log_path = os.path.join(LOG_DIR, f"{date_str}.json")
    clean = {k: v for k, v in snapshot.items()
             if isinstance(v, (bool, int, float, str)) or v is None}
    with open(log_path, 'w', encoding='utf-8') as f:
        json.dump(clean, f, ensure_ascii=False, indent=2)
    print(f"  ✅ 日志已保存：{log_path}")

def fmt(val, fmt_str):
    try:
        import math
        f = float(val)
        if math.isnan(f): return 'N/A'
        return format(f, fmt_str)
    except: return 'N/A'

def print_summary(snapshot):
    print()
    print("═" * 60)
    print(f"  SP500监控日报 — {snapshot.get('date')}")
    print("═" * 60)
    print(f"  标普500       : {fmt(snapshot.get('sp500'), ',.0f'):>12}")
    print(f"  ATH回撤       : {fmt(snapshot.get('sp_dd_pct'), '.2f'):>11}%")
    print(f"  均线环境      : {'W+200 牛市' if snapshot.get('W200') else 'W-200 熊市'}")
    print(f"  VIX           : {fmt(snapshot.get('vix'), '.2f'):>12}")
    print(f"  V_e偏离度     : {fmt(snapshot.get('V_e'), '.2f'):>12}")
    print(f"  ERP           : {fmt(snapshot.get('erp'), '.2f'):>12}")
    print(f"  Forward PE    : {fmt(snapshot.get('forward_pe'), '.2f'):>12}")
    print(f"  E+/E+2        : {str(snapshot.get('E_plus'))}/{str(snapshot.get('E_plus2'))}")
    print(f"  E-/E-2        : {str(snapshot.get('E_minus'))}/{str(snapshot.get('E_minus2'))}")
    print(f"  NFCI          : {fmt(snapshot.get('nfci'), '.3f'):>12}")
    print(f"  HY利差        : {fmt(snapshot.get('hy_spread'), '.2f'):>12}")
    print(f"  MFG同比       : {fmt(snapshot.get('mfg_yoy'), '.1f'):>11}%")
    print(f"  TLT价格       : {fmt(snapshot.get('tlt_price'), '.2f'):>12}")
    print(f"  当前持仓      : {CURRENT_POSITION if CURRENT_POSITION else '空仓'}")
    print()

    print("  ── 入场情景 ──")
    for key, name in [('SC1A','情景1A'),('SC1D','情景1D'),
                      ('SC2A','情景2A'),('SC2D','情景2D'),
                      ('SC3A','情景3A'),('SC3B','情景3B'),
                      ('SC4A','情景4A'),('SC4B','情景4B'),('SC4C','情景4C')]:
        val = snapshot.get(key)
        mark = '🚨' if val is True else ('✗ ' if val is False else '? ')
        status = '【触发！】' if val is True else ('未触发' if val is False else '数据不足')
        print(f"  {mark} {name:8s} : {status}")

    print()
    print("  ── 离场情景 ──")
    for key, name in [('EX3','离场3（最高优先级）'),('EX1','离场1'),('EX2','离场2')]:
        val = snapshot.get(key)
        mark = '🚨' if (val is True and CURRENT_POSITION) else ('✗ ' if val is False else '? ')
        status = '【触发！】' if (val is True and CURRENT_POSITION) else ('未触发' if val is False else '数据不足/空仓')
        print(f"  {mark} {name:20s} : {status}")

    print("═" * 60)

def run():
    print()
    print("╔══════════════════════════════════════════════╗")
    print("║   标普500全周期交易模型监控系统 v10.1          ║")
    print(f"║   运行时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}          ║")
    print("╚══════════════════════════════════════════════╝")
    print()

    ensure_dirs()
    print("检查数据库...")
    db_status()

    try:
        data  = fetch_all_data()
        db    = load_db()
        sp_val= float(data['sp500_series'].iloc[-1]) if data.get('sp500_series') is not None else None
        pe_val= data.get('forward_pe')

        e_plus, e_plus2, e_minus, e_minus2 = get_eps_signals(db, sp_val, pe_val)
        data['e_plus_from_cache']  = e_plus
        data['e_plus2_from_cache'] = e_plus2
        data['e_minus_from_cache'] = e_minus
        data['e_minus2_from_cache']= e_minus2

        print("\n计算模型信号中...")
        snapshot = compute_signals(data)

        eps_val = (sp_val / pe_val) if (sp_val and pe_val) else None
        update_db(snapshot, sp_val=sp_val, pe_val=pe_val, eps_val=eps_val)
        backfill_eps(pe_val)

        print_summary(snapshot)
        save_log(snapshot)

        print("发送邮件通知...")
        send_email(snapshot)

        print("\n✅ 本次运行完成。\n")

    except Exception as e:
        print(f"\n❌ 运行出错：{e}")
        traceback.print_exc()

if __name__ == '__main__':
    run()
