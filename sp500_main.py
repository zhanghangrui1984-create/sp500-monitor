# ══════════════════════════════════════════════════════════════════════════
# 标普500监控系统 v15 — 本地主程序(T1-T10 触发器版)
# ══════════════════════════════════════════════════════════════════════════

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
    """保存简化版 snapshot 到日志(只保留 JSON 可序列化字段)"""
    date_str = snapshot.get('date', datetime.today().strftime('%Y-%m-%d'))
    log_path = os.path.join(LOG_DIR, f"{date_str}.json")

    # 压平 triggers 为简单 dict
    clean = {}
    for k, v in snapshot.items():
        if k == 'triggers':
            # 每个触发器只保留关键字段
            clean['triggers'] = {
                tid: {
                    'triggered':       r.get('triggered'),
                    'satisfied_pct':   r.get('satisfied_pct'),
                    'satisfied_count': r.get('satisfied_count'),
                    'total_must':      r.get('total_must'),
                    'alert':           r.get('alert'),
                    'missing_factors': r.get('missing_factors', []),
                }
                for tid, r in v.items()
            }
        elif k == 'factors':
            clean['factors'] = v   # 全部因子值
        elif k == 'alerts':
            clean['alerts'] = [(a, b, c) for a, b, c in v]
        elif isinstance(v, (bool, int, float, str)) or v is None:
            clean[k] = v

    with open(log_path, 'w', encoding='utf-8') as f:
        json.dump(clean, f, ensure_ascii=False, indent=2)
    print(f"  ✅ 日志已保存:{log_path}")


def fmt(val, fmt_str):
    try:
        import math
        f = float(val)
        if math.isnan(f): return 'N/A'
        return format(f, fmt_str)
    except: return 'N/A'


# 触发器显示元信息
TRIGGER_DISPLAY = [
    ('T1_2000离场',  'T1 2000 互联网泡沫顶',   'exit'),
    ('T2_2007离场',  'T2 2007 鸽派假反弹顶',   'exit'),
    ('T3_2015离场',  'T3 2015 工业衰退顶',     'exit'),
    ('T4_2022离场',  'T4 2022 加息顶',         'exit'),
    ('T5_2002入场',  'T5 2002 互联网底',       'entry'),
    ('T6_2009入场',  'T6 2009 次贷底',         'entry'),
    ('T7_2020入场',  'T7 2020 COVID 底',       'entry'),
    ('T8_2022入场',  'T8 2022 加息底',         'entry'),
    ('T9_白银坑1组', 'T9 白银坑 1 组(广义)',   'entry'),
    ('T10_白银坑2组','T10 白银坑 2 组(中庸态)','entry'),
]


def print_summary(snapshot):
    print()
    print("═" * 78)
    print(f"  SP500 触发器系统监控日报 — {snapshot.get('date')}")
    print("═" * 78)
    print(f"  SP500              : {fmt(snapshot.get('sp500'), ',.2f'):>14}")
    print(f"  ATH 回撤           : {fmt(snapshot.get('sp_dd_pct'), '.2f'):>13}%")
    print(f"  200 周均线         : {fmt(snapshot.get('ma200w'), ',.2f'):>14}")
    print(f"  Sσ_200(标准差偏离) : {fmt(snapshot.get('sigma_dev_200'), '.2f'):>13}σ")
    print(f"  VIX                : {fmt(snapshot.get('vix'), '.2f'):>14}")
    print(f"  V_c ± σ            : {fmt(snapshot.get('V_c'), '.2f')} ± {fmt(snapshot.get('V_c_sigma'), '.2f')}")
    print(f"  ERP                : {fmt(snapshot.get('erp'), '.2f'):>13}%")
    print(f"  Forward PE         : {fmt(snapshot.get('forward_pe'), '.2f'):>14}")
    print(f"  NFCI / N_c         : {fmt(snapshot.get('nfci'), '.3f')} / {fmt(snapshot.get('n_c'), '+.3f')}")
    print(f"  HY / HY_c21        : {fmt(snapshot.get('hy_spread'), '.2f')}% / {fmt(snapshot.get('hy_c21'), '+.3f')}")
    print(f"  Y 利差             : {fmt(snapshot.get('y_spread'), '.3f'):>14}")
    print(f"  联储利率           : {fmt(snapshot.get('fed_rate'), '.2f'):>13}%")
    print(f"  CPI 同比           : {fmt(snapshot.get('cpi_y'), '.2f'):>13}%")
    print(f"  WALCL 13 周变化    : {fmt(snapshot.get('walcl_13w_chg'), '+.3f'):>13}T")
    print(f"  OIL / pct5y        : {fmt(snapshot.get('oil_price'), '.1f')} / {fmt(snapshot.get('oil_pct5y'), '.0f')}%")
    print(f"  当前持仓           : {CURRENT_POSITION if CURRENT_POSITION else '空仓'}")
    print()

    triggers = snapshot.get('triggers', {})

    print("  ── 离场触发器 ─────────────────────────────────────────────────────")
    for tid, name, ttype in TRIGGER_DISPLAY:
        if ttype != 'exit': continue
        r = triggers.get(tid, {})
        triggered = r.get('triggered')
        pct = r.get('satisfied_pct', 0)
        n_sat = r.get('satisfied_count', 0)
        n_tot = r.get('total_must', 0)

        if triggered is True:
            mark = '🚨'; status = '【触发!】'
        elif triggered is False:
            if pct >= 0.9:   mark = '🟠'; status = f'高度接近 {pct*100:.0f}%'
            elif pct >= 0.7: mark = '🟡'; status = f'中度接近 {pct*100:.0f}%'
            else:            mark = '⚪'; status = f'远未触发 {pct*100:.0f}%'
        else:
            mark = '? '; status = '数据不足'
        print(f"  {mark} {name:25s}: {status:14s} ({n_sat}/{n_tot} 必有)")

    print()
    print("  ── 入场触发器 ─────────────────────────────────────────────────────")
    for tid, name, ttype in TRIGGER_DISPLAY:
        if ttype != 'entry': continue
        r = triggers.get(tid, {})
        triggered = r.get('triggered')
        pct = r.get('satisfied_pct', 0)
        n_sat = r.get('satisfied_count', 0)
        n_tot = r.get('total_must', 0)

        if triggered is True:
            mark = '🚨'; status = '【触发!】'
        elif triggered is False:
            if pct >= 0.9:   mark = '🟠'; status = f'高度接近 {pct*100:.0f}%'
            elif pct >= 0.7: mark = '🟡'; status = f'中度接近 {pct*100:.0f}%'
            else:            mark = '⚪'; status = f'远未触发 {pct*100:.0f}%'
        else:
            mark = '? '; status = '数据不足'
        print(f"  {mark} {name:25s}: {status:14s} ({n_sat}/{n_tot} 必有)")

    # 显示接近触发的"差什么"
    close_triggers = [(tid, name) for tid, name, _ in TRIGGER_DISPLAY
                      if triggers.get(tid, {}).get('satisfied_pct', 0) >= 0.7
                      and triggers.get(tid, {}).get('triggered') is not True]
    if close_triggers:
        print()
        print("  ── 接近触发详情(差什么) ──────────────────────────────────────────")
        for tid, name in close_triggers:
            r = triggers.get(tid, {})
            missing = r.get('missing_factors', [])
            print(f"  {name}:")
            for m in missing[:5]:
                print(f"      ✗ {m}")

    print("═" * 78)


def run():
    print()
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║   标普500触发器系统(V15 因子矩阵 + 10 触发器)监控              ║")
    print(f"║   运行时间:{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                            ║")
    print("╚════════════════════════════════════════════════════════════════╝")
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

        print("\n计算 V15 因子 + 评估 10 触发器...")
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
        print(f"\n❌ 运行出错:{e}")
        traceback.print_exc()


if __name__ == '__main__':
    run()
