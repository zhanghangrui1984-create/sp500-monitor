# ══════════════════════════════════════════════
# 标普500监控系统 v10.1 — 信号计算引擎
# ══════════════════════════════════════════════

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def to_series(obj):
    if obj is None: return None
    if isinstance(obj, pd.Series): return obj
    if isinstance(obj, pd.DataFrame): return obj.iloc[:, 0]
    return None

def last_val(series):
    if series is None or len(series) == 0: return None
    try:
        v = series.iloc[-1]
        return float(v) if not pd.isna(v) else None
    except: return None

def val_n_ago(series, n):
    if series is None or len(series) <= n: return None
    try:
        v = series.iloc[-(n+1)]
        return float(v) if not pd.isna(v) else None
    except: return None

def compute_signals(data, today=None):
    if today is None:
        today = pd.Timestamp(datetime.today().date())

    sp_s    = to_series(data.get('sp500_series'))
    vix_s   = to_series(data.get('vix_series'))
    tlt_s   = to_series(data.get('tlt_series'))
    y_s     = to_series(data.get('y_series'))
    f_s     = to_series(data.get('f_series'))
    r_s     = to_series(data.get('r_series'))
    hy_s    = to_series(data.get('hy_series'))
    nfci_s  = to_series(data.get('nfci_series'))
    walcl_s = to_series(data.get('walcl_series'))
    cpi_s   = to_series(data.get('cpi_series'))
    mfg_s   = to_series(data.get('mfg_series'))
    fpe_val = data.get('forward_pe')

    if sp_s is None or len(sp_s) < 100:
        raise ValueError("标普500数据不足")

    sp_s = sp_s.sort_index().dropna()

    # ── 标普500基础指标
    sp_now    = float(sp_s.iloc[-1])
    sp_ath    = float(sp_s.expanding().max().iloc[-1])
    sp_dd_pct = (sp_ath - sp_now) / sp_ath * 100

    # 200周均线（严格只用周五收盘价）
    sp_weekly = sp_s[sp_s.index.weekday == 4]
    ma200w    = float(sp_weekly.rolling(200, min_periods=100).mean().iloc[-1]) if len(sp_weekly) >= 100 else float(sp_s.rolling(1000, min_periods=200).mean().iloc[-1])
    W200      = bool(sp_now >= ma200w)

    # 18个月最高点
    sp_18m_hi = float(sp_s.rolling(378, min_periods=100).max().iloc[-1])
    S_ddp     = bool(sp_now >= sp_18m_hi * 0.85)   # S_dd+

    # 月度趋势 S+1 / S-1
    dr    = sp_s.pct_change()
    up21  = dr.rolling(21).apply(lambda x: (x > 0).sum(), raw=True)
    dn21  = dr.rolling(21).apply(lambda x: (x < 0).sum(), raw=True)
    prev21 = val_n_ago(sp_s, 21)
    S_p1  = bool(prev21 and sp_now > prev21 and up21.iloc[-1] >= 11)
    S_m1  = bool(prev21 and sp_now < prev21 and dn21.iloc[-1] >= 11)

    # S+（从ATH后低点反弹）
    sp_arr = sp_s.values
    ath_idx = np.maximum.accumulate(np.arange(len(sp_arr)) * (sp_arr == np.maximum.accumulate(sp_arr)))
    trough  = np.array([sp_arr[int(ath_idx[i]):i+1].min() for i in range(len(sp_arr))])
    sp_pp   = float((sp_arr[-1] - trough[-1]) / trough[-1] * 100) if trough[-1] > 0 else 0.0

    # 5日暴跌
    prev5       = val_n_ago(sp_s, 5)
    sp_5d_drop  = bool(prev5 and (sp_now - prev5) / prev5 <= -0.10)

    # ── VIX指标
    vix_now = None
    V_p = None; V_c = None; V_e = None; V_e15 = False
    V_em20 = None; V_10dn = None; V_new = False; V_calm = False

    if vix_s is not None and len(vix_s) > 50:
        vix_s   = vix_s.sort_index().dropna()
        vix_now = float(vix_s.iloc[-1])

        # V_c / V_p：基于S回撤<10%时期的VIX均值
        sp_aligned  = sp_s.reindex(vix_s.index, method='ffill')
        sp_ath_al   = sp_aligned.expanding().max()
        dd_al       = (sp_ath_al - sp_aligned) / sp_ath_al * 100

        # 过去252日内S回撤<10%的VIX样本
        i    = len(vix_s) - 1
        start = max(0, i - 252 + 1)
        v_calm_s = vix_s.iloc[start:i+1][dd_al.iloc[start:i+1] < 10].dropna()
        if len(v_calm_s) < 63:
            j = start - 1
            while j >= 0 and len(v_calm_s) < 63:
                if dd_al.iloc[j] < 10:
                    v_calm_s = pd.concat([pd.Series([vix_s.iloc[j]]), v_calm_s])
                j -= 1
        if len(v_calm_s) >= 63:
            V_c = float(v_calm_s.mean())
            V_p = float(v_calm_s.mean() + v_calm_s.std())

        if V_p and V_c and vix_now:
            V_e    = (vix_now - V_p) / V_p * 100
            V_e15  = V_e >= 15

            ve_series  = (vix_s - V_p) / V_p * 100
            V_em20     = float(ve_series.rolling(20, min_periods=1).max().iloc[-1])

            vix_pk10   = float(vix_s.rolling(10, min_periods=1).max().iloc[-1])
            V_10dn_val = (vix_now - vix_pk10) / vix_pk10 * 100
            V_10dn     = V_10dn_val  # 负值=回落

            # V_new：V_em20≥18% AND V_10↓≥10% AND V≤V_p
            V_new  = bool(V_em20 >= 18 and V_10dn_val <= -10 and vix_now <= V_p)
            # V_calm：V≤V_c（平静期基线）
            V_calm = bool(vix_now <= V_c)

    # ── 宏观指标
    y_now   = last_val(y_s)
    Y_plus  = bool(y_now >= 0) if y_now is not None else None
    Y_minus = bool(y_now < 0)  if y_now is not None else None

    f_now  = last_val(f_s)
    F_plus = None; F_minus = None; F0 = None
    if f_s is not None and f_now is not None:
        f_s2       = f_s.sort_index()
        F_ma252    = float(f_s2.rolling(252, min_periods=63).mean().iloc[-1])
        F_ma90_max = float(f_s2.rolling(90,  min_periods=21).max().iloc[-1])
        F_plus     = bool(f_now > F_ma252 + 0.25)
        F_minus    = bool(f_now < F_ma252 - 0.25)
        F0         = bool(f_now <= F_ma252 and f_now <= F_ma90_max)

    r_now  = last_val(r_s)

    hy_now = last_val(hy_s)
    hy_c20 = None
    if hy_s is not None and len(hy_s) >= 21:
        hy_c20 = float(hy_s.sort_index().iloc[-1] - hy_s.sort_index().iloc[-21])

    nfci_now = last_val(nfci_s)
    N_c_4w = None; N_plus_015 = None; N_minus_015 = None
    N_lt04 = None; N_front = None; N_c_gt01 = None; N_c_ge01 = None
    N_c_neg = None; N_ge02 = None

    if nfci_s is not None and nfci_now is not None and len(nfci_s) >= 5:  # 周度，>=5保证4周变化可算
        nfci_s2    = nfci_s.sort_index()
        N_c_4w_val = float(nfci_s2.iloc[-1] - nfci_s2.iloc[-5])  # NFCI周度数据，4周前=第5个到最新
        N_c_4w      = N_c_4w_val
        N_plus_015  = bool(N_c_4w_val <= -0.15)
        N_minus_015 = bool(N_c_4w_val >= 0.15)
        N_lt04      = bool(nfci_now < -0.4)
        N_c_gt01    = bool(N_c_4w_val > 0.1)
        N_c_ge01    = bool(N_c_4w_val >= 0.1)
        N_c_neg     = bool(N_c_4w_val < 0)
        N_ge02      = bool(nfci_now >= 0.2)
        # N_front = (N+≥0.15 AND N<0.1) OR N<-0.4
        N_front = bool((N_plus_015 and nfci_now < 0.1) or N_lt04)

    # ── WALCL (W1000)
    W1000 = False
    if walcl_s is not None and len(walcl_s) >= 14:
        ws2    = walcl_s.sort_index() / 1e6  # 转换为万亿$
        chg13w = (ws2 - ws2.shift(13)).dropna()  # 去掉NaN，避免误报
        trig   = (chg13w >= 0.1)
        # 触发后13周内持续有效
        eff = False
        for i in range(max(0, len(trig)-14), len(trig)):
            if bool(trig.iloc[i]):
                eff = True
                break
        W1000 = eff

    # ── CPI（延迟45天）
    cpi_yoy_now = None; CPI_gt4 = False
    if cpi_s is not None and len(cpi_s) >= 13:
        cpi_s2    = cpi_s.sort_index()
        cpi_pub   = today - timedelta(days=45)
        cpi_avail = cpi_s2[cpi_s2.index <= cpi_pub]
        if len(cpi_avail) >= 13:
            cpi_yoy     = (cpi_avail / cpi_avail.shift(12) - 1) * 100
            cpi_yoy_now = float(cpi_yoy.iloc[-1])
            CPI_gt4     = bool(cpi_yoy_now > 4.0)

    # ── MFG同比
    mfg_yoy_now = None; MFG_lt3 = False
    if mfg_s is not None and len(mfg_s) >= 13:
        mfg_s2      = mfg_s.sort_index()
        mfg_yoy     = (mfg_s2 / mfg_s2.shift(12) - 1) * 100
        mfg_yoy_now = float(mfg_yoy.iloc[-1])
        MFG_lt3     = bool(mfg_yoy_now < -3.0)

    # ── TLT
    tlt_now = last_val(tlt_s)

    # ── ERP & PE
    ERP = None; P_plus = None; P_minus = None
    e_plus = data.get('e_plus_from_cache')
    e_plus2= data.get('e_plus2_from_cache')
    e_minus= data.get('e_minus_from_cache')
    e_minus2=data.get('e_minus2_from_cache')

    if fpe_val and fpe_val > 0:
        if r_now is not None:
            ERP = (1.0 / fpe_val * 100) - r_now
        try:
            import sp500_cache_manager as _cm
            _db      = pd.read_csv(_cm.DB_FILE, index_col='date', parse_dates=True)
            _pe_hist = _db['forward_pe'].dropna()
            _window  = min(1260, len(_pe_hist))
            # 注意：初始化时PE历史全为常数，std≈0，P+/P-无意义
            # 需积累真实历史PE才可靠（至少252行不同PE值）
            _pe_unique = _pe_hist.nunique()
            if _window >= 252 and _pe_unique >= 10:
                _pe_avg = float(_pe_hist.rolling(_window, min_periods=252).mean().iloc[-1])
                _pe_std = float(_pe_hist.rolling(_window, min_periods=252).std().iloc[-1])
                if _pe_std > 0.5:  # std太小说明数据质量不足
                    P_plus  = bool(fpe_val > _pe_avg + _pe_std)
                    P_minus = bool(fpe_val < _pe_avg - _pe_std)
                    print(f"  [P+/P-] PE={fpe_val:.1f} 均={_pe_avg:.1f} σ={_pe_std:.1f} → P+={P_plus} P-={P_minus}")
                else:
                    print(f"  [P+/P-] PE历史方差过小(σ={_pe_std:.2f})，数据质量不足，跳过")
            else:
                print(f"  [P+/P-] 历史PE数据不足({_window}行，{_pe_unique}个不同值)，使用简单阈值")
                # 兜底：使用固定历史均值阈值（标普500 Forward PE历史中枢约16~22）
                P_plus  = bool(fpe_val > 24.0)   # 高于1σ上轨近似
                P_minus = bool(fpe_val < 16.0)   # 低于1σ下轨近似
                print(f"  [P+/P-] 兜底: PE={fpe_val:.1f} P+={P_plus} P-={P_minus}")
        except Exception as e:
            print(f"  [P+/P-] 计算失败: {e}")
            # 异常兜底
            if fpe_val:
                P_plus  = bool(fpe_val > 24.0)
                P_minus = bool(fpe_val < 16.0)

    # ── 门槛
    e15 = bool(ERP >= 1.5) if ERP is not None else None
    e25 = bool(ERP >= 2.5) if ERP is not None else None
    ERP_lt0  = bool(ERP < 0.0) if ERP is not None else None
    ERP_lt10 = bool(ERP < 1.0) if ERP is not None else None
    ERP_lt40 = bool(ERP < 4.0) if ERP is not None else None

    # N+≥0.3（不允许）
    N_plus_03 = bool(N_c_4w <= -0.30) if N_c_4w is not None else False

    # ══════════════════════════════════════════════
    # 入场情景（v10.1）
    # ══════════════════════════════════════════════

    def _e2_ok():  return bool(e_plus2) if e_plus2 is not None else None
    def _e1_ok():  return bool(e_plus)  if e_plus  is not None else None
    def _ea_ok():  return bool(e_plus or e_plus2) if (e_plus is not None or e_plus2 is not None) else None

    def _trigger1():
        # 催化剂（1选1）：Y+ / N_front / P- / V_new
        return bool(Y_plus or N_front or P_minus or V_new) if Y_plus is not None else None

    def _trigger2():
        # 催化剂（4选2）
        cnt = sum([bool(Y_plus or False), bool(N_front or False),
                   bool(P_minus or False), bool(V_new or False)])
        return bool(cnt >= 2) if Y_plus is not None else None

    # 公共必要条件（1系列）：W+200 AND 回撤区间 AND ERP≥1.5% AND NOT N+≥0.3 AND NOT P+
    def _base1(low, high):
        if e15 is None: return None
        # P_plus=None时当False处理（无历史PE数据时保守假设估值未过热）
        pp = bool(P_plus) if P_plus is not None else False
        return bool(W200 and low <= sp_dd_pct < high and e15 and not N_plus_03 and not pp)

    def _base1_d():
        if e15 is None: return None
        pp = bool(P_plus) if P_plus is not None else False
        return bool(W200 and sp_dd_pct >= 23.5 and e15 and not N_plus_03 and not pp)

    # 情景1A（半仓，E+2，1选1）
    SC1A = None
    b1 = _base1(10, 23.5)
    if b1 is not None and _e2_ok() is not None and _trigger1() is not None:
        SC1A = bool(b1 and _e2_ok() and _trigger1())

    # 情景1D（全仓，回撤≥23.5%）
    SC1D = None
    b1d = _base1_d()
    if b1d is not None and _e2_ok() is not None and _trigger1() is not None:
        SC1D = bool(b1d and _e2_ok() and _trigger1())

    # 情景2A（半仓，E+，2选2）
    SC2A = None
    if b1 is not None and _e1_ok() is not None and _trigger2() is not None:
        SC2A = bool(b1 and _e1_ok() and _trigger2())

    # 情景2D（全仓，回撤≥23.5%，E+，2选2）
    SC2D = None
    if b1d is not None and _e1_ok() is not None and _trigger2() is not None:
        SC2D = bool(b1d and _e1_ok() and _trigger2())

    # 情景3A（全仓，回撤≥5%，5选3）
    SC3A = None
    if e15 is not None and N_front is not None:
        score3a = sum([
            bool(e_plus or e_plus2) if (e_plus is not None) else 0,
            bool(F0 or F_minus) if F0 is not None else 0,
            bool(e25) if e25 is not None else 0,
            bool(S_p1),
            bool(V_calm or Y_plus) if Y_plus is not None else 0,
        ])
        SC3A = bool(W200 and sp_dd_pct >= 5 and e15 and N_front and score3a >= 3)

    # 情景3B（全仓，回撤<5%，4选2，含CPI/R限制）
    SC3B = None
    if e15 is not None and N_front is not None and _ea_ok() is not None:
        cpi_r_block = bool(CPI_gt4 and r_now is not None and r_now < 0)
        score3b = sum([
            bool(F0 or F_minus) if F0 is not None else 0,
            bool(e25) if e25 is not None else 0,
            bool(S_p1),
            bool(V_calm or Y_plus) if Y_plus is not None else 0,
        ])
        SC3B = bool(W200 and sp_dd_pct < 5 and e15 and N_front and
                    _ea_ok() and not cpi_r_block and score3b >= 2)

    # 情景4A（极端危机，深度）
    SC4A = None
    if hy_now is not None and V_e is not None and nfci_now is not None:
        sc4a_n = bool(N_c_neg or nfci_now < 0)
        SC4A   = bool(sp_dd_pct > 35 and hy_now > 10.0 and V_e > 80.0 and sc4a_n)

    # 情景4B（QE触发）
    SC4B = bool(sp_dd_pct > 50 and W1000)

    # 情景4C（危机+QE）
    SC4C = None
    if hy_now is not None and V_e is not None and nfci_now is not None:
        SC4C = bool(sp_dd_pct > 30 and 8.0 < hy_now < 12.0 and
                    V_e > 80.0 and nfci_now < 1.0 and W1000)

    # ══════════════════════════════════════════════
    # 离场情景（v10.1）
    # ══════════════════════════════════════════════
    from config import ENTRY_SP, SC4_IMMUNE_UNTIL
    sp_t_up = bool(sp_now > float(ENTRY_SP)) if ENTRY_SP and float(ENTRY_SP) > 0 else None

    def exit_ok():
        # 检查SC4免疫期
        if SC4_IMMUNE_UNTIL:
            try:
                immune_end = pd.Timestamp(SC4_IMMUNE_UNTIL)
                if today <= immune_end:
                    return False
            except: pass
        return True

    # 离场1：W+200 AND (Y- or E-/E-2) AND F+ AND ERP<0 AND (N-≥0.15 or P+)
    EX1 = None
    if all(v is not None for v in [Y_minus, F_plus, ERP_lt0, N_minus_015]):  # P_plus允许None
        e_minus_any = bool(e_minus or e_minus2) if e_minus is not None else False
        gate = bool(W200 and (Y_minus or e_minus_any) and F_plus and ERP_lt0)
        cnt  = sum([bool(N_minus_015 or False), bool(P_plus or False)])
        EX1  = bool(gate and cnt >= 1 and exit_ok())

    # 离场2：W+200 AND S-1 AND S_dd+ AND N_c≥0.1 AND V>V_p AND 7选4
    EX2 = None
    if all(v is not None for v in [Y_minus, N_minus_015, F_plus, ERP_lt10]):  # P_plus允许None
        e_minus2_ok = bool(e_minus2) if e_minus2 is not None else False
        gate2 = bool(W200 and S_m1 and S_ddp and N_c_ge01 and
                     vix_now is not None and V_p is not None and vix_now > V_p)
        cnt2 = sum([
            bool(Y_minus or False),
            bool(N_minus_015 or False),
            bool(F_plus or False),
            bool(e_minus2_ok),
            bool(P_plus or False),
            bool(ERP_lt10 or False),
            bool(MFG_lt3),
        ])
        EX2 = bool(gate2 and cnt2 >= 4 and exit_ok())

    # 离场3（最高优先级）：(Y- or E-2) AND N≥0.2 AND N_c>0.1 AND ERP<4%
    EX3 = None
    if all(v is not None for v in [Y_minus, N_ge02, N_c_gt01, ERP_lt40]):
        e_minus2_ok = bool(e_minus2) if e_minus2 is not None else False
        EX3 = bool((Y_minus or e_minus2_ok) and N_ge02 and N_c_gt01 and ERP_lt40 and exit_ok())

    # ── 空仓期F-判断（用于TLT策略）
    F_minus_now = F_minus

    return {
        'date':         today.strftime('%Y-%m-%d'),
        'sp500':        round(sp_now, 2),
        'sp_dd_pct':    round(sp_dd_pct, 2),
        'ma200w':       round(ma200w, 2),
        'W200':         W200,
        'vix':          round(vix_now, 2) if vix_now else None,
        'V_p':          round(V_p, 2) if V_p else None,
        'V_c':          round(V_c, 2) if V_c else None,
        'V_e':          round(V_e, 2) if V_e else None,
        'V_em20':       round(V_em20, 2) if V_em20 else None,
        'V_new':        V_new,  'V_calm': V_calm,
        'y_spread':     round(y_now, 3) if y_now is not None else None,
        'fed_rate':     round(f_now, 2) if f_now is not None else None,
        'real_rate':    round(r_now, 2) if r_now is not None else None,
        'hy_spread':    round(hy_now, 2) if hy_now is not None else None,
        'hy_c20':       round(hy_c20, 3) if hy_c20 is not None else None,
        'nfci':         round(nfci_now, 3) if nfci_now is not None else None,
        'N_c_4w':       round(N_c_4w, 3) if N_c_4w is not None else None,
        'forward_pe':   round(fpe_val, 2) if fpe_val else None,
        'erp':          round(ERP, 2) if ERP is not None else None,
        'cpi_yoy':      round(cpi_yoy_now, 2) if cpi_yoy_now is not None else None,
        'mfg_yoy':      round(mfg_yoy_now, 2) if mfg_yoy_now is not None else None,
        'tlt_price':    round(tlt_now, 2) if tlt_now is not None else None,
        'S_p1':         S_p1, 'S_m1': S_m1, 'S_ddp': S_ddp, 'sp_pp': round(sp_pp, 2),
        'Y_plus':       Y_plus, 'Y_minus': Y_minus,
        'F_plus':       F_plus, 'F_minus': F_minus, 'F0': F0,
        'N_plus_015':   N_plus_015, 'N_minus_015': N_minus_015,
        'N_front':      N_front, 'N_lt04': N_lt04,
        'N_c_gt01':     N_c_gt01, 'N_c_ge01': N_c_ge01,
        'N_c_neg':      N_c_neg, 'N_ge02': N_ge02,
        'P_plus':       P_plus, 'P_minus': P_minus,
        'W1000':        W1000,
        'CPI_gt4':      CPI_gt4, 'MFG_lt3': MFG_lt3,
        'E_plus':       e_plus,  'E_plus2':  e_plus2,
        'E_minus':      e_minus, 'E_minus2': e_minus2,
        'sp_t_up':      sp_t_up,
        # 入场
        'SC1A': SC1A, 'SC1D': SC1D,
        'SC2A': SC2A, 'SC2D': SC2D,
        'SC3A': SC3A, 'SC3B': SC3B,
        'SC4A': SC4A, 'SC4B': SC4B, 'SC4C': SC4C,
        # 离场
        'EX1': EX1, 'EX2': EX2, 'EX3': EX3,
        # TLT
        'F_minus_now': F_minus_now,
    }
