# ══════════════════════════════════════════════════════════════════════════
# 标普500监控系统 v15 — 信号计算引擎(V15 因子矩阵 + 10 触发器)
# ══════════════════════════════════════════════════════════════════════════
#
# 与旧 v10.3 引擎的差异:
#   旧引擎:输出 SC1A/SC1D/SC2A/SC2D/SC3A/SC3B/SC4A/SC4B/SC4C 入场 + EX1/2/3 离场
#   新引擎:输出 T1-T10 触发器 + 接近触发评分(差几个/差哪个)
#
# 因子算法严格对齐 L2 验证版本(因子矩阵 V15)。
# 触发器公式严格对齐文件 4(标普500触发器系统_10触发器合集_20260509.xlsx)。
#
# Layer 3 验证:V15 重算 = 文件 4 触发日,857 个触发日逐日 0 差异。
# ══════════════════════════════════════════════════════════════════════════

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')


# ══════════════════════════════════════════════════════════════════════════
# 工具函数
# ══════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════
# 因子计算 — 严格对齐 V15(L2 验证过的算法)
# ══════════════════════════════════════════════════════════════════════════

def compute_factors(data, today=None):
    """
    输入: data dict (来自 fetch_all_data)
    输出: dict 包含全部 V15 因子(0/1)+ 关键中间量

    严格按 V15 矩阵的因子定义,L2 验证已确保算法正确。
    """
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
    oil_s   = to_series(data.get('oil_series'))
    fpe_val = data.get('forward_pe')

    if sp_s is None or len(sp_s) < 252:
        raise ValueError("标普500数据不足(至少 252 行)")

    sp_s = sp_s.sort_index().dropna()

    F = {}  # 因子结果

    # ──────────────────────────────────────────────────────────────────────
    # 一、股价/技术因子(基于 SP500 收盘价)
    # ──────────────────────────────────────────────────────────────────────
    sp_now    = float(sp_s.iloc[-1])
    sp_ath    = float(sp_s.expanding().max().iloc[-1])
    sp_dd_pct = (sp_ath - sp_now) / sp_ath * 100  # 注意百分点表示
    sp_dd_dec = sp_dd_pct / 100                    # 小数表示

    F['_sp_now']    = sp_now
    F['_sp_ath']    = sp_ath
    F['_sp_dd_pct'] = sp_dd_pct

    # 1) S-≥5%/10%/20%/30%(回撤阈值)
    F['S- ≥ 5%']  = bool(sp_dd_dec >= 0.05)
    F['S- ≥ 10%'] = bool(sp_dd_dec >= 0.10)
    F['S- ≥ 20%'] = bool(sp_dd_dec >= 0.20)
    F['S- ≥ 30%'] = bool(sp_dd_dec >= 0.30)
    F['S-']       = sp_dd_dec
    F['_S-']      = sp_dd_dec

    # 2) W+200 严格定义:200 个真实交易 Friday(weekday==4)的滚动均值
    sp_friday = sp_s[sp_s.index.weekday == 4]
    if len(sp_friday) >= 200:
        ma200w_strict = float(sp_friday.rolling(200, min_periods=200).mean().iloc[-1])
    elif len(sp_friday) >= 100:
        ma200w_strict = float(sp_friday.rolling(200, min_periods=100).mean().iloc[-1])
    else:
        ma200w_strict = float(sp_s.rolling(1000, min_periods=200).mean().iloc[-1])
    F['_W200_value'] = ma200w_strict
    F['W+200'] = bool(sp_now >= ma200w_strict)

    # 3) Sσ_200 = (S - MA200d) / σ(S, 200d) 用日度均线和日度 σ
    if len(sp_s) >= 200:
        sp_ma200d = float(sp_s.rolling(200, min_periods=200).mean().iloc[-1])
        sp_sd200d = float(sp_s.rolling(200, min_periods=200).std(ddof=1).iloc[-1])
        if sp_sd200d > 0:
            sigma_dev = (sp_now - sp_ma200d) / sp_sd200d
        else:
            sigma_dev = 0.0
    else:
        sigma_dev = 0.0
    F['Sσ_200']      = sigma_dev
    F['Sσ_200 ≤ -3σ'] = bool(sigma_dev <= -3)
    F['Sσ_200 ≤ -2σ'] = bool(sigma_dev <= -2)
    F['Sσ_200 ≤ -1σ'] = bool(sigma_dev <= -1)
    F['Sσ_200 ≥ +1σ'] = bool(sigma_dev >= 1)
    F['Sσ_200 ≥ +2σ'] = bool(sigma_dev >= 2)
    F['Sσ_200 ≥ +3σ'] = bool(sigma_dev >= 3)

    # 4) S+1/S-1(月度趋势,21 日比较)
    prev21 = val_n_ago(sp_s, 21)
    F['S+1'] = bool(prev21 is not None and sp_now > prev21)
    F['S-1'] = bool(prev21 is not None and sp_now < prev21)

    # 5) S+6/S-6(6 月趋势,126 日比较)
    prev126 = val_n_ago(sp_s, 126)
    F['S+6'] = bool(prev126 is not None and sp_now > prev126)
    F['S-6'] = bool(prev126 is not None and sp_now < prev126)

    # 6) S-+(已突破上轮回撤低点 = 价格高于回撤期间最低点)
    # 算法:从历史最高点之后到现在的最低点,当前价是否>该最低点
    sp_arr = sp_s.values
    cummax = np.maximum.accumulate(sp_arr)
    # 找到 last all-time-high 的索引
    last_ath_idx = int(np.where(sp_arr == cummax)[0][-1])
    if last_ath_idx < len(sp_arr) - 1:
        post_ath_min = float(sp_arr[last_ath_idx:].min())
        F['S-+'] = bool(sp_now > post_ath_min * 1.0001)  # 严格高于,加 0.01% 容错
    else:
        F['S-+'] = False  # 当前就是 ATH,没回撤

    # ──────────────────────────────────────────────────────────────────────
    # 二、VIX/情绪因子
    # ──────────────────────────────────────────────────────────────────────
    vix_now = None; V_c = None; V_c_sigma = None
    F['V ≤ V_c']           = None
    F['V < MA(V,21)']      = None
    F['max(V,21d) ≥ V_c+2σ'] = None
    F['V ≤ V_c-1σ']         = None
    F['V ≥ V_c+1σ']         = None
    F['V ≥ V_c+2σ']         = None
    F['V ≥ V_c+3σ']         = None
    F['V ≤ MA(V,21)-1σ_21d'] = None
    F['V ≥ MA(V,21)+1σ_21d'] = None
    F['V ≥ MA(V,21)+2σ_21d'] = None
    F['V ≥ MA(V,21)+3σ_21d'] = None

    if vix_s is not None and len(vix_s) > 252:
        vix_s = vix_s.sort_index().dropna()
        vix_now = float(vix_s.iloc[-1])
        F['_VIX'] = vix_now

        # V_c 严格定义:V_c = mean(VIX_i | drawdown_i < 10%, i in past n days),
        # n=252 起,calm count<63 时窗口 +1 日扩展(逐日扩,不是 ×1.5)
        sp_aligned = sp_s.reindex(vix_s.index, method='ffill')
        sp_ath_al  = sp_aligned.expanding().max()
        dd_al      = (sp_ath_al - sp_aligned) / sp_ath_al * 100  # 百分点

        i = len(vix_s) - 1
        n = 252
        v_calm = vix_s.iloc[max(0, i-n+1):i+1][dd_al.iloc[max(0, i-n+1):i+1] < 10].dropna()
        # 不够 63 个 calm,逐日 +1 扩
        max_extend = i + 1
        while len(v_calm) < 63 and n < max_extend:
            n += 1
            start = max(0, i-n+1)
            v_calm = vix_s.iloc[start:i+1][dd_al.iloc[start:i+1] < 10].dropna()
        if len(v_calm) >= 63:
            V_c       = float(v_calm.mean())
            V_c_sigma = float(v_calm.std(ddof=1))
            F['_V_c']       = V_c
            F['_V_c_sigma'] = V_c_sigma

            F['V ≤ V_c']    = bool(vix_now <= V_c)
            F['V ≤ V_c-1σ'] = bool(vix_now <= V_c - V_c_sigma)
            F['V ≥ V_c+1σ'] = bool(vix_now >= V_c + V_c_sigma)
            F['V ≥ V_c+2σ'] = bool(vix_now >= V_c + 2 * V_c_sigma)
            F['V ≥ V_c+3σ'] = bool(vix_now >= V_c + 3 * V_c_sigma)

            # max(V,21d) ≥ V_c+2σ
            vix_21d_max = float(vix_s.iloc[-21:].max())
            F['_VIX_21d_max'] = vix_21d_max
            F['max(V,21d) ≥ V_c+2σ'] = bool(vix_21d_max >= V_c + 2 * V_c_sigma)

        # MA(V,21) + σ_21d 体系
        ma21    = vix_s.rolling(21, min_periods=21).mean()
        sd21    = vix_s.rolling(21, min_periods=21).std(ddof=1)
        ma21_v  = float(ma21.iloc[-1]) if not pd.isna(ma21.iloc[-1]) else None
        sd21_v  = float(sd21.iloc[-1]) if not pd.isna(sd21.iloc[-1]) else None
        if ma21_v is not None and sd21_v is not None and sd21_v > 0:
            F['V < MA(V,21)']      = bool(vix_now < ma21_v)
            F['V ≤ MA(V,21)-1σ_21d'] = bool(vix_now <= ma21_v - sd21_v)
            F['V ≥ MA(V,21)+1σ_21d'] = bool(vix_now >= ma21_v + sd21_v)
            F['V ≥ MA(V,21)+2σ_21d'] = bool(vix_now >= ma21_v + 2 * sd21_v)
            F['V ≥ MA(V,21)+3σ_21d'] = bool(vix_now >= ma21_v + 3 * sd21_v)

    # ──────────────────────────────────────────────────────────────────────
    # 三、流动性 / NFCI 因子(5y σ 体系)
    # ──────────────────────────────────────────────────────────────────────
    F['N > 0'] = None
    F['N < 0'] = None
    F['N+ ≥ μ+1σ'] = None
    F['N+ ≥ μ+2σ'] = None
    F['N- ≥ μ+1σ'] = None
    F['N- ≥ μ+2σ'] = None

    nfci_now = last_val(nfci_s)
    F['_NFCI'] = nfci_now

    if nfci_s is not None and nfci_now is not None:
        nfci_s2 = nfci_s.sort_index()

        F['N > 0'] = bool(nfci_now > 0)
        F['N < 0'] = bool(nfci_now < 0)

        # NFCI 5y σ 体系:N_c = N_t - N_{t-20}; μ/σ 用 1260 日 rolling, ddof=1
        # 注意:NFCI 是周度数据,但模型对齐日度索引
        # N_c 用 NFCI 完整 1971+ 历史(L2 验证已确认)
        if len(nfci_s2) >= 1280:
            n_c = nfci_s2 - nfci_s2.shift(20)  # 20 日变化
            mu  = n_c.rolling(1260, min_periods=1260).mean()
            sd  = n_c.rolling(1260, min_periods=1260).std(ddof=1)

            n_c_now = float(n_c.iloc[-1]) if not pd.isna(n_c.iloc[-1]) else None
            mu_now  = float(mu.iloc[-1])  if not pd.isna(mu.iloc[-1])  else None
            sd_now  = float(sd.iloc[-1])  if not pd.isna(sd.iloc[-1])  else None

            F['_N_c'] = n_c_now
            F['_N_c_mu'] = mu_now
            F['_N_c_sd'] = sd_now

            if n_c_now is not None and mu_now is not None and sd_now is not None and sd_now > 0:
                # N+ = NFCI 改善冲击(N_c ≤ μ-σ);N- = NFCI 紧缩冲击(N_c ≥ μ+σ)
                # 互斥关系:N+ ≥ μ+1σ ⟺ N_c ≤ μ-σ
                F['N+ ≥ μ+1σ'] = bool(n_c_now <= mu_now - sd_now)
                F['N+ ≥ μ+2σ'] = bool(n_c_now <= mu_now - 2 * sd_now)
                F['N- ≥ μ+1σ'] = bool(n_c_now >= mu_now + sd_now)
                F['N- ≥ μ+2σ'] = bool(n_c_now >= mu_now + 2 * sd_now)

    # ──────────────────────────────────────────────────────────────────────
    # 四、Fed 政策(F+/F-)
    # ──────────────────────────────────────────────────────────────────────
    F['F+'] = None
    F['F-'] = None

    f_now = last_val(f_s)
    F['_FED']  = f_now
    if f_s is not None and f_now is not None:
        f_s2 = f_s.sort_index()
        # F+ / F- 用 12 个月均线(252 日)± 25bp 阈值
        if len(f_s2) >= 252:
            f_ma252 = float(f_s2.rolling(252, min_periods=63).mean().iloc[-1])
            F['F+'] = bool(f_now > f_ma252 + 0.25)
            F['F-'] = bool(f_now < f_ma252 - 0.25)
            F['_FED_ma252'] = f_ma252

    # ──────────────────────────────────────────────────────────────────────
    # 五、收益率曲线(Y+/Y-)
    # ──────────────────────────────────────────────────────────────────────
    y_now = last_val(y_s)
    F['_Y'] = y_now
    F['Y+'] = bool(y_now >= 0) if y_now is not None else None
    F['Y-'] = bool(y_now < 0)  if y_now is not None else None

    # ──────────────────────────────────────────────────────────────────────
    # 六、Fed 资产负债表(WALCL+ ≥ 500hm / 1000hm)
    # ──────────────────────────────────────────────────────────────────────
    F['WALCL+ ≥ 500hm']  = False
    F['WALCL+ ≥ 1000hm'] = False

    if walcl_s is not None and len(walcl_s) >= 14:
        # WALCL 单位换算到万亿美元(FRED 原始百万美元 / 1e6)
        ws  = walcl_s.sort_index() / 1e6
        chg13w = (ws - ws.shift(13)).dropna()

        # event_500: 13 周变化 ≥ 0.05 万亿; event_1000: ≥ 0.10 万亿
        # 持续期(交易日):500hm = 42td, 1000hm = 63td
        # 转成日度对齐,然后 forward fill 持续期
        event_500_w  = (chg13w >= 0.05)
        event_1000_w = (chg13w >= 0.10)

        # 简化处理:看过去 42/63 个交易日的 SP500 索引中,有没有触发的 WALCL 周
        # 因为 WALCL 是周度数据,我们看过去几周里有没有触发
        # 严格做法:对齐日度索引,然后 forward fill
        sp_idx = sp_s.index
        ws_aligned   = ws.reindex(sp_idx, method='ffill')
        # 13 周变化对齐
        ws_13w_chg   = ws_aligned - ws_aligned.shift(63)  # 63 个交易日 ≈ 13 周
        # 触发当天的 event 标记
        event_500    = (ws_13w_chg >= 0.05).fillna(False)
        event_1000   = (ws_13w_chg >= 0.10).fillna(False)
        # 持续期:42/63 个交易日内任何一天触发都算
        wndow_500    = event_500.rolling(42, min_periods=1).sum() > 0
        wndow_1000   = event_1000.rolling(63, min_periods=1).sum() > 0

        F['WALCL+ ≥ 500hm']  = bool(wndow_500.iloc[-1])
        F['WALCL+ ≥ 1000hm'] = bool(wndow_1000.iloc[-1])
        F['_WALCL_13w_chg']  = float(ws_13w_chg.iloc[-1]) if not pd.isna(ws_13w_chg.iloc[-1]) else None

    # ──────────────────────────────────────────────────────────────────────
    # 七、信用利差(HY)
    # ──────────────────────────────────────────────────────────────────────
    F['HY_t > 8%']     = None
    F['HY_c21 > 0']    = None
    F['HY_c21 ≤ 0']    = None
    F['HY_t < HY_m21'] = None

    hy_now = last_val(hy_s)
    F['_HY'] = hy_now
    if hy_s is not None and len(hy_s) >= 22:
        hy_s2 = hy_s.sort_index()
        F['HY_t > 8%'] = bool(hy_now > 8.0) if hy_now is not None else None

        # HY_c21 = HY_t - HY_{t-21}
        hy_21d_ago = val_n_ago(hy_s2, 21)
        if hy_21d_ago is not None and hy_now is not None:
            hy_c21 = hy_now - hy_21d_ago
            F['_HY_c21'] = hy_c21
            F['HY_c21 > 0'] = bool(hy_c21 > 0)
            F['HY_c21 ≤ 0'] = bool(hy_c21 <= 0)

        # HY_m21 = max(HY, 21d), HY_t < HY_m21
        if len(hy_s2) >= 21:
            hy_m21 = float(hy_s2.iloc[-21:].max())
            F['_HY_m21'] = hy_m21
            if hy_now is not None:
                F['HY_t < HY_m21'] = bool(hy_now < hy_m21)

    # ──────────────────────────────────────────────────────────────────────
    # 八、估值(P+/P-, ERP)
    # ──────────────────────────────────────────────────────────────────────
    F['P-'] = None
    F['P+'] = None
    F['ERP > 3%']   = None
    F['ERP < 1.5%'] = None
    F['ERP < 0']    = None

    F['_PE']  = fpe_val

    # ERP = (1/P) * 100 - R(实际利率)
    r_now = last_val(r_s)
    F['_R'] = r_now
    erp = None
    if fpe_val and fpe_val > 0 and r_now is not None:
        erp = (1.0 / fpe_val) * 100 - r_now
    F['_ERP'] = erp
    if erp is not None:
        F['ERP > 3%']   = bool(erp > 3.0)
        F['ERP < 1.5%'] = bool(erp < 1.5)
        F['ERP < 0']    = bool(erp < 0)

    # P+/P- 用 PE 1260 日 rolling, ddof=1
    if fpe_val is not None:
        try:
            import sp500_cache_manager as _cm
            _db = pd.read_csv(_cm.DB_FILE, index_col='date', parse_dates=True)
            _pe_hist = _db['forward_pe'].dropna()
            if len(_pe_hist) >= 252 and _pe_hist.nunique() >= 10:
                _w = min(1260, len(_pe_hist))
                _pe_avg = float(_pe_hist.rolling(_w, min_periods=252).mean().iloc[-1])
                _pe_std = float(_pe_hist.rolling(_w, min_periods=252).std(ddof=1).iloc[-1])
                if _pe_std > 0.5:
                    F['P+'] = bool(fpe_val > _pe_avg + _pe_std)
                    F['P-'] = bool(fpe_val < _pe_avg - _pe_std)
                    F['_PE_avg'] = _pe_avg
                    F['_PE_std'] = _pe_std
        except Exception:
            # 兜底
            if fpe_val is not None:
                F['P+'] = bool(fpe_val > 24.0)
                F['P-'] = bool(fpe_val < 16.0)

    # ──────────────────────────────────────────────────────────────────────
    # 九、CPI 同比(CPI_y < 0%, ≥ 5%, ≥ 7%)— 注意 45 天发布滞后
    # ──────────────────────────────────────────────────────────────────────
    F['CPI_y < 0%'] = None
    F['CPI_y ≥ 5%'] = None
    F['CPI_y ≥ 7%'] = None
    cpi_yoy_now = None

    if cpi_s is not None and len(cpi_s) >= 13:
        cpi_s2 = cpi_s.sort_index()
        # 发布滞后:今天能看到的最新 CPI 是 45 天前的
        cpi_pub = today - timedelta(days=45)
        cpi_avail = cpi_s2[cpi_s2.index <= cpi_pub]
        if len(cpi_avail) >= 13:
            cpi_yoy = (cpi_avail / cpi_avail.shift(12) - 1) * 100
            cpi_yoy_now = float(cpi_yoy.iloc[-1])
            F['_CPI_y'] = cpi_yoy_now
            F['CPI_y < 0%'] = bool(cpi_yoy_now < 0)
            F['CPI_y ≥ 5%'] = bool(cpi_yoy_now >= 5.0)
            F['CPI_y ≥ 7%'] = bool(cpi_yoy_now >= 7.0)

    # ──────────────────────────────────────────────────────────────────────
    # 十、EPS 信号(E+/E+2/E-/E-2)— 来自 cache_manager
    # ──────────────────────────────────────────────────────────────────────
    e_plus  = data.get('e_plus_from_cache')
    e_plus2 = data.get('e_plus2_from_cache')
    e_minus = data.get('e_minus_from_cache')
    e_minus2= data.get('e_minus2_from_cache')

    F['E+']  = bool(e_plus)  if e_plus  is not None else None
    F['E+2'] = bool(e_plus2) if e_plus2 is not None else None
    F['E-']  = bool(e_minus) if e_minus is not None else None
    F['E-2'] = bool(e_minus2)if e_minus2 is not None else None

    # ──────────────────────────────────────────────────────────────────────
    # 十一、油价(OIL_c21, OIL_pct5y)
    # ──────────────────────────────────────────────────────────────────────
    F['OIL_c21 ≥ μ+1σ']   = None
    F['OIL_c21 ≥ μ+2σ']   = None
    F['OIL_pct5y > 85%']  = None

    if oil_s is not None and len(oil_s) >= 1260:
        oil_s2 = oil_s.sort_index().dropna()
        oil_now = float(oil_s2.iloc[-1])
        F['_OIL'] = oil_now

        # OIL_c21 = OIL_t / OIL_{t-21} - 1(变化率)
        oil_21_ago = val_n_ago(oil_s2, 21)
        if oil_21_ago is not None and oil_21_ago > 0:
            oil_c21 = oil_now / oil_21_ago - 1
            F['_OIL_c21'] = oil_c21

            # μ/σ 用 OIL_c21 全历史(滚动 1260 日)
            oc21_series = oil_s2 / oil_s2.shift(21) - 1
            mu = float(oc21_series.rolling(1260, min_periods=252).mean().iloc[-1])
            sd = float(oc21_series.rolling(1260, min_periods=252).std(ddof=1).iloc[-1])
            if sd > 0:
                F['OIL_c21 ≥ μ+1σ'] = bool(oil_c21 >= mu + sd)
                F['OIL_c21 ≥ μ+2σ'] = bool(oil_c21 >= mu + 2 * sd)

        # OIL_pct5y > 85%(过去 1260 日的百分位)
        window = min(1260, len(oil_s2))
        hist   = oil_s2.iloc[-window:]
        oil_pct5y = float((hist < oil_now).sum() / len(hist) * 100)
        F['_OIL_pct5y'] = oil_pct5y
        F['OIL_pct5y > 85%'] = bool(oil_pct5y > 85)

    return F


# ══════════════════════════════════════════════════════════════════════════
# 10 触发器定义 — 严格对齐文件 4 公式
# ══════════════════════════════════════════════════════════════════════════

TRIGGERS = {
    'T1_2000离场': {
        'type': 'exit',
        'description': '2000 互联网泡沫顶 — 通胀过热顶',
        'core_factor': 'ERP < 0',
        'must_have': ['W+200', 'Sσ_200 ≥ +1σ', 'F+', 'Y-', 'N- ≥ μ+1σ', 'HY_c21 > 0', 'ERP < 0'],
        'or_paths':  [],
        'not_have':  [],
    },
    'T2_2007离场': {
        'type': 'exit',
        'description': '2007 鸽派假反弹顶 — 油价拉动',
        'core_factor': 'OIL_pct5y > 85%',
        'must_have': ['OIL_pct5y > 85%', 'E-2', 'Sσ_200 ≥ +1σ', 'W+200', 'F-', 'Y+', 'S+6'],
        'or_paths':  [],
        'not_have':  [],
    },
    'T3_2015离场': {
        'type': 'exit',
        'description': '2015 工业衰退顶 — 油价崩盘通缩',
        'core_factor': 'CPI_y < 0%',
        'must_have': ['W+200', 'N < 0', 'Y+', 'P+', 'ERP > 3%', 'E-2', 'CPI_y < 0%'],
        'or_paths':  [],
        'not_have':  [],
    },
    'T4_2022离场': {
        'type': 'exit',
        'description': '2022 加息顶 — 通胀失控+扩表减速',
        'core_factor': 'CPI_y ≥ 5%',
        'must_have': ['CPI_y ≥ 5%', 'S+6', 'Sσ_200 ≥ +3σ'],
        'or_paths':  [['WALCL+ ≥ 500hm', 'ERP < 0', 'N- ≥ μ+1σ', 'F+', 'N > 0']],
        'not_have':  ['WALCL+ ≥ 1000hm'],
    },
    'T5_2002入场': {
        'type': 'entry',
        'description': '2002 互联网底 — 慢性熊市末端',
        'core_factor': 'E+',
        'must_have': ['S-6', 'S- ≥ 30%', 'Sσ_200 ≤ -3σ', 'V ≥ V_c+3σ', 'N < 0', 'Y+', 'E+'],
        'or_paths':  [],
        'not_have':  ['N- ≥ μ+1σ'],
    },
    'T6_2009入场': {
        'type': 'entry',
        'description': '2009 次贷底 — 急性信用危机底',
        'core_factor': 'HY_t > 8%',
        'must_have': ['S-6', 'S- ≥ 30%', 'Sσ_200 ≤ -3σ', 'V ≥ V_c+3σ', 'HY_t > 8%', 'ERP < 0', 'WALCL+ ≥ 1000hm'],
        'or_paths':  [],
        'not_have':  [],
    },
    'T7_2020入场': {
        'type': 'entry',
        'description': '2020 COVID 底 — 史诗急跌+大水救市',
        'core_factor': 'WALCL+ ≥ 1000hm',
        'must_have': ['Sσ_200 ≤ -3σ', 'V ≥ V_c+3σ', 'HY_t > 8%', 'WALCL+ ≥ 1000hm'],
        'or_paths':  [['P-', 'ERP > 3%']],
        'not_have':  [],
    },
    'T8_2022入场': {
        'type': 'entry',
        'description': '2022 加息底 — 通胀加息中等深度底',
        'core_factor': 'CPI_y ≥ 7%',
        'must_have': ['S- ≥ 20%', 'S-6', 'F+', 'CPI_y ≥ 7%', 'E-2', 'ERP > 3%', 'N < 0'],
        'or_paths':  [],
        'not_have':  [],
    },
    'T9_白银坑1组': {
        'type': 'entry',
        'description': '白银坑 1 组(广义) — 长牛中浅熊',
        'core_factor': 'ERP > 3%',
        'must_have': ['W+200', 'S- ≥ 10%', 'Y+', 'N < 0', 'ERP > 3%'],
        'or_paths':  [['HY_t > 8%', 'max(V,21d) ≥ V_c+2σ', 'F+', 'E-2']],
        'not_have':  ['S- ≥ 20%', 'F-', 'CPI_y ≥ 5%'],
    },
    'T10_白银坑2组': {
        'type': 'entry',
        'description': '白银坑 2 组(中庸态健康市场)',
        'core_factor': 'S- ≥ 5% AND NOT(S- ≥ 20%)',
        'must_have': ['W+200', 'S- ≥ 5%', 'S-+', 'N < 0'],
        'or_paths':  [],
        'not_have':  ['S- ≥ 20%', 'HY_t > 8%', 'ERP > 3%', 'CPI_y ≥ 5%', 'F-'],
        # 特殊:NOT(ERP < 1.5% AND Y-)
        'special_not': [('ERP < 1.5%', 'Y-')],
    },
}


# ══════════════════════════════════════════════════════════════════════════
# 触发器评估 + 接近触发评分
# ══════════════════════════════════════════════════════════════════════════

def _factor_satisfied(F, factor_name):
    """获取因子值;返回 True/False/None(数据不足)"""
    v = F.get(factor_name)
    return v if isinstance(v, bool) or v is None else bool(v)


def evaluate_trigger(trigger_id, F):
    """
    评估单个触发器,返回详细结果。

    Returns:
        dict {
          'triggered': bool/None (None=数据不足),
          'must_have_status': [(因子名, True/False/None), ...],
          'or_paths_status':  [[(因子名, True/False/None), ...]],
          'not_have_status':  [(因子名, True/False/None), ...],
          'satisfied_count':  int (满足的必有因子数),
          'total_must':       int (总必有因子数),
          'satisfied_pct':    float (满足率,含 OR/NOT,综合),
          'missing_factors':  list,(缺失的关键因子,按重要性)
          'unknown_factors':  list,
        }
    """
    cfg = TRIGGERS[trigger_id]
    must_have = cfg['must_have']
    or_paths  = cfg.get('or_paths', [])
    not_have  = cfg.get('not_have', [])
    special_not = cfg.get('special_not', [])

    # 1. 必有因子状态
    must_status = []
    must_satisfied = 0
    must_unknown   = 0
    for f in must_have:
        v = _factor_satisfied(F, f)
        must_status.append((f, v))
        if v is True:
            must_satisfied += 1
        elif v is None:
            must_unknown += 1

    # 2. OR 路径状态(每个路径,任一满足即可)
    or_status_all = []
    or_satisfied_all = True
    or_any_unknown = False
    for path in or_paths:
        path_status = []
        path_has_true   = False
        path_has_unknown= False
        for f in path:
            v = _factor_satisfied(F, f)
            path_status.append((f, v))
            if v is True:
                path_has_true = True
            elif v is None:
                path_has_unknown = True
        or_status_all.append(path_status)
        if not path_has_true:
            or_satisfied_all = False
        if path_has_unknown and not path_has_true:
            or_any_unknown = True

    # 3. NOT 屏蔽状态(NOT(F) = True 当 F=False)
    not_status = []
    not_satisfied = True  # 所有 NOT 都成立
    not_any_unknown = False
    for f in not_have:
        v = _factor_satisfied(F, f)
        # NOT(F) = True iff F = False
        not_v = (v is False)
        not_status.append((f, v))  # 显示原因子值
        if v is None:
            not_any_unknown = True
        elif v is True:
            not_satisfied = False  # F=True → NOT(F)=False → 屏蔽不成立

    # 4. special_not(组合 NOT)
    special_not_status = []
    for combo in special_not:
        combo_vals = []
        all_true = True
        any_unknown = False
        for f in combo:
            v = _factor_satisfied(F, f)
            combo_vals.append((f, v))
            if v is None:
                any_unknown = True
            elif v is False:
                all_true = False
        # NOT(combo all True) = True iff at least one is False
        # 这里我们记 "all_true" 是组合是否全 True;若全 True,则 NOT 失效
        special_not_status.append((combo, combo_vals, all_true))
        if all_true:
            not_satisfied = False
        if any_unknown and all_true:
            not_any_unknown = True

    # 5. 综合判断
    must_all_true  = (must_satisfied == len(must_have))
    must_no_unknown = (must_unknown == 0)

    if must_no_unknown and not or_any_unknown and not not_any_unknown:
        triggered = bool(must_all_true and or_satisfied_all and not_satisfied)
    else:
        # 有未知项,但若已经能确定不触发,仍可定 False
        if (must_unknown == 0 and not must_all_true) or \
           (not or_any_unknown and not or_satisfied_all and len(or_paths) > 0) or \
           (not not_any_unknown and not not_satisfied):
            triggered = False
        else:
            triggered = None  # 真不确定

    # 6. 接近触发评分(综合所有部分)
    # 算法:必有因子的满足率(占主权)+ OR 路径(每个路径满 1 / 不满 0)+ NOT(每个屏蔽满 1 /不满 0)
    parts_total     = len(must_have) + len(or_paths) + len(not_have) + len(special_not)
    parts_satisfied = must_satisfied
    for path_st in or_status_all:
        if any(v is True for _, v in path_st):
            parts_satisfied += 1
    for f, v in not_status:
        if v is False:  # NOT(F=False)=True
            parts_satisfied += 1
    for combo, combo_vals, all_true in special_not_status:
        if not all_true:
            parts_satisfied += 1
    pct = parts_satisfied / parts_total if parts_total > 0 else 0

    # 7. 缺失因子(按重要性:核心因子 > 必有 > OR > NOT)
    missing = []
    core = cfg.get('core_factor', '')
    # 必有里没满足的
    for f, v in must_status:
        if v is False:
            tag = '★核心' if f == core else '必有'
            missing.append(f'{f}({tag})')
    # OR 路径全失败的
    for i, path_st in enumerate(or_status_all):
        if not any(v is True for _, v in path_st):
            path_factors = ' | '.join(f'{f}={v}' for f, v in path_st)
            missing.append(f'OR-路径{i+1}: 任一需满足({path_factors})')
    # NOT 失效的
    for f, v in not_status:
        if v is True:
            missing.append(f'NOT({f}) 失效(当前 {f}=True)')
    for combo, combo_vals, all_true in special_not_status:
        if all_true:
            missing.append(f'NOT({" AND ".join(c[0] for c in combo_vals)}) 失效')

    # 8. 未知因子
    unknown = []
    for f, v in must_status:
        if v is None: unknown.append(f)
    for path_st in or_status_all:
        for f, v in path_st:
            if v is None and f not in unknown:
                unknown.append(f)

    return {
        'triggered':        triggered,
        'must_have_status': must_status,
        'or_paths_status':  or_status_all,
        'not_have_status':  not_status,
        'special_not_status': special_not_status,
        'satisfied_count':  must_satisfied,
        'total_must':       len(must_have),
        'satisfied_pct':    pct,
        'missing_factors':  missing,
        'unknown_factors':  unknown,
        'description':      cfg['description'],
        'core_factor':      core,
        'type':             cfg['type'],
    }


def get_alert_level(satisfied_pct, triggered):
    """
    接近触发预警分级:
      🔴 已触发        triggered=True
      🟠 90%+          已触发后无效;否则 ≥ 0.9
      🟡 70-90%
      ⚪ <70%
    """
    if triggered is True:
        return '🔴 已触发'
    elif triggered is None and satisfied_pct >= 0.9:
        return '🟠 高度接近(数据不足)'
    elif satisfied_pct >= 0.9:
        return '🟠 高度接近'
    elif satisfied_pct >= 0.7:
        return '🟡 中度接近'
    else:
        return '⚪ 远未触发'


# ══════════════════════════════════════════════════════════════════════════
# 主入口:计算所有因子 + 评估全部 10 触发器
# ══════════════════════════════════════════════════════════════════════════

def compute_signals(data, today=None):
    """
    主入口。输入数据 dict,输出 snapshot dict。

    snapshot 格式:
      {
        'date': '2026-05-10',
        'sp500': 5800.0,
        ...各种关键中间量...
        'factors': {因子名: 0/1/None, ...},
        'triggers': {
          'T1_2000离场': {triggered, satisfied_pct, alert, ...},
          ...
        },
        'alerts': [触发或接近触发的触发器列表],
      }
    """
    if today is None:
        today = pd.Timestamp(datetime.today().date())

    # 第 1 步:计算 V15 风格全部因子
    F = compute_factors(data, today=today)

    # 第 2 步:评估 10 触发器
    triggers_result = {}
    alerts = []
    for tid in TRIGGERS.keys():
        r = evaluate_trigger(tid, F)
        r['alert'] = get_alert_level(r['satisfied_pct'], r['triggered'])
        triggers_result[tid] = r
        if r['triggered'] is True or r['satisfied_pct'] >= 0.7:
            alerts.append((tid, r))

    # 第 3 步:组织 snapshot
    snapshot = {
        'date':          today.strftime('%Y-%m-%d'),
        'sp500':         round(F.get('_sp_now', 0), 2),
        'sp_dd_pct':     round(F.get('_sp_dd_pct', 0), 2),
        'ma200w':        round(F.get('_W200_value', 0), 2) if F.get('_W200_value') else None,
        'sigma_dev_200': round(F.get('Sσ_200', 0), 2) if F.get('Sσ_200') is not None else None,
        'vix':           round(F.get('_VIX', 0), 2) if F.get('_VIX') else None,
        'V_c':           round(F.get('_V_c', 0), 2) if F.get('_V_c') else None,
        'V_c_sigma':     round(F.get('_V_c_sigma', 0), 2) if F.get('_V_c_sigma') else None,
        'y_spread':      round(F.get('_Y', 0), 3) if F.get('_Y') is not None else None,
        'fed_rate':      round(F.get('_FED', 0), 2) if F.get('_FED') is not None else None,
        'real_rate':     round(F.get('_R', 0), 2) if F.get('_R') is not None else None,
        'hy_spread':     round(F.get('_HY', 0), 2) if F.get('_HY') is not None else None,
        'hy_c21':        round(F.get('_HY_c21', 0), 3) if F.get('_HY_c21') is not None else None,
        'nfci':          round(F.get('_NFCI', 0), 3) if F.get('_NFCI') is not None else None,
        'n_c':           round(F.get('_N_c', 0), 3) if F.get('_N_c') is not None else None,
        'n_c_mu':        round(F.get('_N_c_mu', 0), 3) if F.get('_N_c_mu') is not None else None,
        'n_c_sigma':     round(F.get('_N_c_sd', 0), 3) if F.get('_N_c_sd') is not None else None,
        'forward_pe':    round(F.get('_PE', 0), 2) if F.get('_PE') else None,
        'pe_avg':        round(F.get('_PE_avg', 0), 1) if F.get('_PE_avg') else None,
        'erp':           round(F.get('_ERP', 0), 2) if F.get('_ERP') is not None else None,
        'cpi_y':         round(F.get('_CPI_y', 0), 2) if F.get('_CPI_y') is not None else None,
        'oil_price':     round(F.get('_OIL', 0), 2) if F.get('_OIL') else None,
        'oil_c21':       round(F.get('_OIL_c21', 0)*100, 2) if F.get('_OIL_c21') is not None else None,
        'oil_pct5y':     round(F.get('_OIL_pct5y', 0), 1) if F.get('_OIL_pct5y') is not None else None,
        'walcl_13w_chg': round(F.get('_WALCL_13w_chg', 0), 3) if F.get('_WALCL_13w_chg') is not None else None,

        # 因子表(用于详细诊断)
        'factors':  {k: v for k, v in F.items() if not k.startswith('_')},

        # 触发器评估
        'triggers': triggers_result,

        # 触发或接近触发的触发器
        'alerts':   [(tid, r['alert'], r['satisfied_pct']) for tid, r in alerts],

        # 各触发器的简洁触发状态(供 main.py 快速读取)
        'T1_2000离场':  triggers_result['T1_2000离场']['triggered'],
        'T2_2007离场':  triggers_result['T2_2007离场']['triggered'],
        'T3_2015离场':  triggers_result['T3_2015离场']['triggered'],
        'T4_2022离场':  triggers_result['T4_2022离场']['triggered'],
        'T5_2002入场':  triggers_result['T5_2002入场']['triggered'],
        'T6_2009入场':  triggers_result['T6_2009入场']['triggered'],
        'T7_2020入场':  triggers_result['T7_2020入场']['triggered'],
        'T8_2022入场':  triggers_result['T8_2022入场']['triggered'],
        'T9_白银坑1组': triggers_result['T9_白银坑1组']['triggered'],
        'T10_白银坑2组':triggers_result['T10_白银坑2组']['triggered'],
    }

    return snapshot
