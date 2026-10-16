"""FSX relative financial-sector return and FCS funding spread; no trigger rules."""
import pandas as pd


def calculate_fsx63(fsx,sessions,lookback=63):
    """Mean of exactly the prior 63 sessions, excluding today; missing stays unknown."""
    if not isinstance(lookback,int) or lookback<1:raise ValueError('invalid lookback')
    if not sessions.is_unique or not sessions.is_monotonic_increasing or not fsx.index.is_unique:raise ValueError('invalid dates')
    return fsx.reindex(sessions).shift(1).rolling(lookback,min_periods=lookback).mean().rename('FSX63')


def calculate_fsx(financial, benchmark, sessions, lookback=252):
    if not isinstance(lookback,int) or lookback<1:
        raise ValueError('lookback must be a positive integer')
    if not sessions.is_unique or not sessions.is_monotonic_increasing:
        raise ValueError('sessions must be unique and increasing')
    if not financial.index.is_unique or not benchmark.index.is_unique:
        raise ValueError('duplicate input dates')
    f=pd.DataFrame({'XLF_adj':financial.reindex(sessions),'SPY_adj':benchmark.reindex(sessions)})
    if ((f<=0)&f.notna()).any().any():
        raise ValueError('nonpositive adjusted price')
    f['ratio']=f.XLF_adj/f.SPY_adj
    f['FSX']=f.ratio/f.ratio.shift(lookback)-1
    f['lag_date']=pd.Series(sessions,index=sessions).shift(lookback)
    return f


def calculate_fcs(financial_rate, treasury_rate):
    if not financial_rate.index.is_unique or not treasury_rate.index.is_unique:
        raise ValueError('duplicate input dates')
    f=pd.concat([financial_rate.rename('CP_pct'),treasury_rate.rename('TB_pct')],axis=1,sort=True)
    f['FCS_pp']=f.CP_pct-f.TB_pct
    return f


def weekly_fcs(daily, min_count=3):
    if not isinstance(min_count,int) or min_count<1:
        raise ValueError('min_count must be positive')
    # Pair first, average second. Independent column means can be misleading.
    grouped=daily.FCS_pp.resample('W-FRI')
    f=pd.DataFrame({'mean_pp':grouped.mean(),'valid_days':grouped.count()})
    f['qualified_pp']=f.mean_pp.where(f.valid_days>=min_count)
    return f


def align_fcs(weekly,effective,sessions,max_age_days=14):
    """Align qualified weekly observations; stale data remain unknown, not zero."""
    if not isinstance(max_age_days,int) or max_age_days<0:raise ValueError('invalid maximum age')
    ev=pd.DataFrame({'value':weekly,'available_on':effective}).dropna()
    ev['observed_on']=ev.index
    if not (ev.available_on>ev.observed_on).all():raise ValueError('weekly quote available before completed week')
    if not ev.available_on.is_unique:raise ValueError('duplicate effective dates')
    ev=ev.set_index('available_on').sort_index();ev['available_on']=ev.index
    out=ev.reindex(ev.index.union(sessions)).sort_index().ffill().reindex(sessions)
    out['age_days']=(pd.Series(sessions,index=sessions)-out.observed_on).dt.days
    out['value']=out.value.where(out.age_days<=max_age_days)
    return out
