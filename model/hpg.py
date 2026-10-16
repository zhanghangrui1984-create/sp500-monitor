"""HPG: housing-permit growth, a monthly three-month-average YoY rate.

HPG > 0 is the requested condition. It has no additional growth threshold,
temporary-employment test, or trading rule. Missing inputs stay unknown.
"""
import pandas as pd
from datetime import datetime


def calculate_hpg(permits, release_dates=None):
    """Use a complete month-start index; preserve missing calendar months.

If release_dates are absent, preserve the research convention: next month day
20 is a simulated release date. It is not the actual historical announcement.
For live operation, callers can provide observed publication dates explicitly.
"""
    if not isinstance(permits.index,pd.DatetimeIndex):
        raise ValueError('monthly inputs require a DatetimeIndex')
    if permits.empty or not permits.index.is_unique or not permits.index.is_monotonic_increasing:
        raise ValueError('monthly dates must be nonempty, unique and increasing')
    if not permits.index.is_month_start.all():
        raise ValueError('use observation-month start dates')
    if (permits.dropna()<=0).any():
        raise ValueError('permit quantities must be positive or missing')
    s=permits.reindex(pd.date_range(permits.index[0],permits.index[-1],freq='MS')).astype(float)
    s.index=s.index.as_unit('ns');s.index.name='observation_month'
    m=pd.DataFrame({'PERMIT':s,'mean3':s.rolling(3,min_periods=3).mean()})
    m['same_period_last_year_mean3']=m.mean3.shift(12)
    m['HPG']=m.mean3/m.same_period_last_year_mean3-1
    m['HPG_positive']=m.HPG.gt(0).astype('boolean').mask(m.HPG.isna())
    if release_dates is None:
        m['release_date']=[d+pd.DateOffset(months=1,day=20) for d in m.index]
        m['release_basis']='next_month_day20_proxy'
    else:
        if not release_dates.index.is_unique:raise ValueError('duplicate publication date keys')
        m['release_date']=pd.to_datetime(release_dates.reindex(m.index))
        if m.release_date.isna().any():raise ValueError('missing publication dates')
        m['release_basis']='provided_publication_dates'
    m['release_date']=pd.to_datetime(m.release_date).astype('datetime64[ns]')
    if not m.release_date.is_monotonic_increasing or not m.release_date.is_unique:
        raise ValueError('publication dates must be increasing and unique')
    if not (m.release_date>m.index+pd.offsets.MonthEnd(0)).all():
        raise ValueError('cannot publish a completed month before month end')
    return m


def align_hpg(monthly,sessions):
    """Take the latest publication strictly before the session date.

Monthly values remain in force until another publication; do not backfill
before a release or skip a missing release value in favor of an older value.
"""
    if not sessions.is_unique or not sessions.is_monotonic_increasing:
        raise ValueError('sessions must be unique and increasing')
    q=monthly.copy();q['observation_month']=q.index
    out=pd.merge_asof(pd.DataFrame({'date':sessions.as_unit('ns')}),
        q.sort_values('release_date'),left_on='date',right_on='release_date',
        direction='backward',allow_exact_matches=False).set_index('date')
    out['HPG_positive']=out.HPG.gt(0).astype('boolean').mask(out.HPG.isna())
    return out


def from_workbook(wb, sessions, settings):
    """Recompute from permit quantities and stored release dates, not caches."""
    rows=[r for r in wb[settings['source_sheet']].values
          if isinstance(r[0],datetime)]
    months=pd.DatetimeIndex([r[0] for r in rows]).as_unit('ns')
    permits=pd.Series([r[1] for r in rows],index=months,dtype=float)
    releases=pd.Series([r[6] for r in rows],index=months)
    monthly=calculate_hpg(permits,releases)
    monthly['release_basis']=settings['release_basis']
    sessions=sessions.as_unit('ns')
    positions=sessions.searchsorted(pd.DatetimeIndex(monthly.release_date),side='right')
    monthly['available_date']=[sessions[i] if i<len(sessions) else pd.NaT for i in positions]
    return monthly,align_hpg(monthly,sessions)
