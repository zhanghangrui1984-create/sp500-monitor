"""Shared weekly NFCI direction and monthly inventory/sales direction.

Both exit branches consume the SAME values, lookbacks and availability rules.
Zero is a strict threshold; missing releases remain unknown rather than zero.
"""
from datetime import datetime
from decimal import Decimal
import numpy as np
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar

def _scaled(x,scale):
    if pd.isna(x):return np.nan
    d=Decimal(str(x))*scale
    if not d.is_finite() or abs(d-d.to_integral_value())>Decimal('0.00000001'):
        raise ValueError('Source value has unexpected published precision')
    return int(d.to_integral_value())

def _check_window(h):
    if isinstance(h,bool) or not isinstance(h,int) or h<1:raise ValueError('Positive integer lookback required')

def weekly(source,lookback=26):
    _check_window(lookback);s=source.copy().astype(float).sort_index()
    if s.empty or s.index.has_duplicates or not (s.index.weekday==4).all():raise ValueError('Unique Friday NFCI observations required')
    s=s.reindex(pd.date_range(s.index.min(),s.index.max(),freq='W-FRI'))
    units=s.map(lambda x:_scaled(x,1000))
    w=pd.DataFrame({'NFCI':s,'NFCI_lag':s.shift(lookback),'NFCI_change':(units-units.shift(lookback))/1000})
    w['NFCI_up']=w.NFCI_change.gt(0).astype('boolean').mask(w.NFCI_change.isna())
    holidays=set(USFederalHolidayCalendar().holidays(s.index.min(),s.index.max()+pd.Timedelta(days=14)))
    w['published']=[d+pd.Timedelta(days=5+int(any(d+pd.Timedelta(days=k) in holidays for k in [3,4,5]))) for d in s.index]
    w.index.name='observation_week';return w

def monthly(source,lookback=6,release_day=20,shift_days=0):
    _check_window(lookback)
    if isinstance(release_day,bool) or not isinstance(release_day,int) or not 1<=release_day<=28:raise ValueError('Release day must be1..28')
    if isinstance(shift_days,bool) or not isinstance(shift_days,int):raise ValueError('Integer publication sensitivity offset required')
    s=source.copy().astype(float).sort_index()
    if s.empty or s.index.has_duplicates or not (s.index.day==1).all():raise ValueError('Unique first-of-month observations required')
    s=s.reindex(pd.date_range(s.index.min(),s.index.max(),freq='MS'));units=s.map(lambda x:_scaled(x,100))
    m=pd.DataFrame({'ISRATIO':s,'ISRATIO_lag':s.shift(lookback),'ISRATIO_change':(units-units.shift(lookback))/100})
    m['ISRATIO_up']=m.ISRATIO_change.gt(0).astype('boolean').mask(m.ISRATIO_change.isna())
    m['published']=[d+pd.DateOffset(months=2,day=release_day)+pd.Timedelta(days=shift_days) for d in s.index]
    if any(m.published<=s.index.to_period('M').end_time.normalize()):raise ValueError('Publication cannot precede complete observation month')
    m.index.name='observation_month';return m

def align(frame,sessions,frequency,allow_same_day):
    # A missing expected next release invalidates the old observation from then on.
    z=frame.reset_index();last=z.iloc[-1];obscol=z.columns[0]
    nextobs=frame.index[-1]+(pd.Timedelta(days=7) if frequency=='weekly' else pd.DateOffset(months=1))
    if frequency=='weekly':
        holidays=set(USFederalHolidayCalendar().holidays(nextobs,nextobs+pd.Timedelta(days=8)))
        nextpub=nextobs+pd.Timedelta(days=5+int(any(nextobs+pd.Timedelta(days=k) in holidays for k in [3,4,5])))
    else:nextpub=last.published+pd.DateOffset(months=1)
    z=pd.concat([z,pd.DataFrame([{obscol:nextobs,'published':nextpub}])],ignore_index=True)
    z['published']=pd.to_datetime(z.published).astype('datetime64[ns]')
    left=pd.DataFrame({'date':pd.DatetimeIndex(sessions).astype('datetime64[ns]')})
    d=pd.merge_asof(left,z,left_on='date',right_on='published',allow_exact_matches=allow_same_day).set_index('date')
    flag='NFCI_up' if frequency=='weekly' else 'ISRATIO_up';d[flag]=d[flag].astype('boolean')
    return d

def from_workbook(wb,sessions,settings):
    def extract(name):
        pairs=[(pd.Timestamp(r[0]),r[1]) for r in wb[name].values if len(r)>1 and isinstance(r[0],datetime)]
        if len({d for d,_ in pairs})!=len(pairs):raise ValueError('Duplicate workbook observation')
        return pd.Series(dict(pairs),dtype=float).sort_index()
    w=weekly(extract(settings['nfci_sheet']),settings['nfci_weeks'])
    m=monthly(extract(settings['inventory_sheet']),settings['inventory_months'],settings['inventory_release_day'])
    wd=align(w,sessions,'weekly',True);md=align(m,sessions,'monthly',False)
    return w,m,wd,md
