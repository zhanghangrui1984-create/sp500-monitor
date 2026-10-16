"""Shared financial tightening and cyclical/defensive relative performance.

Raw prices remain session dated. Never fill missing ETF prices or shorten the
126-session window. Weekly NFCI keeps the existing simulated release schedule.
"""
from datetime import datetime
import numpy as np
import pandas as pd
from .inventory_exit import weekly, align

TICKERS=('XLY','XLI','XLB','XLP','XLV','XLU')

def sector_ratio(prices,sessions,lookback=126):
    if isinstance(lookback,bool) or not isinstance(lookback,int) or lookback<1:
        raise ValueError('Positive session lookback required')
    p=prices.loc[:,list(TICKERS)].copy().astype(float)
    if p.empty or p.index.has_duplicates or not p.index.is_monotonic_increasing:
        raise ValueError('Unique sorted daily prices required')
    if ((p<=0)&p.notna()).any().any():raise ValueError('Prices must be positive or explicitly missing')
    cal=pd.DatetimeIndex(sessions)
    if cal.has_duplicates or not cal.is_monotonic_increasing:raise ValueError('Unique sorted sessions required')
    # Ignore prices later than the requested calendar; require every retained quote to be a session.
    p=p.loc[(p.index>=cal.min())&(p.index<=cal.max())]
    if not p.index.isin(cal).all():raise ValueError('Non-session price observation')
    valid=p.dropna()
    if valid.empty:raise ValueError('No common six-ETF starting session')
    first=valid.index[0];norm=p.reindex(cal)/valid.iloc[0]
    norm.loc[cal<first]=np.nan
    c=norm[list(TICKERS[:3])].prod(axis=1,min_count=3)**(1/3)*100
    d=norm[list(TICKERS[3:])].prod(axis=1,min_count=3)**(1/3)*100
    ratio=c/d*100;prior=ratio.shift(lookback);change=(ratio/prior-1)*100
    flag=change.round(10).lt(0).astype('boolean').mask(change.isna())
    return pd.DataFrame({'cyclical':c,'defensive':d,'ratio':ratio,'prior_ratio':prior,
                         'change_pct':change,'DEFENSE':flag},index=cal)

def from_workbook(wb,sessions,settings):
    nr=[(pd.Timestamp(r[0]),r[1]) for r in wb[settings['nfci_sheet']].values
        if len(r)>1 and isinstance(r[0],datetime)]
    if len({x[0] for x in nr})!=len(nr):raise ValueError('Duplicate NFCI observation')
    w=weekly(pd.Series(dict(nr),dtype=float).sort_index(),settings['nfci_weeks'])
    wd=align(w,sessions,'weekly',True)
    rows=[r for r in wb[settings['sector_sheet']].values if len(r)>=7 and isinstance(r[0],datetime)]
    p=pd.DataFrame([r[1:7] for r in rows],columns=TICKERS,index=pd.DatetimeIndex([r[0] for r in rows]))
    return w,wd,sector_ratio(p,sessions,settings['sector_sessions'])
