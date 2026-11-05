"""Live collection policy, separate from the frozen historical research inputs."""
import re
from html import unescape
import numpy as np
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
from model.financial_factors import calculate_fcs, weekly_fcs

POLICY_VERSION='live-data-20261105-cpi-gap1-fcs14d'
FCS_MAX_AGE_DAYS=14
CFNAI_CALENDAR_URL='https://www.chicagofed.org/research/data/data-release-calendar'
CPI_GAP_POLICY='single_missing_month_adjacent_original_mean'


def cpi_monthly(series,asof=None):
    """Fill isolated CPI months from their two available original neighbours only.

    Callers supply the release-filtered vintage for the calculation date. The
    optional cutoff also excludes future observation months. Never mutate the
    original series, extrapolate an edge, chain estimates or fill a longer gap.
    """
    raw=series.copy().astype(float).sort_index()
    if raw.index.has_duplicates or not isinstance(raw.index,pd.DatetimeIndex):
        raise ValueError('CPI needs unique monthly dates')
    if any(raw.index.day!=1):raise ValueError('CPI observations must use month-start dates')
    if asof is not None:raw=raw.loc[:pd.Timestamp(asof)]
    if raw.empty:raise ValueError('No CPI observations available')
    if ((raw.notna())&((~np.isfinite(raw))|(raw<=0))).any():
        raise ValueError('CPI observations must be positive finite index values')
    raw=raw.reindex(pd.date_range(raw.index.min(),raw.index.max(),freq='MS'))
    left,right=raw.shift(1),raw.shift(-1)
    fill=raw.isna()&left.notna()&right.notna()
    frame=pd.DataFrame({'raw_value':raw,'calculation_value':raw.mask(fill,(left+right)/2),
                        'is_imputed':fill},index=raw.index)
    frame.index.name='month'
    return frame


def cpi_gap_records(frame):
    """Explicit provenance for computed values, separate from official inputs."""
    rows=[]
    for month in frame.index[frame.is_imputed]:
        left=month-pd.DateOffset(months=1);right=month+pd.DateOffset(months=1)
        rows.append({'month':str(month.date()),'raw_value':None,
                     'calculation_value':float(frame.loc[month,'calculation_value']),
                     'method':CPI_GAP_POLICY,'left_month':str(left.date()),
                     'left_value':float(frame.loc[left,'raw_value']),
                     'right_month':str(right.date()),'right_value':float(frame.loc[right,'raw_value'])})
    return rows

def live_fcs(cp, treasury, day):
    """Use at least one paired quote; expire 14 calendar days after source Friday."""
    day=pd.Timestamp(day)
    w=weekly_fcs(calculate_fcs(cp.loc[:day],treasury.loc[:day]),min_count=1)
    federal=CustomBusinessDay(calendar=USFederalHolidayCalendar())
    w['available_on']=[d+federal for d in w.index]
    w=w.loc[(w.available_on<=day)&(w.index<day)&w.qualified_pp.notna()]
    if w.empty:
        return {'FCS_pp':np.nan,'last_valid_FCS_pp':np.nan,'source_week':None,
                'age_days':None,'valid_days':0,'available_on':None}
    row=w.iloc[-1];week=w.index[-1]
    age=(day-week).days
    return {'FCS_pp':row.qualified_pp if age<=FCS_MAX_AGE_DAYS else np.nan,
            'last_valid_FCS_pp':row.qualified_pp,'source_week':week,'age_days':age,
            'valid_days':int(row.valid_days),'available_on':row.available_on}

def parse_cfnai_calendar(body):
    text=body.decode('utf-8') if isinstance(body,bytes) else body
    match=re.search(r'<table\b[^>]*\bid=["\']cfnai-table["\'][^>]*>(.*?)</table>',text,re.S|re.I)
    if not match:raise ValueError('CFNAI release calendar table missing')
    rows=[]
    for row in re.findall(r'<tr\b[^>]*>(.*?)</tr>',match[1],re.S|re.I):
        cells=[unescape(re.sub('<[^>]+>','',x)).strip() for x in re.findall(r'<td\b[^>]*>(.*?)</td>',row,re.S|re.I)]
        if len(cells)!=2:continue
        published=pd.to_datetime(cells[0],format='%B %d, %Y')
        # The last explicitly named month is the newest observation in a combined release.
        months=re.findall(r'([A-Z][a-z]+)\s+(20\d{2})',cells[1])
        if not months:raise ValueError('CFNAI reference month missing')
        observed=pd.to_datetime(' '.join(months[-1]),format='%B %Y')
        rows.append({'published_on':str(published.date()),'observed_on':str(observed.date())})
    if len(rows)<2:raise ValueError('Incomplete CFNAI calendar')
    return sorted(rows,key=lambda r:r['published_on'])

def cfnai_health(observed, asof, calendar):
    """Never expire at month-start +80 days. Alert only after a scheduled release is usable."""
    observed=pd.Timestamp(observed);day=pd.Timestamp(asof)
    newer=[r for r in calendar if pd.Timestamp(r['observed_on'])>observed]
    due=[r for r in newer if pd.Timestamp(r['published_on'])<day]
    if due:
        # A second missed release must not restart the grace period for old data.
        r=due[0]
        overdue=(day-pd.Timestamp(r['published_on'])).days
        return overdue>7,f"CFNAI 官方计划 {r['published_on']} 发布 {r['observed_on'][:7]} 数据，尚未取得；沿用旧值，逾期超过7天才标记持续发布延迟或采集异常。"
    if newer:
        r=newer[0]
        return False,f"CFNAI 沿用 {observed:%Y-%m} 已发布数据；下一期计划 {r['published_on']} 发布，发布后的首个交易日起采用。"
    return True,'CFNAI 官方发布日历未覆盖下一期，需更新日历；旧值保留供核查。'
