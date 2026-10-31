"""Live collection policy, separate from the frozen historical research inputs."""
import re
from html import unescape
import numpy as np
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
from model.financial_factors import calculate_fcs, weekly_fcs

POLICY_VERSION='live-data-20261031-fcs14d'
FCS_MAX_AGE_DAYS=14
CFNAI_CALENDAR_URL='https://www.chicagofed.org/research/data/data-release-calendar'

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
