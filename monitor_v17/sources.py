"""Validated public inputs, bounded retries, raw evidence and no invented fallback values."""
import concurrent.futures, hashlib, html, io, json, os, re, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote
from urllib.error import HTTPError
import numpy as np
import pandas as pd
from pypdf import PdfReader
from .data_policy import CFNAI_CALENDAR_URL, parse_cfnai_calendar

BASE=Path(__file__).resolve().parents[1]
DATA=BASE/'data/v17';RAW=DATA/'raw'
MACROS=('NFCI','CFNAIDIFF','PERMIT','CPIAUCSL','DGS10','DGS2','DCPF3M','DTB3')
MONTHS='January February March April May June July August September October November December'
FACTSET_BASE='https://advantage.factset.com/hubfs/Website/Resources%20Section/Research%20Desk/Earnings%20Insight/'

def write_json(path,data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+'.tmp');temp.write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf8');temp.replace(path)

def request(url,key,attempts=3,validate=None):
    RAW.mkdir(parents=True,exist_ok=True)
    for attempt in range(attempts):
        try:
            with urlopen(Request(url,headers={'User-Agent':'Mozilla/5.0 (SP500 research monitor)'}),timeout=25) as r:body=r.read()
            if validate is not None:validate(body)
            digest=hashlib.sha256(body).hexdigest()
            path=RAW/(key+'_'+digest[:16]+'.bin');path.write_bytes(body)
            return body,{'sha256':digest,'raw_file':str(path.relative_to(BASE)),
                         'retrieved_at':datetime.now(timezone.utc).isoformat()}
        except HTTPError as e:
            if e.code in (400,401,403,404) or attempt==attempts-1:raise RuntimeError(f'{key}: HTTP {e.code}') from None
        except Exception as e:
            if attempt==attempts-1:raise RuntimeError(f'{key}: {type(e).__name__}') from None
        time.sleep(2**attempt)

def parse_yahoo(raw,ticker,end,adjusted=False,now=None):
    """A completed regular-session quote can fill Yahoo's delayed final daily row."""
    import exchange_calendars as xc
    result=json.loads(raw)['chart']
    if result.get('error') or not result.get('result'):raise ValueError('Yahoo chart error')
    obj=result['result'][0];q=obj['indicators'];day=pd.Timestamp(end).normalize()
    vals=q['adjclose'][0]['adjclose'] if adjusted else q['quote'][0]['close']
    idx=pd.to_datetime(obj['timestamp'],unit='s',utc=True).tz_convert('America/New_York').tz_localize(None).normalize()
    s=pd.Series(vals,index=idx,dtype=float).sort_index().loc[:day]
    if s.index.has_duplicates or s.empty or ((s<=0)&s.notna()).any():raise ValueError('Invalid price series '+ticker)
    evidence={}
    if pd.isna(s.get(day,np.nan)):
        meta=obj['meta'];stamp=pd.Timestamp(meta.get('regularMarketTime',0),unit='s',tz='UTC')
        current=pd.Timestamp(now or datetime.now(timezone.utc))
        cal=xc.get_calendar('XNYS',start=str(day.date()),end=str((day+pd.Timedelta(days=7)).date()))
        close=cal.session_close(day)
        price=meta.get('regularMarketPrice')
        # No intraday, after-hours, different-date, or historical-date substitution.
        if (day not in idx or idx[-1]!=day or stamp.tz_convert('America/New_York').date()!=day.date()
            or not close<=stamp<=close+pd.Timedelta(minutes=5) or current<close+pd.Timedelta(minutes=60)
            or price is None or not np.isfinite(price) or price<=0):
            raise ValueError('Completed regular-session close unavailable '+ticker)
        # On the latest completed quote date the adjustment factor is one. Earlier
        # values retain the vendor's adjusted history (including same-day distributions).
        s.loc[day]=float(price)
        evidence={'close_completion':'regularMarketPrice_at_verified_session_close',
                  'regular_market_time':stamp.isoformat(),'exchange_close':close.isoformat(),
                  'note':'Yahoo 当日历史行延迟；用同一响应中经交易所收盘时刻核验的常规时段收盘价补齐。'}
    if not adjusted:s=s.round(2)
    return s,evidence

def yahoo(ticker,start,end,adjusted=False):
    params={'period1':int(pd.Timestamp(start,tz='UTC').timestamp()),
            'period2':int((pd.Timestamp(end,tz='UTC')+pd.Timedelta(days=1)).timestamp()),'interval':'1d','events':'history'}
    url='https://query1.finance.yahoo.com/v8/finance/chart/'+quote(ticker,safe='')+'?'+urlencode(params)
    raw,meta=request(url,'yahoo_'+ticker.replace('^',''),validate=lambda b:parse_yahoo(b,ticker,end,adjusted))
    s,evidence=parse_yahoo(raw,ticker,end,adjusted)
    meta.update(source_url=url,observed_on=str(s.last_valid_index().date()),adjusted=adjusted,**evidence)
    return s,meta

def validate_fred(body):
    obj=json.loads(body)
    if not isinstance(obj.get('observations'),list) or not obj['observations']:
        raise ValueError('FRED observations missing')
    if len(obj['observations'])!=obj.get('count'):raise ValueError('FRED pagination not exhausted')
    return obj

def cfnai_calendar():
    path=DATA/'cfnai_calendar.json'
    try:
        raw,proof=request(CFNAI_CALENDAR_URL,'cfnai_calendar',validate=parse_cfnai_calendar)
        result={'rows':parse_cfnai_calendar(raw),'source_url':CFNAI_CALENDAR_URL,**proof}
        write_json(path,result)
        return result
    except Exception:
        cached=json.loads((path if path.exists() else BASE/'seed/cfnai_calendar.json').read_text(encoding='utf8'))
        return dict(cached,fetch_failed=True)

def fred(series,asof):
    key=os.environ.get('FRED_API_KEY')
    if not key:raise RuntimeError('FRED_API_KEY is not configured')
    args={'api_key':key,'series_id':series,'file_type':'json','observation_start':'2010-01-01',
          'observation_end':str(asof),'realtime_start':str(asof),'realtime_end':str(asof),'limit':100000}
    raw,meta=request('https://api.stlouisfed.org/fred/series/observations?'+urlencode(args),'fred_'+series,validate=validate_fred)
    result=json.loads(raw);rows=result['observations']
    if len(rows)!=result['count']:raise ValueError('FRED pagination not exhausted')
    values=pd.Series({pd.Timestamp(r['date']):float(r['value']) if r['value']!='.' else np.nan for r in rows},dtype=float).sort_index()
    if values.empty or not values.notna().any():raise ValueError('Empty FRED series '+series)
    meta.update(source_url='https://fred.stlouisfed.org/series/'+series,observed_on=str(values.index[-1].date()),vintage_asof=str(asof))
    if series in ('NFCI','CFNAIDIFF','PERMIT','CPIAUCSL'):
        # FRED's initial-release output supplies actual dates; historical values still use the requested vintage.
        args.update(output_type=4,realtime_start='1776-07-04',observation_start='2024-01-01')
        initial,proof=request('https://api.stlouisfed.org/fred/series/observations?'+urlencode(args),'release_'+series,validate=validate_fred)
        releases={pd.Timestamp(r['date']):pd.Timestamp(r['realtime_start']) for r in json.loads(initial)['observations']}
        usable=[]
        for obs in values.index:
            if obs<pd.Timestamp('2024-01-01'):usable.append(True);continue
            release=releases.get(obs)
            usable.append(release is not None and (release<pd.Timestamp(asof) if series in ('CFNAIDIFF','PERMIT') else release<=pd.Timestamp(asof)))
        values=values.loc[usable]
        if values.empty:raise ValueError('No confirmed usable release '+series)
        meta.update(observed_on=str(values.index[-1].date()),released_on=str(releases[values.index[-1]].date()),release_evidence=proof)
    return values,meta

def parse_factset(body,url,expected=None):
    """Only official Earnings Insight contemporaneous forward-12m PE and dated valuation."""
    if not body.startswith(b'%PDF'):raise ValueError('FactSet response is not PDF')
    reader=PdfReader(io.BytesIO(body));pages=[p.extract_text() or '' for p in reader.pages[:5]]
    first=re.sub(r'\s+',' ',pages[0]);dates=re.findall(r'('+'|'.join(MONTHS.split())+r')\s+(\d{1,2}),?\s+(20\d{2})',first)
    if not dates:raise ValueError('Missing publication date')
    pub=pd.Timestamp(datetime.strptime(' '.join(dates[0]),'%B %d %Y').date())
    if expected and abs((pub-pd.Timestamp(expected)).days)>2:raise ValueError('Report publication date mismatch')
    text=re.sub(r'\s+',' ',' '.join(pages));text=re.sub(r'12\s*-\s*month','12-month',text,flags=re.I)
    pattern=r'(?:The\s+)?forward\s+12-month P/E ratio\s*(?:for the S&P\s*500\s*)?(?:is|stands at)\s*(?:now\s*)?(\d{1,2}\.\d+)\b'
    matches=re.findall(pattern,text,re.I)
    if not matches or len(set(matches))!=1:raise ValueError('Unambiguous current S&P500 forward PE not found')
    pe=float(matches[0])
    if not 3<pe<80:raise ValueError('Implausible forward PE')
    # The accepted research convention uses the report's prior equity close.
    # The caller verifies the date against XNYS; unusual publications require explicit evidence.
    if pub.weekday() not in (2,3,4):raise ValueError('Unexpected report weekday')
    asof=pub-pd.Timedelta(days=1)
    return dict(report_id='FactSet_'+str(pub.date()),published_on=str(pub.date()),data_asof=str(asof.date()),
                pe_reported=pe,horizon='forward_12_months',eps_method='price_divided_by_rounded_forward_pe',
                source_url=url,asof_basis='accepted_report_prior_equity_close_convention')

def factset(asof):
    path=DATA/'factset_reports.json'
    rows=json.loads((path if path.exists() else BASE/'seed/factset_reports.json').read_text(encoding='utf8'))
    # Keep trying failed recent weeks; old gaps are retried weekly, with approved carry between attempts.
    last=max(pd.Timestamp(r['published_on']) for r in rows);start=pd.Timestamp('2026-02-27')
    dates=pd.date_range(start,pd.Timestamp(asof),freq='W-FRI')
    covered={pd.Timestamp(r['published_on'])-pd.Timedelta(days=pd.Timestamp(r['published_on']).weekday()) for r in rows}
    attempted_path=DATA/'factset_attempts.json'
    attempts=json.loads(attempted_path.read_text(encoding='utf8')) if attempted_path.exists() else {}
    jobs=[d for d in dates if d-pd.Timedelta(days=4) not in covered and
          (d>=pd.Timestamp(asof)-pd.Timedelta(days=21) or str(d.date()) not in attempts or
           pd.Timestamp(attempts[str(d.date())]['checked_on'])<=pd.Timestamp(asof)-pd.Timedelta(days=7))]
    errors=[];new=[]
    def one(d):
        reasons=[]
        for date in (d,d-pd.Timedelta(days=1),d-pd.Timedelta(days=2)):
            for suffix in ('','A','B'):
                url=FACTSET_BASE+'EarningsInsight_'+date.strftime('%m%d%y')+suffix+'.pdf'
                try:
                    body,meta=request(url,'factset_'+date.strftime('%Y%m%d')+suffix,attempts=2)
                    r=parse_factset(body,url,d);r.update(meta);return r
                except Exception as e:
                    reason=str(e);reasons.append(reason)
                    if 'HTTP 429' in reason:return {'error':reason,'week':str(d.date())}
        return {'error':'; '.join(reasons),'week':str(d.date())}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        for result in pool.map(one,jobs):
            if 'error' in result:
                errors.append(result);attempts[result['week']]={'checked_on':str(asof),'error':result['error']}
            else:new.append(result)
    bydate={r['published_on']:r for r in rows}
    bydate.update({r['published_on']:r for r in new if r['published_on']<=str(asof)})
    rows=sorted(bydate.values(),key=lambda r:r['published_on']);write_json(path,rows);write_json(attempted_path,attempts)
    return rows,errors

def collect(asof):
    """Fetch independent sources concurrently, surface each failed source explicitly."""
    DATA.mkdir(parents=True,exist_ok=True);asof=str(pd.Timestamp(asof).date())
    jobs={f'price_{t}':lambda t=t:yahoo(t,'2015-01-01' if t=='^VIX' else '2023-01-01',asof,t[0]!='^')
          for t in ['^GSPC','^VIX','^NDX','XLF','SPY','XLY','XLI','XLB','XLP','XLV','XLU']}
    jobs.update({s:lambda s=s:fred(s,asof) for s in MACROS})
    results={};errors={}
    metadata_path=DATA/'source_metadata.json'
    previous=json.loads(metadata_path.read_text(encoding='utf8')) if metadata_path.exists() else {}
    def one(k):
        try:return k,jobs[k](),None
        except Exception as e:return k,None,str(e)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        for k,result,error in pool.map(one,jobs):
            cache=DATA/(k.replace('^','')+'.csv')
            if error:
                errors[k]=error
                if cache.exists():
                    frame=pd.read_csv(cache,index_col=0,parse_dates=[0])
                    results[k]=(frame.iloc[:,0],dict(previous.get(k,{}),source_status='fetch_failed_using_cache'))
                continue
            series,meta=result;series.rename('value').to_csv(cache,index_label='date');results[k]=result
    return results,errors
