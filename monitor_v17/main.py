"""Daily cloud entry point. No broker integration; reports only."""
import argparse, json, os, sys
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd
import exchange_calendars as xc
from .sources import BASE,DATA,MACROS,collect,factset,write_json
from .calculator import CONFIG,market_features,pe_weekly,evaluate
from .report import clean,render,send,delivery_key

def expression(node):
    if isinstance(node,str):return node
    if 'not' in node:return 'NOT('+expression(node['not'])+')'
    op=next(iter(node));return '('+(' AND ' if op=='all' else ' OR ').join(expression(x) for x in node[op])+')'

def generate(now=None,offline=False):
    now=pd.Timestamp(now or datetime.now(timezone.utc));now=now.tz_convert('UTC')
    cal=xc.get_calendar('XNYS',start='1957-01-01',end=str((now+pd.Timedelta(days=30)).date()))
    schedule=cal.schedule.loc[:now.strftime('%Y-%m-%d')]
    completed=schedule.index[pd.to_datetime(schedule['close'],utc=True)+pd.Timedelta(minutes=60)<=now]
    if len(completed)==0:raise ValueError('No completed equity session')
    asof=completed[-1].tz_localize(None) if completed.tz is not None else completed[-1]
    if offline:
        data={}
        metadata=json.loads((DATA/'source_metadata.json').read_text(encoding='utf8'))
        for k,meta in metadata.items():data[k]=(pd.read_csv(DATA/(k.replace('^','')+'.csv'),index_col=0,parse_dates=[0]).iloc[:,0],meta)
        errors={};facts=json.loads((DATA/'factset_reports.json').read_text(encoding='utf8'));fact_errors=[]
    else:
        data,errors=collect(asof);facts,fact_errors=factset(asof)
        write_json(DATA/'source_metadata.json',{k:v[1] for k,v in data.items()})
    required=['price_'+t for t in ['^GSPC','^VIX','^NDX','XLF','SPY','XLY','XLI','XLB','XLP','XLV','XLU']]+list(MACROS)
    missing=[k for k in required if k not in data]
    if missing:raise RuntimeError('必需来源尚无可用历史：'+', '.join(missing))
    seed=pd.read_csv(BASE/'seed/spx.csv',index_col=0,parse_dates=[0]).iloc[:,0]
    # The complete accepted warmup is immutable; append post-horizon raw index closes.
    current=data['price_^GSPC'][0];spx=pd.concat([seed,current.loc[current.index>seed.index[-1]]]).loc[:asof]
    new_sessions=cal.sessions_in_range(seed.index[-1]+pd.Timedelta(days=1),asof).tz_localize(None)
    if not new_sessions.isin(spx.dropna().index).all() or spx.index[-1]!=asof:
        raise RuntimeError('标普最新收盘或历史连续交易日缺失，停止计算价格分位')
    etfs=pd.concat({t:data['price_'+t][0] for t in ['XLF','SPY','XLY','XLI','XLB','XLP','XLV','XLU']},axis=1)
    sessions=cal.sessions_in_range('1957-01-02',asof).tz_localize(None)
    a,episodes=market_features(spx,data['price_^VIX'][0],data['price_^NDX'][0],etfs,sessions)
    meta={};fail=set();warnings=[]
    mapping={'price_^GSPC':'SPX','price_^VIX':'VIX','price_^NDX':'NDX',
             **{'price_'+t:'ETF' for t in etfs.columns},'DGS10':'Treasury','DGS2':'Treasury',
             'DCPF3M':'FCS','DTB3':'FCS',**{k:k for k in ('NFCI','CFNAIDIFF','PERMIT','CPIAUCSL')}}
    for k,(series,m) in data.items():
        dest=mapping[k]
        prior=meta.get(dest,{}).get('components',{})
        meta[dest]=dict(m,components=dict(prior,**{k:m}))
        if k in errors:fail.add(dest);warnings.append(f'{k}：获取失败，相关因子标为未知。')
        if k.startswith('price_') and (asof not in series.index or pd.isna(series.get(asof))):
            fail.add(dest);warnings.append(f'{k}：{asof.date()} 收盘缺失。')
    # NFCI expiration follows the existing expected weekly publication rule.
    from model.inventory_exit import weekly
    n=data['NFCI'][0];nextobs=n.index[-1]+pd.Timedelta(days=7)
    nextpub=weekly(pd.Series([np.nan],index=[nextobs])).published.iloc[0]
    if nextpub<=asof:fail.add('NFCI');warnings.append('NFCI 已到下一期发布日期，但新一期未取得。')
    for k in ('CFNAIDIFF','PERMIT','CPIAUCSL'):
        if (asof-data[k][0].index[-1]).days>80:
            fail.add(k);warnings.append(f'{k} 最新观察月超过 80 天，数据时效异常。')
    for key in ('DGS10','DGS2','DCPF3M','DTB3'):
        last=data[key][0].last_valid_index()
        if last is None or (asof-last).days>5:
            warnings.append(f'{key} 最近有效报价较旧；FCS 仍按正式 14 天规则判断。')
    seed_pe=json.loads((BASE/'seed/pe_history.json').read_text(encoding='utf8'))
    pe=pe_weekly(seed_pe,facts,spx,cal.sessions.tz_localize(None),asof)
    pp=pe.iloc[-1];meta['FactSet']={k:clean(pp.get(k)) for k in ['published_on','data_asof','source_url','publication_basis']}
    meta['FactSet']['observed_on']=clean(pp.get('data_asof'));meta['FactSet']['released_on']=clean(pp.get('published_on'))
    if pp.get('publication_basis')=='live_carry_last_FactSet':warnings.append('本周无新 FactSet 周报，按正式规则沿用最近一期；原始观察日和发布日期见表。')
    recent=pe.iloc[-260:]
    carry_count=int(recent.publication_basis.eq('live_carry_last_FactSet').sum())
    fed_count=int(recent.source.eq('Fed_monthly_fallback').sum())
    meta['FactSet']['note']=f'当前260周估值窗口：历史美联储补缺{fed_count}周；上线前后沿用上期FactSet共{carry_count}周。'
    if fact_errors:warnings.append(f'FactSet 有 {len(fact_errors)} 个待补周，已记录下载失败原因；没有以其他 PE 或固定值代替。')
    macro={k:data[k][0] for k in MACROS}
    r=evaluate(a,macro,pe,meta,asof,fail)
    meanings={}
    for rule in CONFIG['triggers']:meanings.update(rule.get('factor_meanings',{}))
    meanings.update({'HPG > 0':'住宅许可最近三个月均值÷去年同期三个月均值−1，严格大于0。',
                     'Sσ_200 ≤ -2σ':'标普价格减200日均线，再除以最近252日同类偏离的样本标准差，≤−2。'})
    for k,d in r['factors'].items():d['meaning']=meanings.get(k,'')
    unknown=[k for k,d in r['factors'].items() if d['value'] is None]
    if unknown:warnings.append('未知因子：'+'、'.join(unknown))
    exits=[x['name'] for x in CONFIG['triggers'] if x['side']=='exit'];entries=[x['name'] for x in CONFIG['triggers'] if x['side']=='entry']
    if any(r['triggers'][n] is True for n in exits):decision='离场组合触发；已有持仓时参考清仓信号。'
    elif any(r['triggers'][n] is None for n in exits):decision='离场条件存在未知，维持原状态，待数据恢复。'
    elif any(r['triggers'][n] is True for n in entries):decision='入场组合触发；当前空仓时参考入场信号。'
    elif any(r['triggers'][n] is None for n in entries):decision='入场条件存在未知，维持原状态。'
    else:decision='所有入场和离场组合均未触发，维持原状态。'
    r.update(status='degraded' if fail or unknown else 'ok',warnings=warnings,decision=decision,
        formulas={x['name']:expression({'all':x['all']}) for x in CONFIG['triggers']},
        generated_at=now.isoformat(),code_commit=os.environ.get('GITHUB_SHA','local'),
        session_note='使用最近已完成的美股交易日；休市日重复标明上一交易日截面。',
        download_errors=errors,factset_download_errors=fact_errors,run_mode='offline_validation' if offline else 'live')
    write_json(DATA/'latest_pe_weekly.json',clean(pe.reset_index(names='week').to_dict('records')))
    episodes.to_csv(DATA/'completed_drawdowns.csv',index=False)
    return clean(r)

def main():
    p=argparse.ArgumentParser();p.add_argument('--no-email',action='store_true');p.add_argument('--offline',action='store_true');p.add_argument('--now');a=p.parse_args()
    try:r=generate(a.now,a.offline)
    except Exception as e:
        # No raw request URLs or environment contents in reports/logs (API keys may be embedded).
        r={'model_version':CONFIG['version'],'status':'failed','asof':'未知',
           'generated_at':datetime.now(timezone.utc).isoformat(),'fatal_error':type(e).__name__+': '+str(e)[:220],
           'decision':'本次模型未完成有效计算，维持原状态。','warnings':['请检查工作流和数据源；本报告不代表无触发。']}
    out=BASE/'reports';out.mkdir(exist_ok=True);body=render(r)
    (out/'SP500_v17_latest.html').write_text(body,encoding='utf8');write_json(out/'SP500_v17_latest.json',r)
    write_json(DATA/'latest_report.json',r)
    daykey=datetime.now(timezone.utc).strftime('%Y-%m-%d')
    write_json(DATA/'daily'/f'{daykey}.json',r)
    key=delivery_key(r,daykey)
    sentfile=DATA/'mail_receipts.json';sent=json.loads(sentfile.read_text(encoding='utf8')) if sentfile.exists() else {}
    oldkey=daykey+'_'+CONFIG['version']+'_'+r['status']
    if not a.no_email and key not in sent and oldkey in sent and r['asof'] in sent[oldkey].get('subject',''):
        sent[key]=sent[oldkey];write_json(sentfile,sent)
    if not a.no_email and key not in sent:
        try:
            receipt=send(r,body);receipt['sent_at']=datetime.now(timezone.utc).isoformat();sent[key]=receipt
            write_json(sentfile,sent);write_json(out/'delivery_receipt.json',receipt);print('SMTP accepted report for configured mailbox.')
        except Exception as e:
            print('EMAIL FAILED: '+type(e).__name__);return 2
    elif not a.no_email:
        write_json(out/'delivery_receipt.json',sent[key]);print('Report already delivered for this UTC date, version and health state.')
    print(json.dumps({'status':r['status'],'asof':r.get('asof'),'triggers':r.get('triggers'),'warnings':r.get('warnings'),'error':r.get('fatal_error')},ensure_ascii=False))
    return 1 if r['status']!='ok' else 0

if __name__=='__main__':sys.exit(main())
