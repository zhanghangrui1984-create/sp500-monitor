"""The 20 active v17 predicates. Numerical kernels are shared with research."""
import json
from pathlib import Path
import numpy as np
import pandas as pd
from model.dynamic_drawdown import calculate_daily
from model.financial_factors import calculate_fsx
from .data_policy import live_fcs
from model.common_exit_factors import sector_ratio
from model.inventory_exit import weekly
from model.nfr import calculate_nfr
from model.erx import calculate_erx
from model.hpg import calculate_hpg
from .shared_core import calm_baseline, triggers_from_factors

CONFIG=json.loads(Path(__file__).with_name('model.json').read_text(encoding='utf8'))

def cmp(a,op,b):
    if pd.isna(a) or pd.isna(b):return None
    return bool(getattr(np.float64(round(float(a),12)),op)(round(float(b),12)))

def market_features(spx,vix,ndx,etfs,sessions):
    """No filling or dropping equity sessions; only SPX's frozen warmup is retained."""
    idx=pd.DatetimeIndex(sessions);spx=spx.reindex(idx)
    if spx.loc['1975-01-01':].isna().any():raise ValueError('Missing SPX exchange session from 1975 onward')
    p=CONFIG['parameters'];a=pd.DataFrame({'S':spx},index=idx)
    a['V']=vix.reindex(idx)
    a['MA200']=spx.rolling(p['price_ma']).mean();dev=spx-a.MA200
    a['Sσ_200']=dev/dev.rolling(p['deviation_std']).std(ddof=1)
    a['S_lag_126']=spx.shift(126)
    a['S_dn_126']=spx.diff().lt(0).where(spx.diff().notna()).rolling(126).sum()
    a['V_c'],a['V_c_sd']=calm_baseline(spx,a.V,252,63,.10)
    a['V_threshold']=a.V_c+3*a.V_c_sd
    q=CONFIG['dynamic_drawdown'];dd,episodes,_=calculate_daily(spx.dropna(),episode_start=q['episode_start'],min_episodes=q['min_episodes'])
    for k in ['S_drawdown_pct','DD_P95','DD_P99','sample_count','input_cutoff']:a[k]=dd[k]
    a['NSX']=(ndx.reindex(idx)/spx)/(ndx.reindex(idx).shift(252)/spx.shift(252))-1
    a['FSX']=calculate_fsx(etfs.XLF,etfs.SPY,idx).FSX
    sector=sector_ratio(etfs,idx)
    a['CYCDEF_change_pct']=sector.change_pct;a['DEFENSE']=sector.DEFENSE
    return a,episodes

def pe_weekly(seed,reports,spx,calendar,asof):
    """Historical accepted seed, then only FactSet; approved carry uses original as-of price."""
    asof=pd.Timestamp(asof);cal=pd.DatetimeIndex(calendar)
    rows=[dict(r) for r in seed if pd.Timestamp(r['effective_on'])<=asof]
    if not rows:raise ValueError('Missing frozen PE warmup')
    last_week=pd.Timestamp(rows[-1]['week']);target=asof-pd.Timedelta(days=asof.weekday()+7)
    latest=None;byweek={}
    for r in sorted(reports,key=lambda x:x['published_on']):
        pub=pd.Timestamp(r['published_on'])
        if pub>asof:continue
        w=pub-pd.Timedelta(days=pub.weekday());byweek[w]=r
        if w<=last_week:latest=r
    for w in pd.date_range(last_week+pd.Timedelta(days=7),target,freq='W-MON'):
        if w in byweek:latest=byweek[w]
        effective=cal[cal.searchsorted(w+pd.Timedelta(days=7))]
        if latest is None:
            rows.append({'week':str(w.date()),'effective_on':str(effective.date()),'E':None,'P':None});continue
        price_date=pd.Timestamp(latest['data_asof']);pe=latest.get('pe_reported')
        eligible=spx.reindex(cal).loc[:price_date].dropna()
        actual_price_date=eligible.index[-1] if len(eligible) else price_date
        price=eligible.iloc[-1] if len(eligible) and (price_date-actual_price_date).days<=4 else np.nan
        if pe is None and latest.get('eps_method')=='direct_reported' and latest.get('eps_ntm',0)>0:
            pe=price/latest['eps_ntm']
        if pe is None or not np.isfinite(pe) or pe<=0 or not np.isfinite(price):
            raise ValueError('FactSet requires positive PE and same observation date SPX')
        rows.append(dict(week=str(w.date()),effective_on=str(effective.date()),P=float(pe),E=float(price/pe),
            published_on=latest['published_on'],data_asof=str(actual_price_date.date()),source='FactSet',
            source_url=latest['source_url'],publication_basis='actual_publication' if w in byweek else 'live_carry_last_FactSet'))
    frame=pd.DataFrame(rows).set_index('week');frame.index=pd.to_datetime(frame.index)
    frame=frame.reindex(pd.date_range(frame.index[0],frame.index[-1],freq='W-MON'))
    frame['E_4w']=frame.E.shift(4);frame['E_8w']=frame.E.shift(8)
    frame['P_Avg_260w']=frame.P.rolling(260,min_periods=260).mean()
    frame['P_Std_260w']=frame.P.rolling(260,min_periods=260).std(ddof=1)
    return frame

def evaluate(a,macro,pe,source_meta,asof,failures=()):
    """Produce explicit three-valued conditions and source data, never defaults for missing values."""
    day=pd.Timestamp(asof);v=a.loc[day].to_dict();f={};details={}
    def put(name,value,inputs,source):
        f[name]=None if value is None or pd.isna(value) else bool(value)
        details[name]={'value':f[name],'inputs':inputs,'source':source,'source_status':source_meta.get(source,{})}
    def m(name):return macro[name].loc[:day]
    put('S-6',None if pd.isna(v['S_lag_126']) else v['S']<v['S_lag_126'] and v['S_dn_126']>=61,
        {k:v[k] for k in ['S','S_lag_126','S_dn_126']},'SPX')
    put('S- ≥ P99',cmp(v['S_drawdown_pct'],'__ge__',v['DD_P99']),{k:v[k] for k in ['S_drawdown_pct','DD_P99','sample_count','input_cutoff']},'SPX')
    middle=None if pd.isna(v['DD_P95']) or pd.isna(v['DD_P99']) else round(v['DD_P95'],12)<=round(v['S_drawdown_pct'],12)<round(v['DD_P99'],12)
    put('P95 ≤ S- < P99',middle,{k:v[k] for k in ['S_drawdown_pct','DD_P95','DD_P99','sample_count','input_cutoff']},'SPX')
    put('V ≥ V_c+3σ',cmp(v['V'],'__ge__',v['V_threshold']),{k:v[k] for k in ['V','V_c','V_c_sd','V_threshold']},'VIX')
    put('Sσ_200 ≤ -2σ',cmp(v['Sσ_200'],'__le__',-2),{'Sσ_200':v['Sσ_200']},'SPX')
    put('FSX ≤ -10%',None if pd.isna(v['FSX']) else v['FSX']<=-.10,{'FSX':v['FSX']},'ETF')
    put('DEFENSE',v['DEFENSE'],{'CYCDEF_change_pct':v['CYCDEF_change_pct']},'ETF')
    put('NSX ≥ 40%',None if pd.isna(v['NSX']) else v['NSX']>=.40,{'NSX':v['NSX']},'NDX')
    n=m('NFCI');n4=weekly(n,4).iloc[-1];n26=weekly(n,26).iloc[-1];nr=calculate_nfr(m('NFR_NFCI') if 'NFR_NFCI' in macro else n).iloc[-1]
    put('NFCI_4w_down',cmp(n4.NFCI_change,'__lt__',0),{'NFCI':n4.NFCI,'NFCI_4w':n4.NFCI_lag,'change':n4.NFCI_change},'NFCI')
    put('NFCI_26w_up',n26.NFCI_up,{'NFCI':n26.NFCI,'NFCI_26w':n26.NFCI_lag,'change':n26.NFCI_change},'NFCI')
    put('N < 0',cmp(n.iloc[-1],'__lt__',0),{'NFCI':n.iloc[-1]},'NFCI')
    put('NFR ≥ 0.958',nr.NFR_II,{'NFR':nr.NFR,'NFCI_high52':nr.rolling_high,'NFCI':nr.NFCI},'NFCI')
    er=calculate_erx(m('CFNAIDIFF')).iloc[-1]
    put('ERX达标',er.ERX_qualified,{'CFNAIDIFF':er.CFNAIDIFF,'low36':er.rolling_low,'ERX':er.ERX},'CFNAIDIFF')
    hp=calculate_hpg(m('PERMIT')).iloc[-1]
    put('HPG > 0',hp.HPG_positive,{'PERMIT':hp.PERMIT,'mean3':hp.mean3,'previous_mean3':hp.same_period_last_year_mean3,'HPG':hp.HPG},'PERMIT')
    c=m('CPIAUCSL');c=c.reindex(pd.date_range(c.index.min(),c.index.max(),freq='MS'))
    cy=100*(c/c.shift(12)-1).iloc[-1]
    put('CPI_y ≥ 5.39%',cmp(cy,'__ge__',5.39),{'CPI_y_pp':cy},'CPIAUCSL')
    put('CPI_y < 0%',cmp(cy,'__lt__',0),{'CPI_y_pp':cy},'CPIAUCSL')
    pair=pd.concat([m('DGS10'),m('DGS2')],axis=1).dropna();yy=np.nan
    if len(pair) and (day-pair.index[-1]).days<=5:yy=pair.iloc[-1,0]-pair.iloc[-1,1]
    put('Y-',cmp(yy,'__lt__',0),{'10Y_minus_2Y_pp':yy,'paired_date':pair.index[-1] if len(pair) else None},'Treasury')
    credit=live_fcs(m('DCPF3M'),m('DTB3'),day)
    put('FCS ≥ 1.00pp',cmp(credit['FCS_pp'],'__ge__',1.),credit,'FCS')
    pp=pe.iloc[-1];threshold=pp.P_Avg_260w+pp.P_Std_260w
    put('P+',cmp(pp.P,'__gt__',threshold),{'P':pp.P,'mean260':pp.P_Avg_260w,'sd260':pp.P_Std_260w,'threshold':threshold},'FactSet')
    e2=None if any(pd.isna(pp[k]) for k in ['E','E_4w','E_8w']) else round(pp.E,12)<round(pp.E_4w,12)<round(pp.E_8w,12)
    put('E-2',e2,{k:pp[k] for k in ['E','E_4w','E_8w']},'FactSet')
    # Transport or known freshness failures are explicit unknowns, not stale healthy readings.
    for name,d in details.items():
        if d['source'] in failures:f[name]=d['value']=None;d['unavailable_reason']='数据获取或时效检查失败'
    ff=pd.DataFrame({k:pd.Series([x],dtype='boolean') for k,x in f.items()})
    tt=triggers_from_factors(ff,CONFIG['triggers']).iloc[0]
    return {'model_version':CONFIG['version'],'asof':str(day.date()),'market':v,'factors':details,
            'triggers':{k:None if pd.isna(x) else bool(x) for k,x in tt.items()},'sources':source_meta}
