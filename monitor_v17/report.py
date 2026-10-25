"""Daily report and SMTP delivery; success means SMTP accepted, never inferred from logging."""
import csv, html, io, json, os, smtplib
from email.message import EmailMessage
from pathlib import Path

LABEL={True:'满足',False:'不满足',None:'未知'}

def delivery_key(report,utc_day):
    # A pre-close manual test and the post-close daily run may share a UTC date.
    return '_'.join([utc_day,report.get('asof','未知'),report['model_version'],report['status']])

FIELDS={'S':'标普收盘','S_lag_126':'126交易日前收盘','S_dn_126':'126交易日中下跌日数',
        'S_drawdown_pct':'当前回撤','DD_P95':'P95','DD_P99':'P99','sample_count':'完整轮次数','input_cutoff':'分位输入截止',
        'V':'VIX','V_c':'平静期均值','V_c_sd':'平静期标准差','V_threshold':'均值＋3σ',
        'Sσ_200':'价格偏离σ倍数','FSX':'金融板块相对收益','CYCDEF_change_pct':'周期／防御比值半年变化',
        'NSX':'纳指相对标普超涨','NFCI_4w':'4周前NFCI','NFCI_26w':'26周前NFCI','change':'NFCI变化',
        'NFCI_high52':'52周最高NFCI','low36':'36月低点','mean3':'许可3月均值','previous_mean3':'去年同期均值',
        'CPI_y_pp':'CPI同比','10Y_minus_2Y_pp':'10年−2年收益率','paired_date':'同日报价日期',
        'FCS_pp':'融资利差','source_week':'来源周','age_days':'距来源周天数','valid_days':'有效报价天数','available_on':'可用日期','P':'预期PE',
        'mean260':'260周均值','sd260':'260周标准差','threshold':'PE上界','E':'预期EPS','E_4w':'4周前EPS','E_8w':'8周前EPS'}

def format_input(k,v):
    if v is None:return '未知'
    if isinstance(v,(float,int)):
        if k in ('S_drawdown_pct','DD_P95','DD_P99','FSX','NSX','HPG'):return f'{v*100:.4f}%'
        if k in ('CPI_y_pp','CYCDEF_change_pct'):return f'{v:.4f}%'
        if k in ('FCS_pp','10Y_minus_2Y_pp'):return f'{v:.4f}个百分点'
        return f'{v:.6g}'
    return str(v)
def clean(value):
    import numpy as np
    import pandas as pd
    if isinstance(value,dict):return {str(k):clean(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [clean(x) for x in value]
    if value is None or pd.isna(value):return None
    if isinstance(value,(pd.Timestamp,)):return value.isoformat()
    if isinstance(value,np.generic):return value.item()
    return value

def render(r):
    esc=lambda x:html.escape(str(x))
    warnings=r.get('warnings',[])
    title='标普500正式模型 v17 日报'
    status=r.get('status','ok')
    lead='本次计算完成' if status=='ok' else '数据或运行异常，请查看明细'
    if r.get('fatal_error'):lead+='；'+r['fatal_error']
    rows=[]
    for name,result in r.get('triggers',{}).items():
        rows.append(f'<tr><td>{esc(name)}</td><td>{LABEL[result]}</td><td>{esc(r.get("formulas",{}).get(name,""))}</td></tr>')
    factors=[]
    for name,d in r.get('factors',{}).items():
        meta=d.get('source_status',{})
        val='；'.join(f'{FIELDS.get(k,k)}={format_input(k,v)}' for k,v in d['inputs'].items())
        factors.append(f'<tr><td>{esc(name)}</td><td>{LABEL[d["value"]]}</td><td>{esc(val)}</td>'
                       f'<td>{esc(meta.get("observed_on",""))}<br>{esc(meta.get("released_on",""))}</td>'
                       f'<td>{esc(d.get("meaning",""))}</td></tr>')
    sources=[]
    for name,d in r.get('sources',{}).items():
        url=d.get('source_url','');link=f'<a href="{esc(url)}">来源</a>' if url else ''
        if d.get('components'):
            link=' '.join(f'<a href="{esc(v.get("source_url",""))}">{esc(k)}</a> ({esc(v.get("observed_on",""))})' for k,v in d['components'].items())
        sources.append(f'<tr><td>{esc(name)} {link}</td><td>{esc(d.get("observed_on",""))}</td>'
                       f'<td>{esc(d.get("released_on",""))}</td><td>{esc(d.get("note",""))}</td></tr>')
    return f'''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><body style="font-family:Arial,Microsoft YaHei,sans-serif;color:#17324d;max-width:1150px;margin:auto;padding:24px">
<h1>{title}</h1><p>美股数据截面：<b>{esc(r.get('asof','未知'))}</b>；生成时间：{esc(r.get('generated_at',''))}</p>
<p><b>{esc(lead)}</b></p><p>{esc(r.get('decision',''))}</p><p>{esc(r.get('session_note',''))}</p>
<p>报告列示模型条件；实际持仓未在本程序中维护。有持仓时参考离场，无持仓时参考入场；离场优先，未知维持原状态。信号对应下一美股交易日收盘执行口径。</p>
<ul>{''.join('<li>'+esc(w)+'</li>' for w in warnings)}</ul>
<h2>组合</h2><table border="1" cellpadding="7" cellspacing="0" style="border-collapse:collapse;width:100%"><tr><th>组合</th><th>状态</th><th>完整表达式</th></tr>{''.join(rows)}</table>
<h2>各因子计算值</h2><table border="1" cellpadding="7" cellspacing="0" style="border-collapse:collapse;width:100%;font-size:13px"><tr><th>因子</th><th>状态</th><th>计算输入</th><th>观察日／发布日期</th><th>含义</th></tr>{''.join(factors)}</table>
<h2>数据来源与时效</h2><table border="1" cellpadding="7" cellspacing="0" style="border-collapse:collapse;width:100%"><tr><th>来源</th><th>观察日</th><th>发布日期</th><th>说明</th></tr>{''.join(sources)}</table>
<p>P95、P99 均只用截面前一交易日及以前已恢复前高的完整回撤轮次计算，等权 Type 7 线性分位；未完成的本轮回撤不进入分位样本。CPI 门槛固定为 5.39%。</p>
<p>模型版本：{esc(r.get('model_version','v17'))}<br>数据采集规则：{esc(r.get('data_policy_version','原始规则'))}<br>代码提交：{esc(r.get('code_commit','local'))}<br>
<a href="https://github.com/zhanghangrui1984-create/sp500-monitor/blob/main/docs/因子获取与计算_v17.md">全部因子获取、计算与运行说明</a></p></body></html>'''

def send(r,body):
    sender=os.environ.get('EMAIL_ADDRESS','').strip();password=os.environ.get('EMAIL_PASSWORD','')
    receiver=(os.environ.get('EMAIL_RECEIVER') or sender).strip()
    if not sender or not password or not receiver:raise RuntimeError('邮件环境变量未配置完整')
    # Reuse only the explicitly configured mailbox, never infer other recipients.
    msg=EmailMessage();msg['From']=sender;msg['To']=receiver
    triggered=[n for n,v in r.get('triggers',{}).items() if v is True]
    tag='异常' if r.get('status')!='ok' else '触发' if triggered else '日报'
    msg['Subject']=f'【标普500 v17·{tag}】{r.get("asof","运行失败")} '+('、'.join(triggered) if triggered else '')
    msg.set_content(f'标普500 v17 报告。数据截面：{r.get("asof")}。'+r.get('decision','')+'\n请查看 HTML 正文或附件。')
    msg.add_alternative(body,subtype='html')
    msg.add_attachment(body.encode('utf8'),maintype='text',subtype='html',filename=f'SP500_v17_{r.get("asof","error")}.html')
    with smtplib.SMTP_SSL('smtp.gmail.com',465,timeout=40) as server:
        server.login(sender,password);rejected=server.send_message(msg)
    if rejected:raise RuntimeError('SMTP rejected recipient')
    local,domain=receiver.rsplit('@',1)
    return {'smtp_accepted':True,'receiver_masked':local[:3]+'***@'+domain,'subject':msg['Subject']}
