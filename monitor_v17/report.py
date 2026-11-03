"""Daily report and SMTP delivery; success means SMTP accepted, never inferred from logging."""
import os, smtplib
from email.message import EmailMessage
from pathlib import Path

def delivery_key(report,utc_day):
    # A pre-close manual test and the post-close daily run may share a UTC date.
    return '_'.join([utc_day,report.get('asof','未知'),report['model_version'],report['status']])

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
    from .daily_report import render_html
    return render_html(r)


def build_message(r,body,docx):
    from .daily_report import render_text
    sender=os.environ.get('EMAIL_ADDRESS','').strip()
    receiver=(os.environ.get('EMAIL_RECEIVER') or sender).strip()
    if not sender or not receiver:raise RuntimeError('邮件地址未配置完整')
    msg=EmailMessage();msg['From']=sender;msg['To']=receiver
    triggered=[n for n,v in r.get('triggers',{}).items() if v is True]
    tag='异常' if r.get('status')!='ok' else '触发' if triggered else '日报'
    msg['Subject']=f'【标普500 v17·{tag}】{r.get("asof","运行失败")} '+('、'.join(triggered) if triggered else '')
    msg.set_content(render_text(r));msg.add_alternative(body,subtype='html')
    msg.add_attachment(Path(docx).read_bytes(),maintype='application',
        subtype='vnd.openxmlformats-officedocument.wordprocessingml.document',
        filename=f'SP500_v17_{r.get("asof","error")}.docx')
    return msg


def send(r,body,docx):
    sender=os.environ.get('EMAIL_ADDRESS','').strip();password=os.environ.get('EMAIL_PASSWORD','')
    if not sender or not password:raise RuntimeError('邮件环境变量未配置完整')
    msg=build_message(r,body,docx);receiver=str(msg['To'])
    with smtplib.SMTP_SSL('smtp.gmail.com',465,timeout=40) as server:
        server.login(sender,password);rejected=server.send_message(msg)
    if rejected:raise RuntimeError('SMTP rejected recipient')
    local,domain=receiver.rsplit('@',1)
    return {'smtp_accepted':True,'receiver_masked':local[:3]+'***@'+domain,'subject':msg['Subject']}
