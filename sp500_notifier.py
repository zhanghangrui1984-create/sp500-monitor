# ══════════════════════════════════════════════
# 标普500监控系统 v10.3 — 邮件通知模块
# ══════════════════════════════════════════════

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

from config import EMAIL_SENDER, EMAIL_RECEIVER, EMAIL_PASSWORD

def safe_num(val, fmt, suffix='', default='N/A'):
    try:
        import math
        f = float(val)
        if math.isnan(f): return default
        return f"{format(f, fmt)}{suffix}"
    except: return default

def build_email_body(snapshot):
    s        = snapshot
    date_str = s.get('date', '')
    w200     = '✅ W+200 牛市' if s.get('W200') else '⚠️ W-200 熊市'
    e_plus   = s.get('E_plus')
    e_plus2  = s.get('E_plus2')
    e_minus  = s.get('E_minus')
    e_minus2 = s.get('E_minus2')

    entry_keys = ['SC1A','SC1D','SC2A','SC2D','SC3A','SC3B','SC4A','SC4B','SC4C']
    exit_keys  = ['EX3','EX1','EX2']
    has_entry  = any(s.get(k) is True for k in entry_keys)
    has_exit   = any(s.get(k) is True for k in exit_keys)
    alert_bg   = '#C00000' if (has_entry or has_exit) else '#27ae60'
    alert_txt  = '⚠️ 有信号触发！' if (has_entry or has_exit) else '✅ 无信号，继续等待'

    def sc_row(name, key, desc=''):
        val = s.get(key)
        if val is True:   bg='#FCE4D6'; mark='🚨'; txt='【触发！】'
        elif val is False: bg='#F2F2F2'; mark='✗';  txt='未触发'
        else:              bg='#FFF2CC'; mark='?';  txt='数据不足'
        return f'<tr style="background:{bg}"><td style="padding:4px 8px">{mark} {name}</td><td style="padding:4px 8px;color:#666;font-size:11px">{desc}</td><td style="padding:4px 8px;font-weight:bold">{txt}</td></tr>'

    entry_rows = (
        sc_row('情景1A', 'SC1A', 'W+200 | 10%≤dd<23.5% | E+2 | 1选1 → 半仓') +
        sc_row('情景1D', 'SC1D', 'W+200 | dd≥23.5% | E+2 | 1选1 → 全仓') +
        sc_row('情景2A', 'SC2A', 'W+200 | 10%≤dd<23.5% | E+ | 2选2 → 半仓') +
        sc_row('情景2D', 'SC2D', 'W+200 | dd≥23.5% | E+ | 2选2 → 全仓') +
        sc_row('情景3A', 'SC3A', 'W+200 | dd≥5% | ERP≥2.5% | 5选3(ERP≥3%) | NOT OIL_block → 全仓') +
        sc_row('情景3B', 'SC3B', 'W+200 | dd<5% | E+ | 4选2 | NOT OIL_block → 全仓') +
        sc_row('情景4A', 'SC4A', 'dd>35% | HY>10% | V_e>80% → 全仓（30日免疫）') +
        sc_row('情景4B', 'SC4B', 'dd>50% | QE → 全仓（30日免疫）') +
        sc_row('情景4C', 'SC4C', 'dd>30% | 8<HY<12 | QE → 全仓（30日免疫）')
    )
    exit_rows = (
        sc_row('离场3（最高优先级）', 'EX3', '(Y- or E-2) AND N≥0.2 AND N_c>0.1 AND ERP<4%') +
        sc_row('离场1', 'EX1', 'W+200 AND F+ AND ERP<0 AND 触发器') +
        sc_row('离场2', 'EX2', 'W+200 AND 7选4')
    )

    # TLT状态
    from config import TLT_HOLDING, CURRENT_POSITION
    tlt_status = ''
    if not CURRENT_POSITION:
        fm = s.get('F_minus_now')
        if TLT_HOLDING:
            tlt_status = f'<div style="background:#EBF3FB;padding:8px 12px;border-radius:4px;margin-bottom:12px;font-size:13px">📈 空仓期持有TLT | 当前价格: {safe_num(s.get("tlt_price"), ".2f")} | F-={fm}</div>'
        elif fm:
            tlt_status = f'<div style="background:#FFF9C4;padding:8px 12px;border-radius:4px;margin-bottom:12px;font-size:13px">💡 F-条件满足，空仓期可买入TLT | 当前价格: {safe_num(s.get("tlt_price"), ".2f")}</div>'

    html = f"""
    <html><body style="font-family:Arial,sans-serif;max-width:860px;margin:0 auto;padding:20px">
    <h2 style="background:#1F3864;color:white;padding:15px;border-radius:8px;margin-bottom:8px">
        📊 标普500全周期交易模型  监控日报 — {date_str}
    </h2>
    <div style="background:{alert_bg};color:white;padding:12px;border-radius:6px;
                font-size:15px;font-weight:bold;margin-bottom:12px">{alert_txt}</div>
    <div style="background:#EBF3FB;padding:8px 14px;border-radius:6px;margin-bottom:12px;font-size:13px">
        <b>均线环境：</b>{w200} &nbsp;|&nbsp;
        <b>持仓：</b>{'空仓' if not CURRENT_POSITION else CURRENT_POSITION}
    </div>
    {tlt_status}

    <h3 style="color:#1F3864;border-bottom:2px solid #1F3864;padding-bottom:4px">一、核心指标</h3>
    <table style="width:100%;border-collapse:collapse;margin-bottom:16px;font-size:12px">
        <tr style="background:#2F75B6;color:white">
            <th style="padding:6px">指标</th><th style="padding:6px">当前值</th>
            <th style="padding:6px">指标</th><th style="padding:6px">当前值</th>
        </tr>
        <tr style="background:#f9f9f9">
            <td style="padding:6px"><b>标普500</b></td><td style="padding:6px">{safe_num(s.get('sp500'),',.0f')}</td>
            <td style="padding:6px"><b>ATH回撤</b></td><td style="padding:6px">{safe_num(s.get('sp_dd_pct'),'.1f','%')}</td>
        </tr>
        <tr>
            <td style="padding:6px"><b>VIX</b></td><td style="padding:6px">{safe_num(s.get('vix'),'.2f')}</td>
            <td style="padding:6px"><b>V_e偏离度</b></td><td style="padding:6px">{safe_num(s.get('V_e'),'.1f','%')}</td>
        </tr>
        <tr style="background:#f9f9f9">
            <td style="padding:6px"><b>ERP</b></td><td style="padding:6px">{safe_num(s.get('erp'),'.2f','%')}</td>
            <td style="padding:6px"><b>Forward PE</b></td><td style="padding:6px">{safe_num(s.get('forward_pe'),'.1f','x')}</td>
        </tr>
        <tr>
            <td style="padding:6px"><b>E+/E+2</b></td><td style="padding:6px">{e_plus}/{e_plus2}</td>
            <td style="padding:6px"><b>E-/E-2</b></td><td style="padding:6px">{e_minus}/{e_minus2}</td>
        </tr>
        <tr style="background:#f9f9f9">
            <td style="padding:6px"><b>P+/P-</b></td>
            <td style="padding:6px">{'P+偏贵' if s.get('P_plus') else ('P-偏低' if s.get('P_minus') else '正常')}</td>
            <td style="padding:6px"><b>NFCI</b></td><td style="padding:6px">{safe_num(s.get('nfci'),'.3f')}</td>
        </tr>
        <tr>
            <td style="padding:6px"><b>NFCI月变化</b></td><td style="padding:6px">{safe_num(s.get('N_c_4w'),'+.3f')}</td>
            <td style="padding:6px"><b>HY信用利差</b></td><td style="padding:6px">{safe_num(s.get('hy_spread'),'.2f','%')}</td>
        </tr>
        <tr style="background:#f9f9f9">
            <td style="padding:6px"><b>Y利差(10Y-2Y)</b></td><td style="padding:6px">{safe_num(s.get('y_spread'),'.3f')}</td>
            <td style="padding:6px"><b>联储利率</b></td><td style="padding:6px">{safe_num(s.get('fed_rate'),'.2f','%')}</td>
        </tr>
        <tr>
            <td style="padding:6px"><b>CPI同比</b></td><td style="padding:6px">{safe_num(s.get('cpi_yoy'),'.1f','%')}</td>
            <td style="padding:6px"><b>MFG同比</b></td><td style="padding:6px">{safe_num(s.get('mfg_yoy'),'.1f','%')}</td>
        </tr>
        <tr style="background:#f9f9f9">
            <td style="padding:6px"><b>WALCL_1000亿(13周)</b></td><td style="padding:6px">{'✓' if s.get('W1000') else '✗'}</td>
            <td style="padding:6px"><b>TLT价格</b></td><td style="padding:6px">{safe_num(s.get('tlt_price'),'.2f')}</td>
        </tr>
        <tr>
            <td style="padding:6px"><b>OIL原油价格(WTI)</b></td><td style="padding:6px">{safe_num(s.get('oil_price'),'.1f')}</td>
            <td style="padding:6px"><b>OIL_block能源屏蔽</b></td>
            <td style="padding:6px">{'⚠️ 是' if s.get('OIL_block') else '否'} (20d:{safe_num(s.get('OIL_c20'),'.1f','%')} 5y:{safe_num(s.get('OIL_pct5y'),'.0f','%')})</td>
        </tr>
    </table>

    <h3 style="color:#1F3864;border-bottom:2px solid #1F3864;padding-bottom:4px">二、入场情景</h3>
    <table style="width:100%;border-collapse:collapse;margin-bottom:16px;font-size:12px">
        <tr style="background:#2F75B6;color:white">
            <th style="padding:6px">情景</th><th style="padding:6px">条件摘要</th><th style="padding:6px">状态</th>
        </tr>
        {entry_rows}
    </table>

    <h3 style="color:#1F3864;border-bottom:2px solid #1F3864;padding-bottom:4px">三、离场情景</h3>
    <table style="width:100%;border-collapse:collapse;margin-bottom:16px;font-size:12px">
        <tr style="background:#2F75B6;color:white">
            <th style="padding:6px">情景</th><th style="padding:6px">条件摘要</th><th style="padding:6px">状态</th>
        </tr>
        {exit_rows}
    </table>

    <p style="color:#999;font-size:11px;margin-top:24px;text-align:center">
        标普500全周期交易模型监控系统 v10.3 &nbsp;|&nbsp; {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    </p>
    </body></html>"""
    return html

def send_email(snapshot):
    from config import CURRENT_POSITION
    entry_keys = ['SC1A','SC1D','SC2A','SC2D','SC3A','SC3B','SC4A','SC4B','SC4C']
    exit_keys  = ['EX3','EX1','EX2']
    has_signal = (any(snapshot.get(k) is True for k in entry_keys) or
                  any(snapshot.get(k) is True for k in exit_keys))
    sp_str   = safe_num(snapshot.get('sp500'), ',.0f')
    date_str = snapshot.get('date', '')
    subject  = (f"🚨【SP500信号触发】{date_str} SP={sp_str}" if has_signal
                else f"✅【SP500日报】{date_str} SP={sp_str} 无信号")

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = EMAIL_SENDER
    msg['To']      = EMAIL_RECEIVER
    msg.attach(MIMEText(build_email_body(snapshot), 'html', 'utf-8'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        print(f"  ✅ 邮件发送成功：{subject}")
        return True
    except Exception as e:
        print(f"  ❌ 邮件发送失败：{e}")
        return False

def send_email_with_attachment(snapshot, report_path=None):
    from email.mime.base import MIMEBase
    from email import encoders
    from config import CURRENT_POSITION

    entry_keys = ['SC1A','SC1D','SC2A','SC2D','SC3A','SC3B','SC4A','SC4B','SC4C']
    exit_keys  = ['EX3','EX1','EX2']
    has_signal = (any(snapshot.get(k) is True for k in entry_keys) or
                  any(snapshot.get(k) is True for k in exit_keys))
    sp_str   = safe_num(snapshot.get('sp500'), ',.0f')
    date_str = snapshot.get('date', '')
    subject  = (f"🚨【SP500信号触发】{date_str} SP={sp_str}" if has_signal
                else f"✅【SP500日报】{date_str} SP={sp_str} 无信号")

    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From']    = EMAIL_SENDER
    msg['To']      = EMAIL_RECEIVER

    html_part = MIMEMultipart('alternative')
    html_part.attach(MIMEText(build_email_body(snapshot), 'html', 'utf-8'))
    msg.attach(html_part)

    if report_path and os.path.exists(report_path):
        try:
            with open(report_path, 'rb') as f:
                att = MIMEBase('application',
                    'vnd.openxmlformats-officedocument.wordprocessingml.document')
                att.set_payload(f.read())
                encoders.encode_base64(att)
                att.add_header('Content-Disposition', 'attachment',
                               filename=os.path.basename(report_path))
                msg.attach(att)
            print(f"  📎 附件已添加：{os.path.basename(report_path)}")
        except Exception as e:
            print(f"  ⚠️ 附件失败：{e}")

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        print(f"  ✅ 邮件发送成功：{subject}")
        return True
    except Exception as e:
        print(f"  ❌ 邮件发送失败：{e}")
        return False
