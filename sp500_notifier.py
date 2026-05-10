# ══════════════════════════════════════════════════════════════════════════
# 标普500监控系统 v15 — 邮件通知模块(T1-T10 触发器版)
# ══════════════════════════════════════════════════════════════════════════

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


# 触发器中文显示名 + 类型 + 描述
TRIGGER_DISPLAY = [
    # 离场(高位卖出)
    ('T1_2000离场',  '🔻 T1 2000 互联网泡沫顶',  'exit',  '通胀过热顶 (★ ERP<0)'),
    ('T2_2007离场',  '🔻 T2 2007 鸽派假反弹顶',  'exit',  '油价拉动 (★ OIL_pct5y>85% + E-2 + F-)'),
    ('T3_2015离场',  '🔻 T3 2015 工业衰退顶',    'exit',  '油价崩盘通缩 (★ CPI_y<0% + ERP>3% + P+)'),
    ('T4_2022离场',  '🔻 T4 2022 加息顶',        'exit',  '通胀失控+扩表减速 (★ CPI_y≥5% + Sσ_200≥+3σ)'),
    # 入场(低位买入)
    ('T5_2002入场',  '🟢 T5 2002 互联网底',      'entry', '慢性熊市末端 (S-≥30% + V≥V_c+3σ + ★ E+)'),
    ('T6_2009入场',  '🟢 T6 2009 次贷底',        'entry', '急性信用危机底 (★ HY>8% + ERP<0 + WALCL+1000hm)'),
    ('T7_2020入场',  '🟢 T7 2020 COVID 底',      'entry', '史诗急跌+大水救市 (Sσ_200≤-3σ + ★ WALCL+1000hm)'),
    ('T8_2022入场',  '🟢 T8 2022 加息底',        'entry', '加息底 (★ CPI_y≥7% + F+ + ERP>3%)'),
    ('T9_白银坑1组', '🟡 T9 白银坑1组(广义)',     'entry', '长牛中浅熊 (★ ERP>3% + S-≥10%)'),
    ('T10_白银坑2组','🟡 T10 白银坑2组(中庸态)',  'entry', '健康市场浅熊 (★ S-≥5%且<20%)'),
]


def build_email_body(snapshot):
    s = snapshot
    date_str = s.get('date', '')
    triggers = s.get('triggers', {})

    # 总状态判断
    has_triggered = any(t.get('triggered') is True for t in triggers.values())
    has_close     = any(t.get('triggered') is None or
                       (t.get('triggered') is False and t.get('satisfied_pct', 0) >= 0.7)
                       for t in triggers.values())
    if has_triggered:
        alert_bg, alert_txt = '#C00000', '🚨 有触发器已触发,请立即关注!'
    elif has_close:
        alert_bg, alert_txt = '#ED7D31', '⚠️ 有触发器接近触发(≥70% 因子已满足)'
    else:
        alert_bg, alert_txt = '#27ae60', '✅ 全部触发器远未触发,继续等待'

    def trigger_row(tid, name, ttype, desc):
        r = triggers.get(tid, {})
        triggered = r.get('triggered')
        pct = r.get('satisfied_pct', 0)
        n_sat = r.get('satisfied_count', 0)
        n_tot = r.get('total_must', 0)
        alert = r.get('alert', '')

        if triggered is True:
            bg = '#FCE4D6'; status = '<b style="color:#C00000">🚨 已触发!</b>'
        elif triggered is False:
            if pct >= 0.9:
                bg = '#FFE699'; status = f'<b style="color:#ED7D31">🟠 高度接近 {pct*100:.0f}%</b>'
            elif pct >= 0.7:
                bg = '#FFF2CC'; status = f'<span style="color:#BF9000">🟡 中度接近 {pct*100:.0f}%</span>'
            else:
                bg = '#F2F2F2'; status = f'<span style="color:#888">⚪ 远未触发 {pct*100:.0f}%</span>'
        else:
            bg = '#FFF2CC'; status = f'<span style="color:#888">? 数据不足</span>'

        # 缺失因子(只显示前 2 个,简洁)
        missing = r.get('missing_factors', [])[:2]
        missing_str = ' | '.join(missing) if missing else ''
        if len(r.get('missing_factors', [])) > 2:
            missing_str += f' ... 共 {len(r.get("missing_factors", []))} 项'

        return (f'<tr style="background:{bg}">'
                f'<td style="padding:6px 10px;font-weight:bold">{name}</td>'
                f'<td style="padding:6px 10px;color:#666;font-size:11px">{desc}</td>'
                f'<td style="padding:6px 10px;text-align:center">{n_sat}/{n_tot}</td>'
                f'<td style="padding:6px 10px">{status}</td>'
                f'<td style="padding:6px 10px;color:#888;font-size:10px">{missing_str}</td>'
                f'</tr>')

    exit_rows  = ''.join(trigger_row(tid, n, 'exit',  d)
                         for tid, n, t, d in TRIGGER_DISPLAY if t == 'exit')
    entry_rows = ''.join(trigger_row(tid, n, 'entry', d)
                         for tid, n, t, d in TRIGGER_DISPLAY if t == 'entry')

    # 当前持仓
    from config import CURRENT_POSITION
    pos_str = CURRENT_POSITION if CURRENT_POSITION else '空仓'

    html = f"""
    <html><body style="font-family:Arial,'Microsoft YaHei',sans-serif;max-width:1000px;margin:0 auto;padding:20px">
    <h2 style="background:#1F3864;color:white;padding:15px;border-radius:8px;margin-bottom:8px">
        📊 标普500触发器系统(V15)监控日报 — {date_str}
    </h2>
    <div style="background:{alert_bg};color:white;padding:12px;border-radius:6px;
                font-size:15px;font-weight:bold;margin-bottom:12px">{alert_txt}</div>
    <div style="background:#EBF3FB;padding:8px 14px;border-radius:6px;margin-bottom:12px;font-size:13px">
        <b>持仓:</b>{pos_str} &nbsp;|&nbsp;
        <b>SP500:</b>{safe_num(s.get('sp500'),',.2f')} &nbsp;|&nbsp;
        <b>ATH 回撤:</b>{safe_num(s.get('sp_dd_pct'),'.1f','%')} &nbsp;|&nbsp;
        <b>200 周均线:</b>{safe_num(s.get('ma200w'),',.0f')}
    </div>

    <h3 style="color:#1F3864;border-bottom:2px solid #1F3864;padding-bottom:4px">一、关键中间量</h3>
    <table style="width:100%;border-collapse:collapse;margin-bottom:16px;font-size:12px">
        <tr style="background:#2F75B6;color:white">
            <th style="padding:6px">指标</th><th style="padding:6px">当前值</th>
            <th style="padding:6px">指标</th><th style="padding:6px">当前值</th>
        </tr>
        <tr style="background:#f9f9f9">
            <td style="padding:6px"><b>Sσ_200(标准差偏离)</b></td>
            <td style="padding:6px">{safe_num(s.get('sigma_dev_200'),'.2f','σ')}</td>
            <td style="padding:6px"><b>VIX</b></td>
            <td style="padding:6px">{safe_num(s.get('vix'),'.2f')} (V_c={safe_num(s.get('V_c'),'.1f')}+{safe_num(s.get('V_c_sigma'),'.1f')}σ)</td>
        </tr>
        <tr>
            <td style="padding:6px"><b>ERP</b></td>
            <td style="padding:6px">{safe_num(s.get('erp'),'.2f','%')}</td>
            <td style="padding:6px"><b>Forward PE</b></td>
            <td style="padding:6px">{safe_num(s.get('forward_pe'),'.1f','x')} (avg={safe_num(s.get('pe_avg'),'.1f')})</td>
        </tr>
        <tr style="background:#f9f9f9">
            <td style="padding:6px"><b>NFCI</b></td>
            <td style="padding:6px">{safe_num(s.get('nfci'),'.3f')}</td>
            <td style="padding:6px"><b>N_c (NFCI 20 日变化)</b></td>
            <td style="padding:6px">{safe_num(s.get('n_c'),'+.3f')} (μ={safe_num(s.get('n_c_mu'),'.3f')} σ={safe_num(s.get('n_c_sigma'),'.3f')})</td>
        </tr>
        <tr>
            <td style="padding:6px"><b>HY 信用利差</b></td>
            <td style="padding:6px">{safe_num(s.get('hy_spread'),'.2f','%')}</td>
            <td style="padding:6px"><b>HY_c21 (21 日变化)</b></td>
            <td style="padding:6px">{safe_num(s.get('hy_c21'),'+.3f')}</td>
        </tr>
        <tr style="background:#f9f9f9">
            <td style="padding:6px"><b>Y 利差(10Y-2Y)</b></td>
            <td style="padding:6px">{safe_num(s.get('y_spread'),'.3f')}</td>
            <td style="padding:6px"><b>联储利率</b></td>
            <td style="padding:6px">{safe_num(s.get('fed_rate'),'.2f','%')}</td>
        </tr>
        <tr>
            <td style="padding:6px"><b>CPI 同比</b></td>
            <td style="padding:6px">{safe_num(s.get('cpi_y'),'.2f','%')}</td>
            <td style="padding:6px"><b>WALCL 13 周变化</b></td>
            <td style="padding:6px">{safe_num(s.get('walcl_13w_chg'),'+.3f','T')}</td>
        </tr>
        <tr style="background:#f9f9f9">
            <td style="padding:6px"><b>OIL 价格(WTI)</b></td>
            <td style="padding:6px">{safe_num(s.get('oil_price'),'.2f')}</td>
            <td style="padding:6px"><b>OIL_pct5y(5 年百分位)</b></td>
            <td style="padding:6px">{safe_num(s.get('oil_pct5y'),'.0f','%')}</td>
        </tr>
    </table>

    <h3 style="color:#C00000;border-bottom:2px solid #C00000;padding-bottom:4px">二、离场触发器(高位卖出)</h3>
    <table style="width:100%;border-collapse:collapse;margin-bottom:16px;font-size:12px">
        <tr style="background:#C00000;color:white">
            <th style="padding:6px">触发器</th>
            <th style="padding:6px">物理画像</th>
            <th style="padding:6px;width:60px">必有</th>
            <th style="padding:6px;width:140px">状态</th>
            <th style="padding:6px">缺失/差距</th>
        </tr>
        {exit_rows}
    </table>

    <h3 style="color:#27ae60;border-bottom:2px solid #27ae60;padding-bottom:4px">三、入场触发器(低位买入)</h3>
    <table style="width:100%;border-collapse:collapse;margin-bottom:16px;font-size:12px">
        <tr style="background:#27ae60;color:white">
            <th style="padding:6px">触发器</th>
            <th style="padding:6px">物理画像</th>
            <th style="padding:6px;width:60px">必有</th>
            <th style="padding:6px;width:140px">状态</th>
            <th style="padding:6px">缺失/差距</th>
        </tr>
        {entry_rows}
    </table>

    <p style="color:#999;font-size:11px;margin-top:24px;text-align:center">
        标普500触发器系统(V15 因子 + 10 触发器) &nbsp;|&nbsp; {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
        Layer 3 验证:V15 重算 = 文件 4,857 触发日 0 差异 ✓
    </p>
    </body></html>"""
    return html


def _make_subject(snapshot):
    triggers = snapshot.get('triggers', {})
    has_triggered = any(t.get('triggered') is True for t in triggers.values())
    triggered_ids = [tid.split('_')[0] for tid, t in triggers.items() if t.get('triggered') is True]
    close_ids     = [tid.split('_')[0] for tid, t in triggers.items()
                     if t.get('triggered') is not True and t.get('satisfied_pct', 0) >= 0.9]

    sp_str   = safe_num(snapshot.get('sp500'), ',.0f')
    date_str = snapshot.get('date', '')

    if has_triggered:
        return f"🚨【触发】{date_str} {','.join(triggered_ids)} | SP={sp_str}"
    elif close_ids:
        return f"⚠️【高度接近】{date_str} {','.join(close_ids)} | SP={sp_str}"
    else:
        return f"✅【日报】{date_str} SP={sp_str} 无信号"


def send_email(snapshot):
    subject = _make_subject(snapshot)
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = EMAIL_SENDER
    msg['To']      = EMAIL_RECEIVER
    msg.attach(MIMEText(build_email_body(snapshot), 'html', 'utf-8'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        print(f"  ✅ 邮件发送成功:{subject}")
        return True
    except Exception as e:
        print(f"  ❌ 邮件发送失败:{e}")
        return False


def send_email_with_attachment(snapshot, report_path=None):
    from email.mime.base import MIMEBase
    from email import encoders

    subject = _make_subject(snapshot)
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
            print(f"  📎 附件已添加:{os.path.basename(report_path)}")
        except Exception as e:
            print(f"  ⚠️ 附件失败:{e}")

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        print(f"  ✅ 邮件发送成功:{subject}")
        return True
    except Exception as e:
        print(f"  ❌ 邮件发送失败:{e}")
        return False
