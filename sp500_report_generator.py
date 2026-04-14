# ══════════════════════════════════════════════
# 标普500监控系统 v10.3 — 报告生成模块
# ══════════════════════════════════════════════

import os
from datetime import datetime
from docx import Document
from docx.shared import Pt, RGBColor, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
from docx.oxml.ns import qn
from docx.oxml import OxmlElement

C_DARK   = RGBColor(0x1F, 0x38, 0x64)
C_MID    = RGBColor(0x2F, 0x75, 0xB6)
C_WHITE  = RGBColor(0xFF, 0xFF, 0xFF)
C_GREEN  = RGBColor(0x37, 0x56, 0x23)
C_RED    = RGBColor(0xC0, 0x00, 0x00)
C_GRAY   = RGBColor(0x59, 0x59, 0x59)
C_ORANGE = RGBColor(0xED, 0x7D, 0x31)

def set_cell_bg(cell, hex_color):
    tc = cell._tc; tcPr = tc.get_or_add_tcPr()
    shd = OxmlElement('w:shd')
    shd.set(qn('w:val'), 'clear'); shd.set(qn('w:color'), 'auto')
    shd.set(qn('w:fill'), hex_color); tcPr.append(shd)

def set_cell_text(cell, text, bold=False, color=None, size=10, align='center'):
    cell.text = ''
    para = cell.paragraphs[0]
    para.alignment = WD_ALIGN_PARAGRAPH.CENTER if align == 'center' else WD_ALIGN_PARAGRAPH.LEFT
    run = para.add_run(str(text))
    run.bold = bold; run.font.size = Pt(size); run.font.name = 'Arial'
    if color: run.font.color.rgb = color
    cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER

def add_heading(doc, text, level=2):
    para = doc.add_paragraph()
    run = para.add_run(text); run.bold = True; run.font.name = 'Arial'
    if level == 1:
        run.font.size = Pt(14); run.font.color.rgb = C_DARK
    else:
        run.font.size = Pt(11); run.font.color.rgb = C_MID
    para.paragraph_format.space_before = Pt(12)
    para.paragraph_format.space_after  = Pt(4)
    if level == 2:
        pPr = para._p.get_or_add_pPr(); pBdr = OxmlElement('w:pBdr')
        bot = OxmlElement('w:bottom')
        bot.set(qn('w:val'), 'single'); bot.set(qn('w:sz'), '6')
        bot.set(qn('w:space'), '1'); bot.set(qn('w:color'), '2F75B6')
        pBdr.append(bot); pPr.append(pBdr)

def add_kv_table(doc, rows, cols=4):
    table = doc.add_table(rows=len(rows), cols=cols)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.style = 'Table Grid'
    for i, row_data in enumerate(rows):
        row = table.rows[i]
        bg = 'F2F2F2' if i % 2 == 0 else 'FFFFFF'
        for j, (label, value) in enumerate(row_data):
            if j * 2 + 1 >= cols: break
            lc = row.cells[j*2]; vc = row.cells[j*2+1]
            set_cell_bg(lc, bg); set_cell_bg(vc, bg)
            set_cell_text(lc, label, bold=True, color=C_DARK, align='left', size=9)
            set_cell_text(vc, value, color=C_DARK, size=9)
    for row in table.rows:
        row.height = Cm(0.65)
        for j, cell in enumerate(row.cells):
            cell.width = Cm(3.5 if j % 2 == 0 else 2.5)
    return table

def fmt_val(val, fmt_str='', suffix=''):
    try:
        import math
        f = float(val)
        if math.isnan(f): return 'N/A'
        return f"{format(f, fmt_str)}{suffix}"
    except: return 'N/A'

def build_conditions(snapshot):
    s = snapshot

    def yn(val, positive=True, none_as=None):
        """none_as: 指定None时的返回值，默认'?'"""
        if val is None: return none_as if none_as else '?'
        return '✓' if (bool(val) == positive) else '✗'

    sp_dd  = fmt_val(s.get('sp_dd_pct'), '.1f', '%')
    w200   = '✓ W+200' if s.get('W200') else '✗ W-200'
    erp    = fmt_val(s.get('erp'), '.2f', '%')
    nfci   = fmt_val(s.get('nfci'), '.3f')
    n_c4w  = fmt_val(s.get('N_c_4w'), '.3f')
    hy     = fmt_val(s.get('hy_spread'), '.2f', '%')
    hy_c20 = fmt_val(s.get('hy_c20'), '+.3f')
    y_sp   = fmt_val(s.get('y_spread'), '.3f')
    vix    = fmt_val(s.get('vix'), '.2f')
    v_e    = fmt_val(s.get('V_e'), '.1f', '%')
    ve_m20 = fmt_val(s.get('V_em20'), '.1f', '%')
    cpi    = fmt_val(s.get('cpi_yoy'), '.1f', '%')
    mfg    = fmt_val(s.get('mfg_yoy'), '.1f', '%')
    fed    = fmt_val(s.get('fed_rate'), '.2f', '%')
    fpe    = fmt_val(s.get('forward_pe'), '.1f', 'x')
    rr     = fmt_val(s.get('real_rate'), '.2f', '%')
    e_plus = s.get('E_plus')
    e_plus2= s.get('E_plus2')
    e_minus= s.get('E_minus')
    e_minus2=s.get('E_minus2')
    sp_pp  = fmt_val(s.get('sp_pp'), '.1f', '%')

    # NDX_t⬆ 类似，这里是 SP_t⬆
    sp_t_up = s.get('sp_t_up')
    st_str  = '✓ 当前价>入场价' if sp_t_up else ('✗ 当前价≤入场价' if sp_t_up is False else '—空仓')

    conds = {}

    # 公共催化剂说明
    def _trigger_v(n):
        """n=1：1选1；n=2：2选2"""
        cnt = sum([bool(s.get('Y_plus') or False),
                   bool(s.get('N_front') or False),
                   bool(s.get('P_minus') or False),
                   bool(s.get('V_new') or False)])
        return '✓' if cnt >= n else '✗'

    # ── 情景1A
    conds['情景1A'] = [
        ('W+200（牛市均线上方）',      w200,   yn(s.get('W200'))),
        ('10%≤S回撤<23.5%',           sp_dd,  yn(10 <= (s.get('sp_dd_pct') or 0) < 23.5)),
        ('ERP≥1.5%',                  erp,    yn(s.get('erp') and s['erp'] >= 1.5)),
        ('E+2（EPS连续两期增长）',     str(e_plus2), yn(e_plus2)),
        ('NOT N+≥0.3（无过度宽松）',  n_c4w,  yn(not (s.get('N_c_4w') and s['N_c_4w'] <= -0.3))),
        ('NOT P+（估值不过热）',       fpe,    yn(not s.get('P_plus', False))),
        ('触发器（1选1）：Y+/N_front/P-/V_new', '',
         _trigger_v(1)),
        ('  Y+（收益率曲线不倒挂）',   y_sp,   yn(s.get('Y_plus', False))),
        ('  N_front（流动性改善）',    n_c4w,  yn(s.get('N_front', False))),
        ('  P-（估值偏低）',           fpe,    yn(s.get('P_minus'), none_as='✗')),
        ('  V_new（恐慌后企稳）',      ve_m20, yn(s.get('V_new', False))),
    ]

    # ── 情景1D
    conds['情景1D'] = [
        ('W+200',                      w200,   yn(s.get('W200'))),
        ('S回撤≥23.5%',                sp_dd,  yn((s.get('sp_dd_pct') or 0) >= 23.5)),
        ('ERP≥1.5%',                   erp,    yn(s.get('erp') and s['erp'] >= 1.5)),
        ('E+2',                        str(e_plus2), yn(e_plus2)),
        ('NOT N+≥0.3',                 n_c4w,  yn(not (s.get('N_c_4w') and s['N_c_4w'] <= -0.3))),
        ('NOT P+',                     fpe,    yn(not bool(s.get('P_plus') or False))),
        ('触发器（1选1）：Y+/N_front/P-/V_new', '',
         _trigger_v(1)),
    ]

    # ── 情景2A
    conds['情景2A'] = [
        ('W+200',                      w200,   yn(s.get('W200'))),
        ('10%≤S回撤<23.5%',            sp_dd,  yn(10 <= (s.get('sp_dd_pct') or 0) < 23.5)),
        ('ERP≥1.5%',                   erp,    yn(s.get('erp') and s['erp'] >= 1.5)),
        ('E+（EPS单期增长，弱版）',    str(e_plus), yn(e_plus)),
        ('NOT N+≥0.3',                 n_c4w,  yn(not (s.get('N_c_4w') and s['N_c_4w'] <= -0.3))),
        ('NOT P+',                     fpe,    yn(not bool(s.get('P_plus') or False))),
        ('触发器（4选2）：Y+/N_front/P-/V_new', '',
         _trigger_v(2)),
        ('  Y+',                       y_sp,   yn(s.get('Y_plus', False))),
        ('  N_front',                  n_c4w,  yn(s.get('N_front', False))),
        ('  P-',                       fpe,    yn(s.get('P_minus'), none_as='✗')),
        ('  V_new',                    ve_m20, yn(s.get('V_new', False))),
    ]

    # ── 情景2D
    conds['情景2D'] = [
        ('W+200',                      w200,   yn(s.get('W200'))),
        ('S回撤≥23.5%',                sp_dd,  yn((s.get('sp_dd_pct') or 0) >= 23.5)),
        ('ERP≥1.5%',                   erp,    yn(s.get('erp') and s['erp'] >= 1.5)),
        ('E+',                         str(e_plus), yn(e_plus)),
        ('NOT N+≥0.3',                 n_c4w,  yn(not (s.get('N_c_4w') and s['N_c_4w'] <= -0.3))),
        ('NOT P+',                     fpe,    yn(not bool(s.get('P_plus') or False))),
        ('触发器（4选2）：Y+/N_front/P-/V_new', '',
         _trigger_v(2)),
    ]

    # ── 情景3A
    score3a_items = [
        bool(e_plus or e_plus2) if e_plus is not None else False,
        bool(s.get('F0') or s.get('F_minus')),
        bool(s.get('erp') and s['erp'] >= 3.0),   # v10.3: ERP≥3%
        bool(s.get('S_p1', False)),
        bool(s.get('V_calm') or s.get('Y_plus')),
    ]
    score3a = sum(score3a_items)
    oil_str = f"{fmt_val(s.get('OIL_c20'),'.1f','%')} / {fmt_val(s.get('OIL_pct5y'),'.1f','%')}"
    conds['情景3A'] = [
        ('W+200',                              w200,  yn(s.get('W200'))),
        ('S回撤≥5%',                           sp_dd, yn((s.get('sp_dd_pct') or 0) >= 5)),
        ('ERP≥2.5%（v10.3升级）',              erp,   yn(s.get('erp') and s['erp'] >= 2.5)),
        ('N_front（流动性改善）',               n_c4w, yn(s.get('N_front', False))),
        ('NOT OIL_block（无能源冲击）',        oil_str, yn(not s.get('OIL_block', False))),
        (f'计分≥3（当前{score3a}/5）',          '',    yn(score3a >= 3)),
        ('  E+或E+2',                          str(e_plus or e_plus2), yn(score3a_items[0])),
        ('  F0或F-（联储中性/宽松）',           fed,   yn(score3a_items[1])),
        ('  ERP≥3%（v10.3升级）',              erp,   yn(score3a_items[2])),
        ('  S+1（月度上涨）',                   '',    yn(score3a_items[3])),
        ('  V≤V_c或Y+',                        vix,   yn(score3a_items[4])),
    ]

    # ── 情景3B
    score3b_items = [
        bool(s.get('F0') or s.get('F_minus')),
        bool(s.get('erp') and s['erp'] >= 2.5),
        bool(s.get('S_p1', False)),
        bool(s.get('V_calm') or s.get('Y_plus')),
    ]
    score3b = sum(score3b_items)
    conds['情景3B'] = [
        ('W+200',                              w200,  yn(s.get('W200'))),
        ('S回撤<5%',                           sp_dd, yn((s.get('sp_dd_pct') or 0) < 5)),
        ('ERP≥1.5%',                           erp,   yn(s.get('erp') and s['erp'] >= 1.5)),
        ('N_front',                            n_c4w, yn(s.get('N_front', False))),
        ('E+或E+2',                            str(e_plus or e_plus2), yn(bool(e_plus or e_plus2) if e_plus is not None else False)),
        ('NOT OIL_block（无能源冲击，v10.3）', oil_str, yn(not s.get('OIL_block', False))),
        (f'计分≥2（当前{score3b}/4）',          '',    yn(score3b >= 2)),
        ('  F0或F-',                            fed,   yn(score3b_items[0])),
        ('  ERP≥2.5%',                         erp,   yn(score3b_items[1])),
        ('  S+1',                               '',    yn(score3b_items[2])),
        ('  V≤V_c或Y+',                        vix,   yn(score3b_items[3])),
    ]

    # ── 情景4A
    sc4a_n = bool(s.get('N_c_neg') or (s.get('nfci') is not None and s['nfci'] < 0))
    conds['情景4A'] = [
        ('S回撤>35%',                      sp_dd, yn((s.get('sp_dd_pct') or 0) > 35)),
        ('HY信用利差>10%（系统性危机）',    hy,    yn(s.get('hy_spread') and s['hy_spread'] > 10.0)),
        ('V_e>80%（极度恐慌）',            v_e,   yn(s.get('V_e') and s['V_e'] > 80.0)),
        ('N_c<0 OR N<0（流动性转暖）',     nfci,  yn(sc4a_n)),
    ]

    # ── 情景4B
    conds['情景4B'] = [
        ('S回撤>50%（历史级别崩盘）',      sp_dd, yn((s.get('sp_dd_pct') or 0) > 50)),
        ('WALCL+≥1000亿/13周（QE触发）',  '',    yn(s.get('W1000', False))),
    ]

    # ── 情景4C
    conds['情景4C'] = [
        ('S回撤>30%',                      sp_dd, yn((s.get('sp_dd_pct') or 0) > 30)),
        ('8%<HY<12%（危机但未极端）',      hy,    yn(s.get('hy_spread') and 8.0 < s['hy_spread'] < 12.0)),
        ('V_e>80%',                        v_e,   yn(s.get('V_e') and s['V_e'] > 80.0)),
        ('N<1（流动性尚可）',              nfci,  yn(s.get('nfci') is not None and s['nfci'] < 1.0)),
        ('WALCL+≥1000亿/13周（QE）',      '',    yn(s.get('W1000', False))),
    ]

    # ── 离场情景
    e_minus_any = bool(e_minus or e_minus2) if e_minus is not None else False

    # 离场3（最高优先级）
    conds['离场3'] = [
        ('Y-或E-2（基本面恶化）',          '',    yn(s.get('Y_minus') or bool(e_minus2) if e_minus2 is not None else False)),
        ('N≥0.2（流动性显著收紧）',        nfci,  yn(s.get('nfci') is not None and s['nfci'] >= 0.2)),
        ('N_c>0.1（月度快速恶化）',        n_c4w, yn(s.get('N_c_gt01', False))),
        ('ERP<4.0%',                       erp,   yn(s.get('erp') is not None and s['erp'] < 4.0)),
        # S_t⬆ 由操作人自行判断，系统不检测
    ]

    # 离场1
    cnt1 = sum([bool(s.get('N_minus_015') or False), bool(s.get('P_plus') or False)])
    conds['离场1'] = [
        ('W+200',                          w200,   yn(s.get('W200'))),
        ('Y-或E-/E-2（基本面恶化）',        '',     yn(s.get('Y_minus') or e_minus_any)),
        ('F+（联储紧缩）',                  fed,    yn(s.get('F_plus', False))),
        ('ERP<0（股票性价比为负）',         erp,    yn(s.get('erp') is not None and s['erp'] < 0)),
        (f'触发器≥1（当前{cnt1}/2）：N-≥0.15 or P+', '',
         yn(cnt1 >= 1)),
        ('  N-≥0.15（流动性月度收紧）',    n_c4w,  yn(s.get('N_minus_015', False))),
        ('  P+（估值偏高）',               fpe,    yn(s.get('P_plus', False))),
        # S_t⬆ 由操作人自行判断
    ]

    # 离场2
    e_minus2_ok = bool(e_minus2) if e_minus2 is not None else False
    cnt2 = sum([
        bool(s.get('Y_minus') or False),
        bool(s.get('N_minus_015') or False),
        bool(s.get('F_plus') or False),
        bool(e_minus2_ok),
        bool(s.get('P_plus') or False),
        bool(s.get('erp') is not None and s['erp'] < 1.0),
        bool(s.get('MFG_lt3', False)),
    ])
    vp_hit = bool(s.get('vix') and s.get('V_p') and s['vix'] > s['V_p'])
    conds['离场2'] = [
        ('W+200',                          w200,   yn(s.get('W200'))),
        ('S-1（月度下跌趋势）',             '',     yn(s.get('S_m1', False))),
        ('S_dd+（距18月高点<15%）',         '',     yn(s.get('S_ddp', False))),
        ('N_c≥0.1（流动性快速恶化）',      n_c4w,  yn(s.get('N_c_ge01', False))),
        ('V>V_p（恐慌超基线）',             vix,    yn(vp_hit)),
        (f'触发器≥4（当前{cnt2}/7）',       '',     yn(cnt2 >= 4)),
        ('  Y-',                            y_sp,   yn(s.get('Y_minus', False))),
        ('  N-≥0.15',                       n_c4w,  yn(s.get('N_minus_015', False))),
        ('  F+',                            fed,    yn(s.get('F_plus', False))),
        ('  E-2',                           str(e_minus2), yn(e_minus2_ok)),
        ('  P+',                            fpe,    yn(s.get('P_plus', False))),
        ('  ERP<1.0%',                      erp,    yn(s.get('erp') is not None and s['erp'] < 1.0)),
        ('  MFG<-3%（制造业收缩）',        mfg,    yn(s.get('MFG_lt3', False))),
        # S_t⬆ 由操作人自行判断
    ]

    return conds

def generate_report(snapshot):
    from config import CURRENT_POSITION

    date_str = snapshot.get('date', datetime.today().strftime('%Y-%m-%d'))
    year = date_str[:4]; month = date_str[5:7]

    import platform
    if platform.system() == 'Windows':
        report_dir = f"D:\\sp500_monitor\\reports\\{year}\\{month}"
    else:
        report_dir = f"reports/{year}/{month}"
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"SP500监控日报_{date_str}.docx")

    doc = Document()
    for section in doc.sections:
        section.top_margin = Cm(1.5); section.bottom_margin = Cm(1.5)
        section.left_margin = Cm(2.0); section.right_margin = Cm(2.0)

    # 标题
    title = doc.add_paragraph()
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = title.add_run('标普500全周期交易模型  监控日报')
    run.bold = True; run.font.size = Pt(16); run.font.name = 'Arial'
    run.font.color.rgb = C_DARK

    sub = doc.add_paragraph()
    sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run2 = sub.add_run(f'{date_str}  |  v10.3  |  自动生成')
    run2.font.size = Pt(9); run2.font.name = 'Arial'; run2.font.color.rgb = C_GRAY

    env_para = doc.add_paragraph()
    env_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    w200_text = 'W+200 牛市' if snapshot.get('W200') else 'W-200 熊市'
    pos_text  = f'持仓：{CURRENT_POSITION}' if CURRENT_POSITION else '当前：空仓'
    run3 = env_para.add_run(f'均线环境：{w200_text}  |  {pos_text}')
    run3.bold = True; run3.font.size = Pt(10); run3.font.name = 'Arial'
    run3.font.color.rgb = C_DARK
    doc.add_paragraph()

    # 一、核心指标快照
    add_heading(doc, '一、核心指标快照')
    e_plus  = snapshot.get('E_plus');  e_plus2  = snapshot.get('E_plus2')
    e_minus = snapshot.get('E_minus'); e_minus2 = snapshot.get('E_minus2')
    from config import TLT_HOLDING
    tlt_str = f"{'持有中' if TLT_HOLDING else '未持有'} | 价格{fmt_val(snapshot.get('tlt_price'),'.2f')}"

    indicator_rows = [
        [('标普500点位',    fmt_val(snapshot.get('sp500'), ',.0f')),
         ('ATH回撤',        fmt_val(snapshot.get('sp_dd_pct'), '.1f', '%'))],
        [('VIX恐慌指数',    fmt_val(snapshot.get('vix'), '.2f')),
         ('V_e偏离度',      fmt_val(snapshot.get('V_e'), '.1f', '%'))],
        [('ERP股权溢价',    fmt_val(snapshot.get('erp'), '.2f', '%')),
         ('Forward PE',     fmt_val(snapshot.get('forward_pe'), '.1f', 'x'))],
        [('E+/E+2',         f"{e_plus}/{e_plus2}"),
         ('E-/E-2',         f"{e_minus}/{e_minus2}")],
        [('P+/P-（估值）',  ('P+偏贵' if snapshot.get('P_plus') else ('P-偏低' if snapshot.get('P_minus') else '正常'))),
         ('S+从底部反弹',   fmt_val(snapshot.get('sp_pp'), '.1f', '%'))],
        [('NFCI',           fmt_val(snapshot.get('nfci'), '.3f')),
         ('NFCI月变化',      fmt_val(snapshot.get('N_c_4w'), '+.3f'))],
        [('HY信用利差',     fmt_val(snapshot.get('hy_spread'), '.2f', '%')),
         ('HY_c20变化',     fmt_val(snapshot.get('hy_c20'), '+.3f'))],
        [('Y利差（10Y-2Y）', fmt_val(snapshot.get('y_spread'), '.3f')),
         ('联储利率',        fmt_val(snapshot.get('fed_rate'), '.2f', '%'))],
        [('实际利率',        fmt_val(snapshot.get('real_rate'), '.2f', '%')),
         ('CPI同比',        fmt_val(snapshot.get('cpi_yoy'), '.1f', '%'))],
        [('MFG制造业同比',  fmt_val(snapshot.get('mfg_yoy'), '.1f', '%')),
         ('WALCL_1000亿/13周', '✓' if snapshot.get('W1000') else '✗')],
        [('TLT（空仓期）',  tlt_str),
         ('V_new（恐慌企稳）', '✓' if snapshot.get('V_new') else '✗')],
        [('OIL原油价格',    fmt_val(snapshot.get('oil_price'), '.1f')),
         ('OIL_block屏蔽',  '⚠️ 是' if snapshot.get('OIL_block') else '否')],
        [('OIL_c20(20日涨幅)', fmt_val(snapshot.get('OIL_c20'), '.1f', '%')),
         ('OIL_pct5y(5年百分位)', fmt_val(snapshot.get('OIL_pct5y'), '.1f', '%'))],
    ]
    add_kv_table(doc, indicator_rows, cols=4)
    doc.add_paragraph()

    # 信号总状态
    entry_keys = ['SC1A','SC1D','SC2A','SC2D','SC3A','SC3B','SC4A','SC4B','SC4C']
    exit_keys  = ['EX1','EX2','EX3']
    has_entry  = any(snapshot.get(k) is True for k in entry_keys)
    has_exit   = any(snapshot.get(k) is True for k in exit_keys) and bool(CURRENT_POSITION)

    sig_para = doc.add_paragraph()
    sig_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    if has_entry or has_exit:
        rs = sig_para.add_run('⚠  有信号触发，请立即关注！')
        rs.bold = True; rs.font.size = Pt(13); rs.font.color.rgb = C_RED
    else:
        rs = sig_para.add_run('✓  无信号触发，继续等待')
        rs.bold = True; rs.font.size = Pt(13); rs.font.color.rgb = C_GREEN
    doc.add_paragraph()

    conds = build_conditions(snapshot)

    sc_result_map = {
        'SC1A':'情景1A','SC1D':'情景1D','SC2A':'情景2A','SC2D':'情景2D',
        'SC3A':'情景3A','SC3B':'情景3B',
        'SC4A':'情景4A','SC4B':'情景4B','SC4C':'情景4C',
    }
    sc_key_map = {v: k for k, v in sc_result_map.items()}

    # 二、入场情景
    add_heading(doc, '二、入场情景详细检测')
    for sc_name, cond_list in conds.items():
        if sc_name.startswith('离场'): continue
        result = snapshot.get(sc_key_map.get(sc_name))

        sc_para = doc.add_paragraph()
        sc_para.paragraph_format.space_before = Pt(8)
        sc_para.paragraph_format.space_after  = Pt(2)
        if result is True:
            label = '🚨 【触发！】'; color = C_RED
        elif result is False:
            label = '✗ 未触发'; color = C_GRAY
        else:
            label = '? 数据不足'; color = C_ORANGE
        rsc = sc_para.add_run(f'{sc_name}  {label}')
        rsc.bold = True; rsc.font.size = Pt(10)
        rsc.font.name = 'Arial'; rsc.font.color.rgb = color

        table = doc.add_table(rows=len(cond_list), cols=3)
        table.style = 'Table Grid'
        for i, (cond_name, cond_val, satisfied) in enumerate(cond_list):
            row = table.rows[i]
            bg = 'E2EFDA' if satisfied == '✓' else ('FCE4D6' if satisfied == '✗' else 'FFF2CC')
            for cell in row.cells: set_cell_bg(cell, bg)
            clr = C_GREEN if satisfied == '✓' else (C_RED if satisfied == '✗' else C_ORANGE)
            set_cell_text(row.cells[0], cond_name, align='left', size=9)
            set_cell_text(row.cells[1], cond_val, size=9)
            set_cell_text(row.cells[2], satisfied, bold=True, color=clr, size=10)
            row.height = Cm(0.6)
            row.cells[0].width = Cm(7.5)
            row.cells[1].width = Cm(3.0)
            row.cells[2].width = Cm(1.5)
    doc.add_paragraph()

    # 三、离场情景（离场3最高优先级排最前）
    add_heading(doc, '三、离场情景详细检测')
    ex_key_map = {'离场3':'EX3','离场1':'EX1','离场2':'EX2'}
    for ex_name in ['离场3', '离场1', '离场2']:
        cond_list = conds.get(ex_name, [])
        result    = snapshot.get(ex_key_map.get(ex_name))

        ex_para = doc.add_paragraph()
        ex_para.paragraph_format.space_before = Pt(8)
        ex_para.paragraph_format.space_after  = Pt(2)
        label_sfx = '（最高优先级）' if ex_name == '离场3' else ''
        if result is True:
            label = '🚨 【触发！】'; color = C_RED
        elif result is False:
            label = '✗ 未触发'; color = C_GRAY
        else:
            label = '? 数据不足'; color = C_ORANGE
        rex = ex_para.add_run(f'{ex_name}{label_sfx}  {label}')
        rex.bold = True; rex.font.size = Pt(10)
        rex.font.name = 'Arial'; rex.font.color.rgb = color

        if cond_list:
            table = doc.add_table(rows=len(cond_list), cols=3)
            table.style = 'Table Grid'
            for i, (cond_name, cond_val, satisfied) in enumerate(cond_list):
                row = table.rows[i]
                bg = 'E2EFDA' if satisfied == '✓' else ('FCE4D6' if satisfied == '✗' else 'FFF2CC')
                for cell in row.cells: set_cell_bg(cell, bg)
                clr = C_GREEN if satisfied == '✓' else (C_RED if satisfied == '✗' else C_ORANGE)
                set_cell_text(row.cells[0], cond_name, align='left', size=9)
                set_cell_text(row.cells[1], cond_val, size=9)
                set_cell_text(row.cells[2], satisfied, bold=True, color=clr, size=10)
                row.height = Cm(0.6)
                row.cells[0].width = Cm(7.5)
                row.cells[1].width = Cm(3.0)
                row.cells[2].width = Cm(1.5)

    doc.add_paragraph()
    footer = doc.add_paragraph(
        f'标普500全周期交易模型监控系统 v10.3  |  自动生成于 {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
    footer.runs[0].font.size = Pt(8)
    footer.runs[0].font.color.rgb = C_GRAY

    doc.save(report_path)
    print(f"  ✅ 报告已保存：{report_path}")
    return report_path
