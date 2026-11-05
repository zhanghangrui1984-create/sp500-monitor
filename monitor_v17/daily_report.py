"""Readable daily conditions and a separate, reproducible data archive."""
import csv
import hashlib
import html
import io
import json
import zipfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

BASE=Path(__file__).resolve().parents[1]
CONFIG=json.loads((BASE/'monitor_v17/model.json').read_text(encoding='utf-8'))
FORMAT_VERSION='concise-docx-v1-20261103'
COLORS={True:'176346',False:'A33232',None:'916000'}
LABELS={
    'S-6':'中期下跌（S−6）',
    'S- ≥ P99':'深度回撤（回撤 ≥ P99）',
    'P95 ≤ S- < P99':'中等回撤（P95 ≤ 回撤 < P99）',
    'V ≥ V_c+3σ':'恐慌升高（VIX ≥ 平静期均值＋3σ）',
    'Sσ_200 ≤ -2σ':'价格明显低于200日均线（偏离 ≤ −2σ）',
    'NFR ≥ 0.958':'金融条件修复（NFR ≥ 0.958）',
    'ERX达标':'经济活动修复（ERX达标）',
    'CPI_y ≥ 5.39%':'高通胀（CPI同比 ≥ 5.39%）',
    'CPI_y < 0%':'物价下降（CPI同比 < 0%）',
    'E-2':'预测盈利连续下调（E−2）',
    'N < 0':'金融条件宽松（NFCI < 0）',
    'HPG > 0':'住宅许可增长（HPG > 0）',
    'NFCI_26w_up':'半年金融收紧（NFCI高于26周前）',
    'NFCI_4w_down':'四周金融改善（NFCI低于4周前）',
    'DEFENSE':'防御板块占优（DEFENSE）',
    'P+':'预期估值偏高（P＋）',
    'Y-':'收益率曲线倒挂（十年减两年利差 < 0）',
    'NSX ≥ 40%':'纳指相对标普超涨（NSX ≥ 40%）',
    'FSX ≤ -10%':'金融板块相对弱势（FSX ≤ −10%）',
    'FCS ≥ 1.00pp':'融资利差偏高（FCS ≥ 1.00个百分点）',
}


def status(value,group=False):
    if value is not None and type(value) is not bool:raise ValueError('Invalid condition state')
    return '待确认' if value is None else ('满足' if value else '未满足') if group else ('达标' if value else '未达标')


def condition_tree(node,factors):
    """Use engine flags; never compare rounded inputs or substitute false for null."""
    if isinstance(node,str):
        value=factors.get(node,{}).get('value')
        return dict(kind='factor',key=node,label=LABELS[node],value=value,status=status(value))
    op=next(iter(node))
    if len(node)!=1 or op not in ('all','any','not'):raise ValueError('Invalid report expression')
    if op=='not':
        child=condition_tree(node[op],factors)
        value=None if child['value'] is None else not child['value']
        label='非高通胀（CPI同比 < 5.39%）' if node[op]=='CPI_y ≥ 5.39%' else '不满足：'+child['label']
        return dict(kind='factor',key='NOT '+child['key'],label=label,value=value,status=status(value))
    children=[condition_tree(x,factors) for x in node[op]]
    if not children:raise ValueError('Empty report expression')
    values=[c['value'] for c in children]
    decisive=False if op=='all' else True
    value=decisive if decisive in values else None if None in values else not decisive
    label='以下条件需全部达标' if op=='all' else '以下条件至少一项满足'
    return dict(kind=op,label=label,value=value,status=status(value,True),children=children)


def report_groups(r):
    groups=[]
    failed=r.get('status')=='failed'
    for rule in sorted(CONFIG['triggers'],key=lambda x:x['side']!='entry'):
        tree=condition_tree({'all':rule['all']},{} if failed else r.get('factors',{}))
        if rule['name']=='统一离场':
            branch=tree['children'][-1];branch['label']='以下三个情景至少满足一个'
            branch['children'][0]['label']='科技情景 两项需全部达标'
        elif rule['name']=='深跌修复入场':tree['children'][-1]['label']='以下修复条件至少一项满足'
        value=None if failed else r.get('triggers',{}).get(rule['name'])
        status(value,True)
        if value is not tree['value']:raise ValueError('Report/engine condition mismatch: '+rule['name'])
        name=rule.get('display_name') or ('金融信用离场' if rule['name']=='2007离场' else rule['name'])
        groups.append(dict(code=rule['name'],name=name,side=rule['side'],value=value,status=status(value,True),tree=tree))
    return groups


def headline(r,groups):
    if r.get('status')=='failed':return '本次监控未完成，组合与因子均待确认。'
    names=[g['name'] for g in groups if g['value'] is True]
    text='满足组合：'+'、'.join(names)+'。' if names else '今日暂无组合满足。'
    if any(v.get('value') is None for v in r.get('factors',{}).values()):text+='部分因子待确认。'
    elif r.get('status')!='ok':text+='数据检查存在异常。'
    return text


def run_time(r):
    value=r.get('generated_at')
    if not value:return '未知'
    dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None:return '未知'
    return dt.astimezone(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M')


def lines(tree,depth=0):
    for child in tree.get('children',[]):
        yield depth,child
        yield from lines(child,depth+1)


def note(r):
    prefix='离线核验样本，非最新行情报告。 ' if r.get('run_mode')=='offline_validation' else ''
    return prefix+'待确认表示数据不足、已过期或本次计算未完成；不等同于未达标。'


def render_text(r):
    groups=report_groups(r)
    text=['标普500模型每日监控',f"行情日期 {r.get('asof','未知')}    监控时间 {run_time(r)} 北京时间",headline(r,groups)]
    for g in groups:
        text.extend(['',g['name']+'：'+g['status'],'以下条件需全部达标'])
        text.extend('  '*depth+c['label']+'：'+c['status'] for depth,c in lines(g['tree']))
    return '\n'.join(text+['',note(r),'报告见DOCX附件，相关数据另存ZIP档案。'])


def render_html(r):
    groups=report_groups(r);esc=html.escape
    parts=['<h1 style="font-size:24px;color:#000">标普500模型每日监控</h1>',
           f"<p>行情日期 <b>{esc(r.get('asof','未知'))}</b>　监控时间 {esc(run_time(r))} 北京时间</p>",
           '<p><b>'+esc(headline(r,groups))+'</b></p>']
    for g in groups:
        parts.append(f'<h2 style="font-size:18px;margin:22px 0 5px;color:#000">{esc(g["name"])}　{g["status"]}</h2><p style="margin:4px 0">以下条件需全部达标</p>')
        for depth,c in lines(g['tree']):
            weight='bold' if c['kind']!='factor' else 'normal'
            parts.append(f'<p style="margin:4px 0 4px {depth*20}px;font-weight:{weight}">{esc(c["label"])}　<span style="color:#{COLORS[c["value"]]}">{c["status"]}</span></p>')
    parts.extend(['<p style="margin-top:24px">'+esc(note(r))+'</p>','<p>报告见DOCX附件，相关数据另存ZIP档案。</p>'])
    return '<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><body style="font:15px/1.6 Arial,Microsoft YaHei,sans-serif;max-width:850px;margin:auto;padding:20px;color:#222">'+''.join(parts)+'</body></html>'


def generate_docx(r,destination):
    from docx import Document
    from docx.shared import Cm,Inches,Pt,RGBColor
    from docx.enum.table import WD_CELL_VERTICAL_ALIGNMENT
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    groups=report_groups(r);doc=Document();section=doc.sections[0]
    section.page_width,section.page_height=Inches(8.5),Inches(11)
    section.top_margin=section.bottom_margin=Cm(1.6)
    section.left_margin=section.right_margin=Cm(1.8)
    for name,size in [('Normal',11),('Title',20),('Heading 1',13),('Heading 2',11)]:
        style=doc.styles[name];style.font.name='Microsoft YaHei';style.font.size=Pt(size);style.font.color.rgb=RGBColor(0,0,0)
        style.element.get_or_add_rPr().rFonts.set(qn('w:eastAsia'),'Microsoft YaHei')
        style.paragraph_format.space_after=Pt(3);style.paragraph_format.line_spacing=1.05
    doc.styles['Heading 1'].paragraph_format.space_before=Pt(9)
    doc.add_paragraph('标普500模型每日监控','Title')
    doc.add_paragraph(f"行情日期 {r.get('asof','未知')}    监控时间 {run_time(r)} 北京时间")
    doc.add_paragraph(headline(r,groups))
    if r.get('run_mode')=='offline_validation':doc.add_paragraph('离线核验样本，非最新行情报告。')
    table=doc.add_table(rows=1,cols=2);table.autofit=False
    table.columns[0].width,table.columns[1].width=Cm(14),Cm(3.99)
    for cell,text in zip(table.rows[0].cells,['组合','结果']):cell.text=text
    for g in groups:
        cells=table.add_row().cells;cells[0].text=g['name'];cells[1].text=g['status']
    for ri,row in enumerate(table.rows):
        row._tr.get_or_add_trPr().append(OxmlElement('w:cantSplit'))
        if ri==0:row._tr.get_or_add_trPr().append(OxmlElement('w:tblHeader'))
        for ci,cell in enumerate(row.cells):
            cell.width=Cm([14,3.99][ci]);cell.vertical_alignment=WD_CELL_VERTICAL_ALIGNMENT.CENTER
            pr=cell._tc.get_or_add_tcPr();borders=OxmlElement('w:tcBorders')
            for edge in ['top','left','bottom','right','insideH','insideV']:
                e=OxmlElement('w:'+edge)
                for k,v in [('val','single'),('sz','4'),('color','D9D9D9')]:e.set(qn('w:'+k),v)
                borders.append(e)
            pr.append(borders);margin=OxmlElement('w:tcMar')
            for edge in ['top','left','bottom','right']:
                e=OxmlElement('w:'+edge);e.set(qn('w:w'),'70');e.set(qn('w:type'),'dxa');margin.append(e)
            pr.append(margin)
            if ri==0:
                shade=OxmlElement('w:shd');shade.set(qn('w:fill'),'DCE6EE');pr.append(shade)
            for para in cell.paragraphs:
                para.paragraph_format.space_after=Pt(0)
                if ci==1:para.alignment=WD_ALIGN_PARAGRAPH.CENTER
                for run in para.runs:run.bold=ri==0
    for index,g in enumerate(groups):
        if index==2:doc.add_page_break()
        doc.add_paragraph(g['name']+'  '+g['status'],'Heading 1')
        p=doc.add_paragraph('以下条件需全部达标');p.paragraph_format.keep_with_next=True
        for depth,c in lines(g['tree']):
            p=doc.add_paragraph();p.paragraph_format.left_indent=Cm(depth*.45)
            p.paragraph_format.keep_together=True;p.paragraph_format.keep_with_next=c['kind']!='factor'
            run=p.add_run(c['label']+'  ');run.bold=c['kind']!='factor'
            run=p.add_run(c['status']);run.font.color.rgb=RGBColor.from_string(COLORS[c['value']])
    p=doc.add_paragraph(note(r));p.paragraph_format.space_before=Pt(10)
    for run in p.runs:run.font.size=Pt(9)
    footer=section.footer.paragraphs[0];footer.alignment=WD_ALIGN_PARAGRAPH.RIGHT
    footer.add_run('标普500每日监控  ')
    field=OxmlElement('w:fldSimple');field.set(qn('w:instr'),'PAGE');footer._p.append(field)
    for root in [doc.element,doc.styles.element]:
        for border in list(root.iter(qn('w:pBdr'))):border.getparent().remove(border)
    destination=Path(destination);destination.parent.mkdir(parents=True,exist_ok=True);doc.save(destination)
    return destination


def json_bytes(value):return json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False).encode('utf-8')


def csv_bytes(rows,fields):
    s=io.StringIO(newline='');w=csv.DictWriter(s,fieldnames=fields);w.writeheader();w.writerows(rows)
    return s.getvalue().encode('utf-8-sig')


def package_data(r,data,docx,destination):
    """Allowlist numerical database files and source evidence; never include mail credentials."""
    data=Path(data);groups=report_groups(r)
    saved_snapshot=data/'latest_report.json'
    if saved_snapshot.exists() and json.loads(saved_snapshot.read_text(encoding='utf-8'))!=r:
        raise ValueError('Report/database snapshot mismatch')
    members={'snapshot.json':json_bytes(r),'source_metadata.json':json_bytes(r.get('sources',{})),
             'data_warnings.json':json_bytes(r.get('warnings',[])),Path(docx).name:Path(docx).read_bytes(),
             'group_status.csv':csv_bytes([{k:g[k] for k in ('code','name','side','status','value')} for g in groups],['code','name','side','status','value'])}
    rows=[]
    for g in groups:
        for depth,c in lines(g['tree']):
            rows.append(dict(group=g['code'],depth=depth,kind=c['kind'],factor=c.get('key',''),condition=c['label'],value=c['value'],status=c['status']))
    members['factor_status.csv']=csv_bytes(rows,['group','depth','kind','factor','condition','value','status'])
    members['factor_values.csv']=csv_bytes([{'factor':k,'input':key,'value':value,'missing':value is None}
        for k,d in r.get('factors',{}).items() for key,value in d.get('inputs',{}).items()],['factor','input','value','missing'])
    files=[p for p in data.glob('*.csv') if p.is_file()]
    files += [data/n for n in ['source_metadata.json','factset_reports.json','cfnai_calendar.json','cpi_imputations.json','latest_pe_weekly.json','latest_report.json'] if (data/n).is_file()]
    for p in files:members['database/'+p.name]=p.read_bytes()
    references=set()
    def collect(value):
        if isinstance(value,dict):
            if isinstance(value.get('raw_file'),str):references.add(value['raw_file'].replace('\\','/'))
            for child in value.values():collect(child)
        elif isinstance(value,list):
            for child in value:collect(child)
    collect(r)
    for name in ['factset_reports.json','cfnai_calendar.json']:
        if (data/name).exists():collect(json.loads((data/name).read_text(encoding='utf-8')))
    missing=[];excluded=[]
    for ref in sorted(references):
        relative=Path(ref.removeprefix('data/v17/raw/'))
        if relative.name!=str(relative):raise ValueError('Unsafe raw evidence path')
        if relative.name.startswith('factset_'):
            excluded.append(ref);continue
        p=data/'raw'/relative
        if p.is_file():members['raw/'+relative.name]=p.read_bytes()
        else:missing.append(ref)
    members['raw_evidence_inventory.json']=json_bytes({'referenced_files':sorted(references),'unavailable_in_this_run':missing,
        'excluded_full_factset_reports':excluded,
        'note':'未在本次缓存取得的原始响应如实列出；整份FactSet PDF延续既有不再分发政策，保留提取值、来源和校验信息；原始数值序列保存在database目录。'})
    for folder,patterns in [('seed',['*.json','*.csv']),('model',['*.py']),('monitor_v17',['*.py','*.json']),('docs',['*.md'])]:
        for pattern in patterns:
            for p in (BASE/folder).glob(pattern):members['code/'+folder+'/'+p.name]=p.read_bytes()
    members['code/requirements.txt']=(BASE/'requirements.txt').read_bytes()
    members['说明.txt']=('DOCX为阅读版。snapshot.json为本次完整快照；factor_status.csv逐组合保留全部/任一条件及三态状态；'
        'factor_values.csv保存未为展示而舍入的输入值。database目录保存当次线上CSV/JSON数据库，seed保存历史初始化数据，'
        'code保存来源、算法、有效期政策及程序，raw保存本次可取得的相关来源响应。'
        '本档案对应线上监控数据库，不包含离线研究工作簿。缺少的原始响应见raw_evidence_inventory.json。'
        '模型冻结配置与当前采集政策有区别，生效政策见snapshot.json的data_policy_version及code/monitor_v17/data_policy.py。').encode('utf-8')
    members['manifest.json']=json_bytes({'format_version':FORMAT_VERSION,'asof':r.get('asof'),'generated_at':r.get('generated_at'),
        'code_commit':r.get('code_commit'),'data_policy_version':r.get('data_policy_version'),
        'files':{name:{'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()} for name,body in members.items()}})
    destination=Path(destination);stage=destination.with_suffix('.pending.zip')
    with zipfile.ZipFile(stage,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for name,body in members.items():z.writestr(name,body)
    with zipfile.ZipFile(stage) as z:
        if z.testzip():raise RuntimeError('Data archive integrity failure')
    stage.replace(destination);return destination


def build_deliverables(r,data,out):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    docx=generate_docx(r,out/'SP500_v17_latest.docx')
    archive=package_data(r,data,docx,out/'SP500_v17_data.zip')
    return docx,archive
