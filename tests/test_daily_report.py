import copy,hashlib,itertools,json,os,tempfile,unittest,zipfile
from pathlib import Path
from unittest.mock import patch
import pandas as pd
from docx import Document
from monitor_v17.daily_report import CONFIG,report_groups,render_html,render_text,build_deliverables,FORMAT_VERSION
from monitor_v17.shared_core import triggers_from_factors
from monitor_v17.report import build_message


def snapshot(values=None):
    keys=set()
    def collect(node):
        if isinstance(node,str):keys.add(node)
        else:
            for op,items in node.items():
                for child in ([items] if op=='not' else items):collect(child)
    for rule in CONFIG['triggers']:collect({'all':rule['all']})
    flags={k:False for k in keys};flags.update(values or {})
    frame=pd.DataFrame({k:pd.Series([v],dtype='boolean') for k,v in flags.items()})
    triggers=triggers_from_factors(frame,CONFIG['triggers']).iloc[0]
    return {'status':'ok','asof':'2026-11-02','generated_at':'2026-11-02T22:30:00+00:00',
            'model_version':CONFIG['version'],'report_format_version':FORMAT_VERSION,
            'factors':{k:{'value':v,'inputs':{'test_full_precision':.123456789012345},'source_status':{'source_url':'https://example.test/internal-provenance'}} for k,v in flags.items()},
            'triggers':{k:None if pd.isna(v) else bool(v) for k,v in triggers.items()},'sources':{},'warnings':[]}


class DailyReportTests(unittest.TestCase):
    def test_financial_three_states_and_negation_preserved(self):
        keys=['NFCI_4w_down','FSX ≤ -10%','FCS ≥ 1.00pp']
        for states in itertools.product([True,False,None],repeat=3):
            s=snapshot(dict(zip(keys,states)))
            g=next(g for g in report_groups(s) if g['code']=='2007离场')
            self.assertIs(g['value'],s['triggers']['2007离场'])
        s=snapshot({'CPI_y ≥ 5.39%':None})
        g=next(g for g in report_groups(s) if g['code']=='白银坑入场')
        neg=g['tree']['children'][-1]
        self.assertIsNone(neg['value']);self.assertEqual(neg['status'],'待确认')

    def test_or_path_and_factor_unknown_are_not_flattened(self):
        s=snapshot({'S-6':True,'S- ≥ P99':True,'V ≥ V_c+3σ':True,'NFR ≥ 0.958':True,'ERX达标':None})
        g=next(g for g in report_groups(s) if g['code']=='深跌修复入场')
        self.assertIs(g['value'],True)
        self.assertEqual(g['tree']['children'][-1]['kind'],'any')
        self.assertIsNone(g['tree']['children'][-1]['children'][-1]['value'])
        text=render_text(s);self.assertIn('至少一项满足',text);self.assertIn('待确认',text)
        s=snapshot({'NFCI_26w_up':True,'DEFENSE':True,'P+':True,'Y-':True,'NSX ≥ 40%':False})
        self.assertFalse(next(g for g in report_groups(s) if g['code']=='统一离场')['value'])
        s=snapshot({'NFCI_26w_up':True,'DEFENSE':True,'P+':True,'CPI_y < 0%':True,'CPI_y ≥ 5.39%':None})
        self.assertTrue(next(g for g in report_groups(s) if g['code']=='统一离场')['value'])

    def test_report_mismatch_stops_misleading_delivery_and_failure_is_unknown(self):
        s=snapshot();s['triggers']['2007离场']=True
        with self.assertRaises(ValueError):report_groups(s)
        failed={'status':'failed','asof':'未知','fatal_error':'internal failure'}
        self.assertTrue(all(g['value'] is None for g in report_groups(failed)))
        self.assertIn('本次监控未完成',render_text(failed))
        self.assertNotIn('internal failure',render_html(failed))

    def test_docx_only_mail_and_complete_separate_archive(self):
        s=snapshot({'FCS ≥ 1.00pp':None})
        s['factors']['FCS ≥ 1.00pp']['inputs']={'FCS_pp':None,'last_valid_FCS_pp':2.466666666666,'age_days':33}
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);data=root/'data';data.mkdir();(data/'raw').mkdir()
            (data/'DCPF3M.csv').write_text('date,value\n2026-10-20,4.08\n')
            (data/'CPIAUCSL_calculation.csv').write_text('month,raw_value,calculation_value,is_imputed\n2025-10-01,,324.654,True\n')
            cpi_proof={'fills':[{'month':'2025-10-01','raw_value':None,'calculation_value':324.654}]}
            (data/'cpi_imputations.json').write_text(json.dumps(cpi_proof),encoding='utf-8')
            (data/'mail_receipts.json').write_text('{"private":"not part of numerical database"}')
            (data/'raw/sample.bin').write_bytes(b'public response')
            (data/'raw/factset_example.bin').write_bytes(b'full licensed PDF')
            s['sources']={'test':{'raw_file':'data/v17/raw/sample.bin'},'FactSet':{'raw_file':'data/v17/raw/factset_example.bin'}}
            (data/'latest_report.json').write_text(json.dumps(s),encoding='utf-8')
            original=copy.deepcopy(s)
            docx,archive=build_deliverables(s,data,root/'reports')
            doc=Document(docx);text='\n'.join(p.text for p in doc.paragraphs)
            self.assertIn('待确认',text);self.assertIn('2026-11-03 06:30',text)
            for forbidden in ['internal-provenance','source_url','last_valid_FCS_pp','code_commit','2.466666666666']:
                self.assertNotIn(forbidden,text);self.assertNotIn(forbidden,render_html(s))
            with zipfile.ZipFile(archive) as z:
                manifest=json.loads(z.read('manifest.json'))
                for name,meta in manifest['files'].items():self.assertEqual(hashlib.sha256(z.read(name)).hexdigest(),meta['sha256'])
                self.assertEqual(json.loads(z.read('snapshot.json')),s)
                self.assertEqual(z.read(docx.name),docx.read_bytes())
                self.assertIn('database/DCPF3M.csv',z.namelist());self.assertIn('raw/sample.bin',z.namelist())
                self.assertIn('database/CPIAUCSL_calculation.csv',z.namelist())
                self.assertEqual(json.loads(z.read('database/cpi_imputations.json')),cpi_proof)
                self.assertNotIn('database/mail_receipts.json',z.namelist())
                self.assertNotIn('raw/factset_example.bin',z.namelist())
                self.assertEqual(json.loads(z.read('raw_evidence_inventory.json'))['excluded_full_factset_reports'],['data/v17/raw/factset_example.bin'])
                self.assertIn('code/monitor_v17/data_policy.py',z.namelist())
                self.assertIn('2.466666666666',z.read('factor_values.csv').decode('utf-8-sig'))
            with patch.dict(os.environ,{'EMAIL_ADDRESS':'sender@example.test','EMAIL_RECEIVER':'recipient@example.test'}):
                msg=build_message(s,render_html(s),docx)
            attachments=list(msg.iter_attachments());self.assertEqual(len(attachments),1)
            self.assertTrue(attachments[0].get_filename().endswith('.docx'))
            self.assertEqual(attachments[0].get_payload(decode=True),docx.read_bytes())
            self.assertEqual(s,original)
            self.assertFalse((data/'deliverables').exists())


if __name__=='__main__':unittest.main()
