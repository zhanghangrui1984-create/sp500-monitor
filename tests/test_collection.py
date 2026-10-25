import json, tempfile, unittest
from pathlib import Path
from unittest.mock import patch, MagicMock
import numpy as np
import pandas as pd
from monitor_v17 import sources
from monitor_v17.data_policy import live_fcs, cfnai_health, parse_cfnai_calendar

class CollectionTests(unittest.TestCase):
    def test_fcs_uses_one_or_two_paired_days_and_carries_empty_weeks(self):
        dates=pd.to_datetime(['2026-10-05','2026-10-06','2026-10-13','2026-10-16','2026-10-20'])
        cp=pd.Series([4.08,4.07,4.08,4.13,4.08],index=dates)
        tb=pd.Series([4.05,4.04,4.09,4.08,4.08],index=dates)
        r=live_fcs(cp,tb,'2026-10-23')
        self.assertEqual(r['valid_days'],2);self.assertAlmostEqual(r['FCS_pp'],.02)
        self.assertEqual(r['source_week'],pd.Timestamp('2026-10-16'))
        r=live_fcs(cp,tb,'2026-10-26')
        self.assertEqual(r['valid_days'],1);self.assertAlmostEqual(r['FCS_pp'],0)
        r=live_fcs(cp,tb,'2026-11-23')
        self.assertEqual(r['valid_days'],1);self.assertEqual(r['age_days'],31)
        self.assertAlmostEqual(r['FCS_pp'],0)

    def test_fcs_never_pairs_different_dates_or_uses_unfinished_week(self):
        cp=pd.Series([5.,7.],index=pd.to_datetime(['2026-10-12','2026-10-20']))
        tb=pd.Series([3.,4.],index=pd.to_datetime(['2026-10-13','2026-10-20']))
        self.assertTrue(pd.isna(live_fcs(cp,tb,'2026-10-23')['FCS_pp']))
        self.assertEqual(live_fcs(cp,tb,'2026-10-26')['FCS_pp'],3)

    def test_cfnai_calendar_and_carry_before_release(self):
        html='<table id="other"><tr><td>bad</td></tr></table><table id="cfnai-table"><tr><td>September 21, 2026</td><td>August 2026</td></tr><tr><td>October 26, 2026</td><td>September 2026</td></tr><tr><td>November 23, 2026</td><td>October 2026</td></tr></table>'
        rows=parse_cfnai_calendar(html)
        for day in ['2026-10-23','2026-10-26','2026-11-02']:
            self.assertFalse(cfnai_health('2026-08-01',day,rows)[0])
        self.assertTrue(cfnai_health('2026-08-01','2026-11-03',rows)[0])
        self.assertTrue(cfnai_health('2026-08-01','2026-11-24',rows)[0])
        self.assertFalse(cfnai_health('2026-09-01','2026-11-03',rows)[0])
        self.assertTrue(cfnai_health('2026-10-01','2027-01-01',rows)[0])
        with self.assertRaises(ValueError):parse_cfnai_calendar('<html>unavailable</html>')

    def yahoo_payload(self):
        # Values and timestamps from the failing 2026-10-24 production XLF response.
        return {'chart':{'error':None,'result':[{'timestamp':[1792675800,1792762200],
            'indicators':{'quote':[{'close':[56.650001525878906,None]}],
                          'adjclose':[{'adjclose':[56.650001525878906,None]}]},
            'meta':{'regularMarketTime':1792785600,'regularMarketPrice':55.9}}]}}

    def test_yahoo_delayed_daily_row_uses_verified_regular_close(self):
        s,proof=sources.parse_yahoo(json.dumps(self.yahoo_payload()),'XLF','2026-10-23',True,'2026-10-24T00:53:00Z')
        self.assertEqual(s.iloc[-1],55.9)
        self.assertIn('close_completion',proof)
        self.assertEqual(s.iloc[0],56.650001525878906)

    def test_yahoo_rejects_intraday_afterhours_stale_and_wrong_day(self):
        for seconds in [-3600,3600,-86400,86400]:
            obj=self.yahoo_payload();obj['chart']['result'][0]['meta']['regularMarketTime']+=seconds
            with self.assertRaises(ValueError):
                sources.parse_yahoo(json.dumps(obj),'XLF','2026-10-23',True,'2026-10-24T00:53:00Z')
        with self.assertRaises(ValueError):
            sources.parse_yahoo(json.dumps(self.yahoo_payload()),'XLF','2026-10-23',True,'2026-10-23T20:30:00Z')

    def test_yahoo_retains_published_adjusted_close(self):
        obj=self.yahoo_payload();obj['chart']['result'][0]['indicators']['adjclose'][0]['adjclose'][-1]=55.85
        s,proof=sources.parse_yahoo(json.dumps(obj),'XLF','2026-10-23',True)
        self.assertEqual(s.iloc[-1],55.85);self.assertFalse(proof)

    def test_http_200_non_json_response_retries_before_acceptance(self):
        body=json.dumps({'count':1,'observations':[{'date':'2026-10-20','value':'4.08'}]}).encode()
        bad=MagicMock();bad.__enter__.return_value.read.return_value=b'<html>temporary upstream error</html>'
        good=MagicMock();good.__enter__.return_value.read.return_value=body
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            with patch.object(sources,'BASE',root),patch.object(sources,'RAW',root/'raw'),patch.object(sources,'urlopen',side_effect=[bad,good]) as call,patch.object(sources.time,'sleep'):
                raw,meta=sources.request('https://example.test','fred_test',validate=sources.validate_fred)
                self.assertEqual(raw,body);self.assertEqual(call.call_count,2)

    def test_cache_fallback_keeps_release_provenance(self):
        with tempfile.TemporaryDirectory() as folder:
            dest=Path(folder)
            (dest/'source_metadata.json').write_text(json.dumps({'CFNAIDIFF':{'observed_on':'2026-08-01','released_on':'2026-09-21'}}))
            (dest/'CFNAIDIFF.csv').write_text('date,value\n2026-08-01,0.02\n')
            with patch.object(sources,'DATA',dest),patch.object(sources,'fred',side_effect=ValueError('unavailable')),patch.object(sources,'yahoo',side_effect=ValueError('unavailable')):
                data,errors=sources.collect('2026-10-23')
            self.assertIn('CFNAIDIFF',errors)
            self.assertEqual(data['CFNAIDIFF'][1]['released_on'],'2026-09-21')

if __name__=='__main__':unittest.main()
