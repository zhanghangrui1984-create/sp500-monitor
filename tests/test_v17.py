import hashlib, itertools, json, unittest
from pathlib import Path
import numpy as np
import pandas as pd
from monitor_v17.calculator import CONFIG,pe_weekly
from monitor_v17.shared_core import triggers_from_factors
from model.dynamic_drawdown import DrawdownThresholdState
from model.inventory_exit import weekly
from model.financial_factors import calculate_fcs,weekly_fcs,align_fcs
from monitor_v17.report import delivery_key

BASE=Path(__file__).resolve().parents[1]

class ContractTests(unittest.TestCase):
    def test_new_market_close_is_not_suppressed_by_same_day_manual_test(self):
        old={'model_version':'v17','status':'ok','asof':'2026-10-15'}
        new=dict(old,asof='2026-10-16')
        self.assertNotEqual(delivery_key(old,'2026-10-16'),delivery_key(new,'2026-10-16'))
        self.assertEqual(delivery_key(new,'2026-10-16'),delivery_key(dict(new),'2026-10-16'))
        self.assertNotEqual(delivery_key(new,'2026-10-16'),delivery_key(new,'2026-10-17'))

    def test_frozen_contract(self):
        manifest=json.loads((BASE/'monitor_v17/research_manifest.json').read_text(encoding='utf8'))
        key='configs\\backtest\\model_1995_split_financial_exit_v17.json'
        expected=manifest.get(key,manifest.get(key.replace('\\','/')))
        self.assertEqual(hashlib.sha256((BASE/'monitor_v17/model.json').read_bytes()).hexdigest(),expected)
        self.assertEqual([x['all'] for x in CONFIG['triggers'] if x['name']=='2007离场'][0],['NFCI_4w_down','FSX ≤ -10%','FCS ≥ 1.00pp'])
        self.assertEqual(len(CONFIG['triggers']),5)
        for name,expected in manifest.items():
            normalized=name.replace('\\','/')
            if normalized.startswith('model/'):
                self.assertEqual(hashlib.sha256((BASE/normalized).read_bytes()).hexdigest(),expected,normalized)

    def test_all_three_valued_financial_states(self):
        rule=[x for x in CONFIG['triggers'] if x['name']=='2007离场']
        for values in itertools.product([True,False,None],repeat=3):
            f=pd.DataFrame({k:pd.Series([v],dtype='boolean') for k,v in zip(rule[0]['all'],values)})
            result=triggers_from_factors(f,rule).iloc[0,0]
            expected=False if False in values else None if None in values else True
            self.assertEqual(None if pd.isna(result) else bool(result),expected)

    def test_no_continuous_decline_or_positive_nfci_requirement(self):
        dates=pd.date_range('2025-01-03',periods=5,freq='W-FRI')
        w=weekly(pd.Series([-.2,-.1,-.4,-.1,-.3],index=dates),4)
        self.assertLess(w.NFCI_change.iloc[-1],0)
        w=weekly(pd.Series([-.2,-.1,-.4,-.1,-.2],index=dates),4)
        self.assertEqual(w.NFCI_change.iloc[-1],0)
        w=weekly(pd.Series([-.2,-.1,-.1,-.3],index=dates[[0,1,3,4]]),4)
        self.assertEqual(len(w),5)

    def test_drawdown_threshold_precedes_current_price(self):
        s=DrawdownThresholdState(episode_start='2000-01-01',min_episodes=2)
        dates=pd.bdate_range('2000-01-03',periods=7)
        for d,p in zip(dates[:5],[100,90,100,80,100]):s.observe(d,p)
        before=s.snapshot(dates[5]);s.observe(dates[5],50)
        self.assertEqual(before['DD_P99'],s.snapshot(dates[6])['DD_P99'])
        self.assertAlmostEqual(before['DD_P99'],.199)

    def test_fcs_pairs_before_week_mean_and_expires(self):
        idx=pd.bdate_range('2026-01-05',periods=5)
        cp=pd.Series([4,np.nan,4,4,4],index=idx);tb=pd.Series([3,3,np.nan,3,3],index=idx)
        w=weekly_fcs(calculate_fcs(cp,tb));self.assertEqual(w.valid_days.iloc[0],3)
        self.assertEqual(w.qualified_pp.iloc[0],1)
        dates=pd.DatetimeIndex(['2026-01-23','2026-01-26'])
        a=align_fcs(w.qualified_pp,pd.Series([pd.Timestamp('2026-01-12')],index=w.index),dates,14)
        self.assertEqual(a.value.iloc[0],1);self.assertTrue(pd.isna(a.value.iloc[1]))

    def test_pe_carry_uses_original_price_not_new_price(self):
        cal=pd.bdate_range('2019-01-01','2026-03-30');spx=pd.Series(6000.,index=cal)
        spx.loc['2026-02-26']=5000
        seed=[{'week':'2026-02-23','effective_on':'2026-03-02','P':20.,'E':250.}]
        reports=[{'published_on':'2026-02-27','data_asof':'2026-02-26','pe_reported':20.,'source_url':'https://example.test'}]
        pe=pe_weekly(seed,reports,spx,cal,'2026-03-16')
        self.assertEqual(pe.iloc[-1].E,250);self.assertEqual(pe.iloc[-1].P,20)
        self.assertEqual(pe.iloc[-1].data_asof,'2026-02-26')

    def test_frozen_historical_factor_signals(self):
        fpath=BASE/'tests/fixtures/historical_factors.csv'
        expected=BASE/'tests/fixtures/historical_triggers.csv'
        self.assertTrue(fpath.exists(),'Historical parity fixtures must ship with production')
        f=pd.read_csv(fpath,index_col=0).astype('boolean');t=pd.read_csv(expected,index_col=0).astype('boolean')
        actual=triggers_from_factors(f,CONFIG['triggers'])
        pd.testing.assert_frame_equal(actual,t)

if __name__=='__main__':unittest.main()
