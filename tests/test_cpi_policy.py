import unittest
import numpy as np
import pandas as pd
from monitor_v17.data_policy import cpi_monthly,cpi_gap_records
from monitor_v17.calculator import cmp


class CPIGapTests(unittest.TestCase):
    def test_single_missing_month_including_absent_row_uses_arithmetic_mean(self):
        for explicit in (True,False):
            raw=pd.Series([320.,np.nan,324.],index=pd.date_range('2025-09-01',periods=3,freq='MS'))
            if not explicit:raw=raw.dropna()
            before=raw.copy()
            frame=cpi_monthly(raw)
            self.assertEqual(frame.loc['2025-10-01','calculation_value'],322.)
            self.assertTrue(frame.loc['2025-10-01','is_imputed'])
            self.assertTrue(pd.isna(frame.loc['2025-10-01','raw_value']))
            pd.testing.assert_series_equal(raw,before)
            record=cpi_gap_records(frame)[0]
            self.assertEqual((record['left_month'],record['right_month']),('2025-09-01','2025-11-01'))
            self.assertEqual((record['left_value'],record['right_value']),(320.,324.))

    def test_repeated_isolated_gaps_fill_without_chaining(self):
        raw=pd.Series([320.,np.nan,324.,np.nan,330.],index=pd.date_range('2025-09-01',periods=5,freq='MS'))
        frame=cpi_monthly(raw)
        self.assertEqual(list(frame.calculation_value),[320.,322.,324.,327.,330.])
        self.assertEqual(int(frame.is_imputed.sum()),2)

    def test_no_edge_extrapolation_or_multi_month_fill(self):
        raw=pd.Series([np.nan,320.,np.nan,np.nan,324.,np.nan],index=pd.date_range('2025-07-01',periods=6,freq='MS'))
        frame=cpi_monthly(raw)
        self.assertFalse(frame.is_imputed.any())
        self.assertEqual(int(frame.calculation_value.isna().sum()),4)

    def test_future_or_unavailable_right_neighbour_cannot_fill(self):
        raw=pd.Series([320.,np.nan,324.],index=pd.date_range('2025-09-01',periods=3,freq='MS'))
        frame=cpi_monthly(raw,'2025-10-31')
        self.assertTrue(pd.isna(frame.calculation_value.iloc[-1]))
        # Production passes only observations whose releases are usable as of the run.
        release_filtered=raw.iloc[:2]
        self.assertFalse(cpi_monthly(release_filtered,'2025-12-01').is_imputed.any())

    def test_later_official_value_supersedes_previous_estimate(self):
        raw=pd.Series([320.,321.8,324.],index=pd.date_range('2025-09-01',periods=3,freq='MS'))
        frame=cpi_monthly(raw)
        self.assertEqual(frame.loc['2025-10-01','calculation_value'],321.8)
        self.assertFalse(frame.is_imputed.any());self.assertEqual(cpi_gap_records(frame),[])

    def test_imputed_year_ago_index_restores_known_yoy_and_thresholds(self):
        raw=pd.Series(320.,index=pd.date_range('2025-09-01','2026-10-01',freq='MS'))
        raw.loc['2025-10-01']=np.nan;raw.loc['2025-11-01']=324.
        for change,high,deflation in [(0.0539,True,False),(-.01,False,True),(0.,False,False)]:
            raw.loc['2026-10-01']=322.*(1+change)
            frame=cpi_monthly(raw);c=frame.calculation_value
            cy=(100*(c/c.shift(12)-1)).iloc[-1]
            self.assertAlmostEqual(cy,change*100)
            self.assertIs(cmp(cy,'__ge__',5.39),high)
            self.assertIs(cmp(cy,'__lt__',0),deflation)

    def test_invalid_values_or_duplicate_months_are_not_estimates(self):
        for invalid in (0.,-1.,float('inf')):
            raw=pd.Series([320.,invalid,324.],index=pd.date_range('2025-09-01',periods=3,freq='MS'))
            with self.assertRaises(ValueError):cpi_monthly(raw)
        with self.assertRaises(ValueError):
            cpi_monthly(pd.Series([320.,324.],index=pd.to_datetime(['2025-09-01','2025-09-01'])))


if __name__=='__main__':unittest.main()
