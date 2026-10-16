"""Verbatim numerical functions from the frozen research engine."""
import numpy as np
import pandas as pd

def calm_baseline(prices,vix,lookback=252,minimum=63,threshold=.1):
    calm=(1-prices/prices.cummax()<threshold)&vix.notna();positions=np.flatnonzero(calm)
    mu=np.full(len(prices),np.nan);sd=mu.copy();vals=vix.to_numpy()
    for i in range(len(prices)):
        end=np.searchsorted(positions,i,side='right');start=np.searchsorted(positions,max(0,i-lookback+1))
        if end-start<minimum:start=max(0,end-minimum)
        if end-start>=minimum:
            x=vals[positions[start:end]];mu[i]=x.mean();sd[i]=x.std(ddof=1)
    return pd.Series(mu,index=prices.index),pd.Series(sd,index=prices.index)

def trigger_expression(node,factors):
    if isinstance(node,str):return factors[node].astype('boolean')
    if len(node)!=1:raise ValueError('One logical operator per node')
    if 'not' in node:return ~trigger_expression(node['not'],factors)
    op=next(iter(node))
    if op not in {'all','any'} or not node[op]:raise ValueError('Invalid trigger operator')
    values=[trigger_expression(x,factors) for x in node[op]];r=values[0]
    for x in values[1:]:r=r&x if op=='all' else r|x
    return r

def triggers_from_factors(factors,rules):
    return pd.DataFrame({r['name']:trigger_expression({'all':r['all']},factors) for r in rules},index=factors.index)
