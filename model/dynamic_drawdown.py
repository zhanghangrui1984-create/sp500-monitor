"""Causal daily ATH-drawdown quantiles: threshold first, today's price second.

The default estimator preserves the legacy equal-weight linear P95/P99
definition. An explicitly configured half-life uses weighted inverse-CDF
quantiles instead; the two estimators must not be conflated.
"""
from dataclasses import dataclass
import math

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Episode:
    peak_date: pd.Timestamp
    trough_date: pd.Timestamp
    recovery_date: pd.Timestamp
    peak_close: float
    trough_close: float
    decline: float


def linear_quantile(values, probability):
    """Type 7: zero-based h=(n-1)*p; interpolate adjacent sorted depths."""
    x = sorted(float(v) for v in values)
    if not x or not 0 <= probability <= 1 or not all(math.isfinite(v) for v in x):
        raise ValueError('Finite nonempty sample and probability in [0, 1] required')
    h = (len(x) - 1) * probability
    j, g = math.floor(h), h - math.floor(h)
    return x[j] + g * (x[min(j + 1, len(x) - 1)] - x[j])


class DrawdownThresholdState:
    """Append-only price state. Snapshot for t can only precede observe(t)."""
    def __init__(self, episode_start='1975-01-01', min_episodes=100,
                 probabilities=(.95, .99), half_life_years=None):
        if not isinstance(min_episodes, int) or isinstance(min_episodes, bool) or min_episodes < 2:
            raise ValueError('min_episodes must be an integer >= 2')
        if tuple(probabilities) != (.95, .99):
            raise ValueError('This model defines exactly P95 and P99')
        if half_life_years is not None and (not math.isfinite(half_life_years) or half_life_years <= 0):
            raise ValueError('half_life_years must be finite and positive')
        self.episode_start = pd.Timestamp(episode_start)
        self.min_episodes = min_episodes
        self.probabilities = tuple(probabilities)
        self.half_life_years = half_life_years
        self.episodes = []
        self.last_date = None
        self.peak_date = self.trough_date = None
        self.peak_close = self.trough_close = None
        self.underwater = False
        self._cache = None

    def observe(self, date, close):
        date, close = pd.Timestamp(date), float(close)
        if pd.isna(date) or (self.last_date is not None and date <= self.last_date):
            raise ValueError('Observation dates must be strictly increasing')
        if not math.isfinite(close) or close <= 0:
            raise ValueError('Every price must be finite and positive; never silently fill missing prices')
        if self.peak_close is None or close >= self.peak_close:
            if self.underwater:
                if self.peak_date >= self.episode_start:
                    self.episodes.append(Episode(self.peak_date, self.trough_date, date,
                        self.peak_close, self.trough_close, 1 - self.trough_close / self.peak_close))
                    self._cache = None
            self.peak_date = self.trough_date = date
            self.peak_close = self.trough_close = close
            self.underwater = False
        else:
            self.underwater = True
            if close < self.trough_close:
                self.trough_close, self.trough_date = close, date
        self.last_date = date

    def snapshot(self, decision_date):
        date = pd.Timestamp(decision_date)
        if pd.isna(date) or (self.last_date is not None and date <= self.last_date):
            raise ValueError('Decision day must be strictly after the latest observed price')
        n = len(self.episodes)
        if self._cache is None:
            values = np.array([e.decline for e in self.episodes], dtype=float)
            weights = np.ones(n, dtype=float)
            if n and self.half_life_years is not None:
                # Common time decay cancels after normalization. Anchoring at
                # the latest completed trough gives identical ratios without underflow.
                anchor = max(e.trough_date for e in self.episodes)
                ages = np.array([(anchor-e.trough_date).days/365.25 for e in self.episodes])
                weights = np.exp2(-ages/self.half_life_years)
            lower = upper = float('nan')
            if n >= self.min_episodes:
                if self.half_life_years is None:
                    lower, upper = [linear_quantile(values, p) for p in self.probabilities]
                else:
                    order = np.argsort(values, kind='stable')
                    cumulative = np.cumsum(weights[order]) / weights.sum()
                    ids = np.minimum(np.searchsorted(cumulative, self.probabilities, side='left'), n-1)
                    lower, upper = values[order][ids]
            self._cache = (float(lower), float(upper), float(weights.sum()**2/np.square(weights).sum()) if n else 0.)
        lower, upper, effective_n = self._cache
        return dict(date=date, input_cutoff=self.last_date, sample_count=n,
                    latest_sample_recovery=self.episodes[-1].recovery_date if n else pd.NaT,
                    DD_P95=lower, DD_P99=upper, effective_n=effective_n)


def calculate_daily(prices, **settings):
    """Return daily thresholds/factors and episodes reconstructed online.

Callers supply a complete ordered price history including ATH warmup. Rows
are supplied price sessions; missing exchange sessions must be caught by the
data-loading/calendar layer, rather than inferred or filled here.
"""
    if not isinstance(prices.index, pd.DatetimeIndex) or prices.empty:
        raise ValueError('A nonempty DatetimeIndex price series is required')
    if prices.index.hasnans or not prices.index.is_unique or not prices.index.is_monotonic_increasing:
        raise ValueError('Unique increasing nonmissing dates required')
    if prices.isna().any() or not np.isfinite(prices).all() or (prices <= 0).any():
        raise ValueError('Complete positive finite prices required')
    state = DrawdownThresholdState(**settings)
    rows = []
    for date, close in prices.items():
        row = state.snapshot(date)  # Only the state through the previous observation.
        state.observe(date, close)
        row.update(close=float(close), S_max_history=state.peak_close,
                   S_drawdown_pct=1-float(close)/state.peak_close)
        rows.append(row)
    daily = pd.DataFrame(rows).set_index('date')
    dd, lower, upper = [daily[c].round(12) for c in ['S_drawdown_pct','DD_P95','DD_P99']]
    unknown = lower.isna() | upper.isna()
    daily['DD_attention'] = dd.ge(lower).astype('boolean').mask(unknown)
    daily['DD_middle'] = (dd.ge(lower) & dd.lt(upper)).astype('boolean').mask(unknown)
    daily['DD_deep'] = dd.ge(upper).astype('boolean').mask(unknown)
    episodes = pd.DataFrame([vars(e) for e in state.episodes], columns=Episode.__annotations__)
    return daily, episodes, state
