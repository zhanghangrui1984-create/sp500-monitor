"""NFR (NFCI Recovery): trailing weekly high minus current NFCI, in points."""
from datetime import datetime
from decimal import Decimal
import numpy as np
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar


def _mills(value):
    x = Decimal(str(value)) * 1000
    if not x.is_finite() or x != x.to_integral_value():
        raise ValueError('NFCI and NFR thresholds require finite values with <=3 decimals')
    return int(x)


def calculate_nfr(source, window=52, tier1=.634, tier2=.958):
    """Complete consecutive W-FRI window, inclusive of the current week.

    Flags are nested; display level is 0/1/2. Incomplete windows stay missing.
    Integer thousandths prevent inclusive-boundary floating-point errors.
    """
    if not isinstance(window, int) or isinstance(window, bool) or window < 1:
        raise ValueError('window must be a positive integer')
    lower, upper = _mills(tier1), _mills(tier2)
    if not 0 <= lower < upper:
        raise ValueError('Need 0 <= tier1 < tier2')
    s = source.copy().astype(float)
    s.index = pd.DatetimeIndex(s.index)
    if s.empty or s.index.has_duplicates or not (s.index.weekday == 4).all():
        raise ValueError('Need unique Friday observation dates')
    s = s.sort_index().reindex(pd.date_range(s.index.min(), s.index.max(), freq='W-FRI'))
    mills = s.map(lambda x: np.nan if pd.isna(x) else _mills(x))
    count = mills.rolling(window, min_periods=1).count().astype(int)
    high = mills.rolling(window, min_periods=window).max()
    recovery = high - mills
    one = recovery.ge(lower).astype('Int64').mask(recovery.isna())
    two = recovery.ge(upper).astype('Int64').mask(recovery.isna())
    level = (one + two).astype('Int64')
    return pd.DataFrame({'NFCI': s, 'rolling_high': high/1000, 'NFR': recovery/1000,
                         'NFR_I': one, 'NFR_II': two, 'NFR_level': level,
                         'valid_count': count}, index=s.index)


def publication_schedule(observations, sessions, actual_release_dates=None):
    """Current weekly cadence proxy: next Wed 08:30 ET, Thu if Mon-Wed holiday.

    Use at that day's close (or the first later XNYS session). For future
    observations pass confirmed release dates; no inferred historical PIT claim.
    """
    observations = pd.DatetimeIndex(observations)
    sessions = pd.DatetimeIndex(sessions)
    if sessions.tz is not None:
        sessions = sessions.tz_localize(None)
    if (observations.empty or observations.has_duplicates or
            not observations.is_monotonic_increasing or not (observations.weekday == 4).all()):
        raise ValueError('Need increasing unique weekly Friday observations')
    if not sessions.is_monotonic_increasing or sessions.has_duplicates:
        raise ValueError('Sessions must be sorted and unique')
    actual = {pd.Timestamp(k): pd.Timestamp(v).normalize() for k,v in (actual_release_dates or {}).items()}
    holidays = USFederalHolidayCalendar().holidays(observations[0], observations[-1]+pd.Timedelta(days=10))
    releases = []
    for obs in observations:
        date = obs + pd.Timedelta(days=5)
        if any(date-pd.Timedelta(days=2) <= h <= date for h in holidays):
            date += pd.Timedelta(days=1)
        releases.append(actual.get(obs, date))
    releases = pd.DatetimeIndex(releases)
    if not (releases > observations).all():
        raise ValueError('Release must follow observation Friday')
    pos = sessions.searchsorted(releases, side='left')
    if (pos >= len(sessions)).any():
        raise ValueError('Extend the exchange calendar beyond every release')
    usable = sessions[pos]
    if not usable.is_monotonic_increasing or usable.has_duplicates:
        raise ValueError('Availability must increase strictly')
    return pd.DataFrame({'release_date': releases, 'available_date': usable,
                         'release_basis': ['actual' if d in actual else 'current_weekly_schedule_proxy' for d in observations]}, index=observations)


def align_to_sessions(weekly, sessions):
    days = pd.DataFrame({'date': pd.DatetimeIndex(sessions).as_unit('ns')})
    if not days.date.is_monotonic_increasing or days.date.duplicated().any():
        raise ValueError('Daily axis must be sorted and unique')
    data = weekly.rename_axis('observation_week').reset_index()
    data['available_date'] = pd.to_datetime(data.available_date).astype('datetime64[ns]')
    if data.available_date.isna().any() or not data.available_date.is_monotonic_increasing or data.available_date.duplicated().any():
        raise ValueError('Availability dates must be complete and increasing')
    return pd.merge_asof(days, data, left_on='date', right_on='available_date', direction='backward').set_index('date')


def from_workbook(wb, sessions, settings):
    """Recompute from raw NFCI, not cached NFR, and use the saved release calendar."""
    rows = [r for r in wb[settings['source_sheet']].values if r and isinstance(r[0], datetime)]
    if not rows or any(len(r) < 9 or not isinstance(r[7], datetime) or not isinstance(r[8], datetime) for r in rows):
        raise ValueError('Every weekly source row needs observation, release and available dates')
    dates = pd.DatetimeIndex([r[0] for r in rows])
    if dates.has_duplicates or not dates.is_monotonic_increasing:
        raise ValueError('Weekly rows must be sorted and unique')
    weekly = calculate_nfr(pd.Series([r[1] for r in rows], index=dates),
                           settings['window_weeks'], settings['tier1'], settings['tier2'])
    if not weekly.index.equals(dates):
        raise ValueError('Missing week: insert an explicit blank observation and release dates')
    weekly['release_date'] = pd.to_datetime([r[7] for r in rows])
    weekly['available_date'] = pd.to_datetime([r[8] for r in rows])
    if (weekly.release_date <= dates).any() or (weekly.available_date < weekly.release_date).any():
        raise ValueError('Invalid release/availability ordering')
    return weekly, align_to_sessions(weekly, sessions)
