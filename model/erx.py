"""ERX: CFNAIDIFF recovery from a rolling, complete calendar-month low."""
from decimal import Decimal
from datetime import datetime
import numpy as np
import pandas as pd


def _hundredths(value):
    n = Decimal(str(value)) * 100
    if n != n.to_integral_value():
        raise ValueError('CFNAIDIFF and thresholds must have at most two decimals')
    return int(n)


def calculate_erx(source, window=36, low_threshold=-0.27, rebound_threshold=0.50):
    """Include current month. Missing months invalidate windows, never compress time."""
    if not isinstance(window, int) or isinstance(window, bool) or window < 1:
        raise ValueError('window must be a positive integer')
    s = source.copy().astype(float)
    s.index = pd.DatetimeIndex(s.index)
    if s.empty or s.index.has_duplicates or not s.index.is_month_start.all():
        raise ValueError('Need unique month-start observation dates')
    s = s.sort_index().reindex(pd.date_range(s.index.min(), s.index.max(), freq='MS'))
    cents = s.map(lambda v: np.nan if pd.isna(v) else _hundredths(v))
    count = cents.rolling(window, min_periods=1).count().astype(int)
    low = cents.rolling(window, min_periods=window).min()
    rebound = cents - low
    qualified = ((low <= _hundredths(low_threshold)) &
                 (rebound >= _hundredths(rebound_threshold))).astype('Int64').mask(low.isna())
    return pd.DataFrame({'CFNAIDIFF': s, 'rolling_low': low / 100,
                         'ERX': rebound / 100, 'ERX_qualified': qualified,
                         'valid_count': count}, index=s.index)


def publication_schedule(months, sessions, actual_release_dates=None):
    """Historical proxy: next month 28th; usable strictly after release on XNYS.

    actual_release_dates may map observation months to confirmed dates for future
    records. Missing entries retain an explicitly labelled simulated release date.
    """
    months = pd.DatetimeIndex(months)
    sessions = pd.DatetimeIndex(sessions)
    if sessions.tz is not None:
        sessions = sessions.tz_localize(None)
    if not sessions.is_monotonic_increasing or sessions.has_duplicates:
        raise ValueError('sessions must be sorted and unique')
    actual = {pd.Timestamp(k): pd.Timestamp(v).normalize()
              for k, v in (actual_release_dates or {}).items()}
    dates = pd.DatetimeIndex([actual.get(d, d + pd.DateOffset(months=1, day=28)) for d in months])
    if any(r <= d + pd.offsets.MonthEnd(0) for d, r in zip(months, dates)):
        raise ValueError('release must follow observation month')
    positions = sessions.searchsorted(dates, side='right')
    if (positions >= len(sessions)).any():
        raise ValueError('calendar does not extend past every release')
    usable = sessions[positions]
    if not usable.is_monotonic_increasing or usable.has_duplicates:
        raise ValueError('monthly availability dates must increase strictly')
    return pd.DataFrame({'release_date': dates, 'available_date': usable,
                         'release_basis': ['actual' if d in actual else 'proxy_next_month_28' for d in months]}, index=months)


def align_to_sessions(monthly, sessions):
    """Most recent published observation only; never backfill before publication."""
    daily = pd.DataFrame({'date': pd.DatetimeIndex(sessions).as_unit('ns')})
    if not daily.date.is_monotonic_increasing or daily.date.duplicated().any():
        raise ValueError('daily dates must be sorted and unique')
    data = monthly.rename_axis('observation_month').reset_index()
    # Excel datetimes can arrive in microseconds; exchange sessions in ns.
    data['available_date'] = pd.to_datetime(data.available_date).astype('datetime64[ns]')
    if not data.available_date.is_monotonic_increasing or data.available_date.duplicated().any():
        raise ValueError('monthly dates must have increasing availability')
    return pd.merge_asof(daily, data, left_on='date', right_on='available_date',
                         direction='backward', allow_exact_matches=True).set_index('date')


def from_workbook(wb, sessions, settings):
    """Recompute from raw monthly values, retaining the database release schedule.

    Never read cached ERX/flags as inputs. All months (including missing ones)
    must carry explicit release and availability dates. The database schedule
    preserves the audited historical exchange holidays and future actual dates.
    """
    rows = [r for r in wb[settings['source_sheet']].values
            if r and isinstance(r[0], datetime)]
    if not rows:
        raise ValueError('ERX source has no dated monthly rows')
    if any(len(r) < 7 or not isinstance(r[5], datetime)
           or not isinstance(r[6], datetime) for r in rows):
        raise ValueError('Every ERX month requires release and availability dates')
    months = pd.DatetimeIndex([r[0] for r in rows])
    if months.has_duplicates or not months.is_monotonic_increasing:
        raise ValueError('ERX source months must be sorted and unique')
    monthly = calculate_erx(pd.Series([r[1] for r in rows], index=months),
                            settings['window_months'], settings['low_threshold'],
                            settings['rebound_threshold'])
    if not monthly.index.equals(months):
        raise ValueError('Missing calendar month: add an explicit blank value and release dates')
    monthly['release_date'] = pd.to_datetime([r[5] for r in rows])
    monthly['available_date'] = pd.to_datetime([r[6] for r in rows])
    if ((monthly.release_date <= months + pd.offsets.MonthEnd(0)).any()
            or (monthly.available_date <= monthly.release_date).any()):
        raise ValueError('ERX availability must follow release, which must follow observation month')
    return monthly, align_to_sessions(monthly, sessions)
