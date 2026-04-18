#!/usr/bin/env python3
"""
mf_compute_returns.py
─────────────────────
Return and CAGR computations for MF NAV enrichment.

Columns produced (all Float64, % values):
  return_1m   — 1-month absolute return
  return_3m   — 3-month absolute return
  return_6m   — 6-month absolute return
  return_1y   — 1-year absolute return
  return_3y   — 3-year absolute return
  return_5y   — 5-year absolute return
  return_10y  — 10-year absolute return
  cagr_1y     — 1-year CAGR (annualized)
  cagr_3y     — 3-year CAGR (annualized)
  cagr_5y     — 5-year CAGR (annualized)
  cagr_10y    — 10-year CAGR (annualized)
  sip_return_1y — XIRR on ₹1000/month SIP for 1 year
  sip_return_3y — XIRR on ₹1000/month SIP for 3 years
  sip_return_5y — XIRR on ₹1000/month SIP for 5 years

All values stored as percentage (e.g. 12.5 means 12.5%).
NaN/undefined periods → 0.
"""

import numpy as np
import pandas as pd

try:
    import numpy_financial as npf
    _HAS_NPF = True
except ImportError:
    _HAS_NPF = False


# ══════════════════════════════════════════════════════
# Absolute return
# ══════════════════════════════════════════════════════

def _approx_trading_days(calendar_days: int) -> int:
    """Convert calendar days to approximate trading days (MF publishes ~252/year)."""
    return max(1, int(calendar_days * 252 / 365))


def compute_period_return(nav: pd.Series, calendar_days: int) -> pd.Series:
    """
    Absolute return over `calendar_days` calendar days.
    return = (nav_t - nav_{t-N}) / nav_{t-N} * 100
    Uses trading-day shift (MF NAV only on business days).
    Returns 0 where lookback data unavailable.
    """
    shift = _approx_trading_days(calendar_days)
    nav_past = nav.shift(shift)
    ret      = (nav - nav_past) / nav_past.replace(0, np.nan) * 100
    return ret.fillna(0)


# ══════════════════════════════════════════════════════
# CAGR
# ══════════════════════════════════════════════════════

def compute_cagr(nav: pd.Series, years: int) -> pd.Series:
    """
    Compound Annual Growth Rate over `years` years.
    CAGR = (nav_t / nav_{t-N})^(1/years) - 1  expressed as %.
    Returns 0 where lookback data unavailable or nav_past <= 0.
    """
    shift    = _approx_trading_days(years * 365)
    nav_past = nav.shift(shift)
    ratio    = nav / nav_past.replace(0, np.nan)
    cagr     = (ratio ** (1 / years) - 1) * 100
    return cagr.fillna(0)


# ══════════════════════════════════════════════════════
# SIP XIRR
# ══════════════════════════════════════════════════════

def _xirr_newton(cashflows: list[float], dates: list,
                 guess: float = 0.1, tol: float = 1e-6,
                 max_iter: int = 100) -> float:
    """
    Newton-Raphson XIRR — works without numpy_financial.
    cashflows: list of floats (negative = outflow, positive = inflow)
    dates    : list of datetime.date objects
    Returns annualized rate as decimal (0.12 = 12%).
    Returns 0.0 on failure to converge.
    """
    if not dates:
        return 0.0
    t0 = dates[0]

    def year_fracs():
        return [(d - t0).days / 365.0 for d in dates]

    fracs = year_fracs()

    rate = guess
    for _ in range(max_iter):
        # BUG-005: guard against invalid domain before exponentiation.
        # (1 + rate) must be positive; rate <= -1 causes ZeroDivisionError
        # or complex results with fractional exponents.
        if rate <= -1:
            return 0.0
        try:
            with np.errstate(over="raise", invalid="raise"):
                npv  = sum(cf / (1 + rate) ** t for cf, t in zip(cashflows, fracs))
                dnpv = sum(-t * cf / (1 + rate) ** (t + 1)
                           for cf, t in zip(cashflows, fracs))
        except (ZeroDivisionError, OverflowError, FloatingPointError):
            return 0.0
        if abs(dnpv) < 1e-12:
            return 0.0
        rate_new = rate - npv / dnpv
        if abs(rate_new - rate) < tol:
            return rate_new if -1 < rate_new < 10 else 0.0
        rate = rate_new
    return 0.0


def _compute_sip_xirr_for_row(nav_series: pd.Series,
                               date_series: pd.Series,
                               end_idx: int,
                               months: int,
                               sip_amount: float = 1000.0) -> float:
    """
    Compute XIRR for a SIP ending at end_idx.

    Logic:
      - Monthly SIP of sip_amount invested on the 1st trading day of each month
      - For each SIP date, units purchased = sip_amount / NAV_on_that_date
      - Redemption value = total_units * NAV_at_end_idx
      - XIRR on the cashflow stream
    Returns annualized XIRR as % (e.g. 14.5 for 14.5%).
    """
    end_date = date_series.iloc[end_idx]
    end_nav  = nav_series.iloc[end_idx]

    # Build a date→nav lookup for fast access
    nav_map = dict(zip(date_series, nav_series))
    all_dates = sorted(nav_map.keys())

    # Identify monthly SIP dates going back `months` months from end_date
    import calendar
    from datetime import date

    sip_entries = []
    y, m = end_date.year, end_date.month

    for _ in range(months):
        # First available trading date in this month
        month_start = date(y, m, 1)
        # Find the first date >= month_start in our date list
        sip_date = next((d for d in all_dates if d >= month_start), None)
        if sip_date is None or sip_date > end_date:
            pass
        else:
            nav_on_date = nav_map.get(sip_date)
            if nav_on_date and nav_on_date > 0:
                sip_entries.append((sip_date, nav_on_date))

        # Go back one month
        m -= 1
        if m == 0:
            m = 12
            y -= 1

    if not sip_entries:
        return 0.0

    # Total units accumulated
    total_units = sum(sip_amount / nav for _, nav in sip_entries)
    final_value = total_units * end_nav

    # Cashflows: negative on each SIP date, positive at end
    cashflows = []
    dates     = []
    for sip_date, _ in sip_entries:
        cashflows.append(-sip_amount)
        dates.append(sip_date)

    # Final redemption
    cashflows.append(final_value)
    dates.append(end_date)

    # Sort by date
    paired = sorted(zip(dates, cashflows))
    dates_sorted = [p[0] for p in paired]
    cf_sorted    = [p[1] for p in paired]

    if _HAS_NPF:
        try:
            rate = npf.xirr(cf_sorted, dates_sorted)
            if rate is None or not np.isfinite(rate):
                return 0.0
            return round(rate * 100, 4)
        except Exception:
            pass

    rate = _xirr_newton(cf_sorted, dates_sorted)
    return round(rate * 100, 4)


def compute_sip_returns(df: pd.DataFrame,
                        sip_amount: float = 1000.0) -> pd.DataFrame:
    """
    Compute sip_return_1y, sip_return_3y, sip_return_5y for every row.

    For efficiency, computes only every 5th row and forward-fills —
    SIP XIRR changes slowly day-to-day.
    """
    df = df.copy()
    n  = len(df)

    sip_1y  = np.zeros(n)
    sip_3y  = np.zeros(n)
    sip_5y  = np.zeros(n)

    nav_ser  = df["nav"].reset_index(drop=True)
    date_ser = df["date"].reset_index(drop=True)

    # Compute every 5th row, interpolate the rest
    for i in range(n):
        if i % 5 != 0 and i != n - 1:
            continue
        sip_1y[i] = _compute_sip_xirr_for_row(nav_ser, date_ser, i, 12,  sip_amount)
        sip_3y[i] = _compute_sip_xirr_for_row(nav_ser, date_ser, i, 36,  sip_amount)
        sip_5y[i] = _compute_sip_xirr_for_row(nav_ser, date_ser, i, 60,  sip_amount)

    # Forward-fill gaps (every-5 sampling → linear interpolation)
    sip_1y_s = pd.Series(sip_1y).replace(0, np.nan).interpolate().fillna(0)
    sip_3y_s = pd.Series(sip_3y).replace(0, np.nan).interpolate().fillna(0)
    sip_5y_s = pd.Series(sip_5y).replace(0, np.nan).interpolate().fillna(0)

    df["sip_return_1y"] = sip_1y_s.values
    df["sip_return_3y"] = sip_3y_s.values
    df["sip_return_5y"] = sip_5y_s.values
    return df


# ══════════════════════════════════════════════════════
# Orchestrator
# ══════════════════════════════════════════════════════

def compute_all_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all return and CAGR columns.
    Input : df with [date, nav] sorted ascending, already has technical cols.
    Returns df with return/CAGR/SIP columns added.
    """
    df = df.copy()
    nav = df["nav"]

    df["return_1m"]  = compute_period_return(nav, 30)
    df["return_3m"]  = compute_period_return(nav, 91)
    df["return_6m"]  = compute_period_return(nav, 182)
    df["return_1y"]  = compute_period_return(nav, 365)
    df["return_3y"]  = compute_period_return(nav, 3 * 365)
    df["return_5y"]  = compute_period_return(nav, 5 * 365)
    df["return_10y"] = compute_period_return(nav, 10 * 365)

    df["cagr_1y"]    = compute_cagr(nav, 1)
    df["cagr_3y"]    = compute_cagr(nav, 3)
    df["cagr_5y"]    = compute_cagr(nav, 5)
    df["cagr_10y"]   = compute_cagr(nav, 10)

    df = compute_sip_returns(df)

    return df