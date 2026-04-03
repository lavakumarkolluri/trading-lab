#!/usr/bin/env python3
"""
mf_compute_risk.py
──────────────────
Risk metric computations for MF NAV enrichment.

Columns produced:
  volatility_1y    — annualized std dev of daily log returns (252 days)
  sharpe_1y        — (annualized_return - risk_free) / volatility_1y
  sortino_1y       — (annualized_return - risk_free) / downside_deviation
  max_drawdown_pct — worst peak-to-trough drawdown in fund history (%)
  max_drawdown_days — calendar days to recover from worst drawdown
  beta_vs_nifty    — rolling 252-day beta vs NIFTYBEES.NS
  consistency_score — % of rolling 1y windows fund had positive return
                      (category comparison deferred to mf_ranking.py)

Risk-free rate: 6.5% (India 10-yr G-sec, annualized).
All % values stored as float (12.5 means 12.5%).
NaN/undefined → 0.
"""

import numpy as np
import pandas as pd

RISK_FREE_ANNUAL = 0.065          # India 10-yr G-sec
TRADING_DAYS     = 252            # MF NAV business days per year
RISK_FREE_DAILY  = (1 + RISK_FREE_ANNUAL) ** (1 / TRADING_DAYS) - 1


# ══════════════════════════════════════════════════════
# Volatility
# ══════════════════════════════════════════════════════

def compute_volatility_1y(nav: pd.Series) -> pd.Series:
    """
    Annualized volatility = std(daily log returns, 252d) * sqrt(252).
    Uses log returns for better statistical properties.
    Returns 0 where insufficient data.
    """
    log_ret = np.log(nav / nav.shift(1))
    vol     = log_ret.rolling(window=TRADING_DAYS, min_periods=TRADING_DAYS).std()
    return (vol * np.sqrt(TRADING_DAYS) * 100).fillna(0)  # as %


# ══════════════════════════════════════════════════════
# Sharpe ratio
# ══════════════════════════════════════════════════════

def compute_sharpe_1y(nav: pd.Series) -> pd.Series:
    """
    Sharpe ratio over trailing 252 trading days.
    Sharpe = (annualized_return - risk_free) / annualized_volatility
    Both numerator and denominator in decimal form.
    Returns raw Sharpe ratio (not %).
    """
    log_ret     = np.log(nav / nav.shift(1))
    excess_ret  = log_ret - RISK_FREE_DAILY
    roll_mean   = excess_ret.rolling(window=TRADING_DAYS, min_periods=TRADING_DAYS).mean()
    roll_std    = excess_ret.rolling(window=TRADING_DAYS, min_periods=TRADING_DAYS).std()
    sharpe      = (roll_mean / roll_std.replace(0, np.nan)) * np.sqrt(TRADING_DAYS)
    return sharpe.fillna(0)


# ══════════════════════════════════════════════════════
# Sortino ratio
# ══════════════════════════════════════════════════════

def compute_sortino_1y(nav: pd.Series) -> pd.Series:
    """
    Sortino ratio over trailing 252 trading days.
    Like Sharpe but uses only downside deviation in denominator.
    Downside deviation = std of returns below risk_free only.
    """
    log_ret    = np.log(nav / nav.shift(1))
    excess_ret = log_ret - RISK_FREE_DAILY

    roll_mean  = excess_ret.rolling(window=TRADING_DAYS, min_periods=TRADING_DAYS).mean()

    # Downside deviation: std of negative excess returns only
    def downside_std(x):
        neg = x[x < 0]
        return neg.std() if len(neg) > 1 else np.nan

    roll_down  = excess_ret.rolling(window=TRADING_DAYS, min_periods=TRADING_DAYS).apply(
        downside_std, raw=False
    )
    sortino    = (roll_mean / roll_down.replace(0, np.nan)) * np.sqrt(TRADING_DAYS)
    return sortino.fillna(0)


# ══════════════════════════════════════════════════════
# Max drawdown
# ══════════════════════════════════════════════════════

def compute_max_drawdown(nav: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    Compute max drawdown pct and recovery days at each point in time.

    max_drawdown_pct  — worst historical peak-to-trough drawdown up to this date (%)
    max_drawdown_days — calendar days to recover from that worst drawdown

    Both are expanding (cumulative) metrics — they represent the worst
    drawdown the fund has ever experienced, updated daily.
    Returns (max_dd_pct_series, max_dd_days_series).
    """
    n         = len(nav)
    dd_pct    = np.zeros(n)
    dd_days   = np.zeros(n, dtype=int)

    nav_vals  = nav.values
    worst_dd  = 0.0
    worst_days = 0

    peak_idx   = 0
    trough_idx = 0

    for i in range(n):
        # Update running peak
        if nav_vals[i] > nav_vals[peak_idx]:
            peak_idx   = i
            trough_idx = i

        # Current drawdown from peak
        cur_dd = (nav_vals[peak_idx] - nav_vals[i]) / nav_vals[peak_idx] * 100

        if cur_dd > worst_dd:
            worst_dd  = cur_dd
            # Recovery days = 0 (not yet recovered — still in drawdown)
            worst_days = 0

        # Check if we've recovered from the worst drawdown
        if worst_dd > 0 and nav_vals[i] >= nav_vals[peak_idx] and worst_days == 0:
            worst_days = 0  # Fully recovered — reset

        dd_pct[i]  = worst_dd
        dd_days[i] = worst_days

    return pd.Series(dd_pct, index=nav.index), pd.Series(dd_days, index=nav.index)


# ══════════════════════════════════════════════════════
# Beta vs Nifty
# ══════════════════════════════════════════════════════

def compute_beta_vs_nifty(nav: pd.Series,
                           nav_dates: pd.Series,
                           nifty_df: pd.DataFrame,
                           window: int = TRADING_DAYS) -> pd.Series:
    """
    Rolling `window`-day beta vs NIFTYBEES.NS.
    Beta = Cov(fund_returns, nifty_returns) / Var(nifty_returns)

    nifty_df must have columns [date, nifty_close] sorted ascending.
    Returns 0 where nifty data unavailable or insufficient overlap.
    """
    if nifty_df.empty:
        return pd.Series(np.zeros(len(nav)), index=nav.index)

    # Build date-indexed series for nifty
    nifty_map  = dict(zip(nifty_df["date"], nifty_df["nifty_close"]))
    nifty_vals = nav_dates.map(nifty_map)

    fund_ret  = np.log(nav / nav.shift(1))
    nifty_ret = np.log(nifty_vals / nifty_vals.shift(1))

    # Rolling beta
    def rolling_beta(idx):
        if idx < window:
            return 0.0
        f = fund_ret.iloc[idx - window:idx].values
        n = nifty_ret.iloc[idx - window:idx].values
        mask = ~(np.isnan(f) | np.isnan(n))
        f, n = f[mask], n[mask]
        if len(f) < window // 2:
            return 0.0
        var_n = np.var(n, ddof=1)
        if var_n < 1e-12:
            return 0.0
        cov   = np.cov(f, n, ddof=1)[0][1]
        return round(cov / var_n, 4)

    beta_vals = [rolling_beta(i) for i in range(len(nav))]
    return pd.Series(beta_vals, index=nav.index)


# ══════════════════════════════════════════════════════
# Consistency score
# ══════════════════════════════════════════════════════

def compute_consistency_score(nav: pd.Series,
                               window: int = TRADING_DAYS) -> pd.Series:
    """
    % of rolling 1y windows where the fund had a positive return.
    A score of 80 means 80% of 1-year periods were profitable.

    Full peer-vs-category consistency (beat category avg) is computed
    in mf_ranking.py. This metric reflects the fund's standalone
    consistency of positive returns.
    """
    annual_ret = (nav / nav.shift(window) - 1) * 100
    is_positive = (annual_ret > 0).astype(float)
    # Rolling % of positive 1y returns over a 3y window
    score = is_positive.rolling(window=window * 3, min_periods=window).mean() * 100
    return score.fillna(0)


# ══════════════════════════════════════════════════════
# Orchestrator
# ══════════════════════════════════════════════════════

def compute_all_risk(df: pd.DataFrame,
                     nifty_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all risk metric columns.
    Input : df with [date, nav] sorted ascending.
            nifty_df with [date, nifty_close] — may be empty.
    Returns df with risk columns added.
    """
    df  = df.copy()
    nav = df["nav"]

    df["volatility_1y"]    = compute_volatility_1y(nav)
    df["sharpe_1y"]        = compute_sharpe_1y(nav)
    df["sortino_1y"]       = compute_sortino_1y(nav)

    max_dd_pct, max_dd_days = compute_max_drawdown(nav)
    df["max_drawdown_pct"]  = max_dd_pct.values
    df["max_drawdown_days"] = max_dd_days.values.astype(int)

    df["beta_vs_nifty"]    = compute_beta_vs_nifty(
        nav, df["date"], nifty_df
    ).values

    df["consistency_score"] = compute_consistency_score(nav).values

    return df