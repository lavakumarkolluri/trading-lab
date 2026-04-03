#!/usr/bin/env python3
"""
mf_compute_technical.py
───────────────────────
Technical indicator computations for MF NAV enrichment.

All functions are pure — they take a pandas Series/DataFrame
and return a Series/DataFrame. No side effects, no DB calls.
"""

import numpy as np
import pandas as pd


# ══════════════════════════════════════════════════════
# Moving averages
# ══════════════════════════════════════════════════════

def compute_sma(series: pd.Series, window: int) -> pd.Series:
    """Simple Moving Average. NaN until `window` rows available."""
    return series.rolling(window=window, min_periods=window).mean()


def compute_ema(series: pd.Series, span: int) -> pd.Series:
    """
    Exponential Moving Average — Wilder's method (adjust=False).
    alpha = 2/(span+1) applied iteratively.
    """
    return series.ewm(span=span, adjust=False, min_periods=span).mean()


# ══════════════════════════════════════════════════════
# Oscillators
# ══════════════════════════════════════════════════════

def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    Wilder's RSI.
    RSI = 100 - 100 / (1 + avg_gain / avg_loss)
    Uses Wilder's EMA smoothing (alpha = 1/period).
    Returns 0 where undefined (insufficient data).
    """
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(0)


def compute_atr_proxy(series: pd.Series, period: int = 14) -> pd.Series:
    """
    ATR proxy for MF (no intraday OHLC available).
    Defined as: 14-day rolling mean of |daily NAV change|.
    Higher value = higher absolute rupee volatility per day.
    Use atr_14 / nav * 100 for percentage volatility.
    """
    return series.diff().abs().rolling(window=period, min_periods=period).mean().fillna(0)


# ══════════════════════════════════════════════════════
# Range indicators
# ══════════════════════════════════════════════════════

def compute_52w_range(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Trailing 252-trading-day high and low."""
    high = series.rolling(window=252, min_periods=1).max()
    low  = series.rolling(window=252, min_periods=1).min()
    return high, low


# ══════════════════════════════════════════════════════
# Return / high-water metrics
# ══════════════════════════════════════════════════════

def compute_ytd_return(df: pd.DataFrame) -> pd.Series:
    """
    YTD return % = (nav - first_nav_of_year) / first_nav_of_year * 100.
    Resets each January 1.
    """
    df        = df.copy()
    df["_yr"] = pd.to_datetime(df["date"]).dt.year
    yr_first  = df.groupby("_yr")["nav"].transform("first")
    return ((df["nav"] - yr_first) / yr_first.replace(0, np.nan) * 100).fillna(0)


def compute_all_time_high(series: pd.Series) -> pd.Series:
    """Cumulative maximum NAV from inception."""
    return series.cummax()


def compute_drawdown(nav: pd.Series, ath: pd.Series) -> pd.Series:
    """% below all-time high. 0 when nav == ath."""
    return ((ath - nav) / ath.replace(0, np.nan) * 100).fillna(0)


# ══════════════════════════════════════════════════════
# Rollups
# ══════════════════════════════════════════════════════

def compute_weekly_rollup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add week_start, week_open, week_high, week_low,
    week_close, week_return_pct columns to df.
    """
    df              = df.copy()
    df["_date_ts"]  = pd.to_datetime(df["date"])
    df["week_start"] = df["_date_ts"].dt.to_period("W").apply(
        lambda p: p.start_time.date()
    )
    weekly = df.groupby("week_start")["nav"].agg(
        week_open="first", week_high="max",
        week_low="min",   week_close="last"
    ).reset_index()
    weekly["week_return_pct"] = (
        (weekly["week_close"] - weekly["week_open"])
        / weekly["week_open"].replace(0, np.nan) * 100
    ).fillna(0)
    df = df.merge(weekly, on="week_start", how="left")
    df.drop(columns=["_date_ts"], inplace=True)
    return df


def compute_monthly_rollup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add month_start, month_open, month_high, month_low,
    month_close, month_return_pct columns to df.
    """
    df               = df.copy()
    df["_date_ts"]   = pd.to_datetime(df["date"])
    df["month_start"] = df["_date_ts"].dt.to_period("M").apply(
        lambda p: p.start_time.date()
    )
    monthly = df.groupby("month_start")["nav"].agg(
        month_open="first", month_high="max",
        month_low="min",   month_close="last"
    ).reset_index()
    monthly["month_return_pct"] = (
        (monthly["month_close"] - monthly["month_open"])
        / monthly["month_open"].replace(0, np.nan) * 100
    ).fillna(0)
    df = df.merge(monthly, on="month_start", how="left")
    df.drop(columns=["_date_ts"], inplace=True)
    return df


# ══════════════════════════════════════════════════════
# Orchestrator
# ══════════════════════════════════════════════════════

def compute_all_technical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all technical indicators on a sorted NAV DataFrame.
    Input  : df with columns [date, nav] sorted by date ascending.
    Returns: df with all technical columns added.
    """
    df = df.copy().sort_values("date").reset_index(drop=True)

    df["sma_20"]  = compute_sma(df["nav"], 20).fillna(0)
    df["sma_50"]  = compute_sma(df["nav"], 50).fillna(0)
    df["sma_200"] = compute_sma(df["nav"], 200).fillna(0)
    df["ema_20"]  = compute_ema(df["nav"], 20).fillna(0)
    df["ema_50"]  = compute_ema(df["nav"], 50).fillna(0)
    df["rsi_14"]  = compute_rsi(df["nav"], 14)
    df["atr_14"]  = compute_atr_proxy(df["nav"], 14)

    df["high_52w"], df["low_52w"] = compute_52w_range(df["nav"])
    df["ytd_return_pct"] = compute_ytd_return(df)
    df["all_time_high"]  = compute_all_time_high(df["nav"])
    df["drawdown_pct"]   = compute_drawdown(df["nav"], df["all_time_high"])

    df = compute_weekly_rollup(df)
    df = compute_monthly_rollup(df)

    return df
