#!/usr/bin/env python3
"""
monthly_breakout_backtest.py
────────────────────────────
Backtest hypothesis:
  Entry : buy at month-end close when monthly_close > max(high) of prior 3 months
  Exit  : (a) daily close >= 2× entry_price  → TARGET hit
          (b) daily close <  prior week's low → STOP hit (momentum failure)

Universe : market.ohlcv_daily WHERE market = 'indian'
Output   : analysis.monthly_breakout_trades (per-trade), summary to stdout

Usage:
  python monthly_breakout_backtest.py            # run and insert to DB
  python monthly_breakout_backtest.py --dry-run  # print summary, no DB write
"""

import os
import logging
import argparse
from datetime import date, timedelta

import numpy as np
import pandas as pd
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

MARKET        = "indian"
TARGET_MULT   = 2.0    # 2x = 100% profit
LOOKBACK_MONTHS = 3    # prior months for breakout check


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


def fetch_daily(ch) -> pd.DataFrame:
    log.info("Fetching daily OHLCV for market='%s'...", MARKET)
    df = ch.query_df(
        f"SELECT symbol, date, open, high, low, close "
        f"FROM market.ohlcv_daily FINAL "
        f"WHERE market = '{MARKET}' AND close > 0 "
        f"ORDER BY symbol, date"
    )
    df["date"] = pd.to_datetime(df["date"])
    log.info("  %d rows, %d symbols", len(df), df["symbol"].nunique())
    return df


def _prior_week_low(daily: pd.DataFrame) -> pd.Series:
    """
    For each row, the minimum low of the prior calendar week (Mon–Fri).
    Calendar week is ISO week (Monday start). Prior week = current_week - 1.
    Forward-filled so every day has a valid prior-week low after the first week.
    """
    daily = daily.copy()
    daily["iso_year"] = daily["date"].dt.isocalendar().year.astype(int)
    daily["iso_week"] = daily["date"].dt.isocalendar().week.astype(int)

    weekly_low = (
        daily.groupby(["iso_year", "iso_week"])["low"]
        .min()
        .rename("week_low")
        .reset_index()
    )
    # Build a lookup: for each (year, week) → prior week's low
    weekly_low["prior_year"] = weekly_low["iso_year"]
    weekly_low["prior_week"] = weekly_low["iso_week"]

    # Shift by 1 week to get prior week's low
    weekly_low = weekly_low.sort_values(["iso_year", "iso_week"]).reset_index(drop=True)
    weekly_low["prior_week_low"] = weekly_low["week_low"].shift(1)

    # Map back to daily rows
    daily = daily.merge(
        weekly_low[["iso_year", "iso_week", "prior_week_low"]],
        on=["iso_year", "iso_week"],
        how="left",
    )
    return daily["prior_week_low"].ffill()


def backtest_symbol(sym_daily: pd.DataFrame) -> list[dict]:
    """
    Run the strategy on a single symbol's daily data.
    Returns list of trade dicts.
    """
    sym_daily = sym_daily.sort_values("date").reset_index(drop=True)

    # ── Monthly aggregation ───────────────────────────────────────────────────
    sym_daily["month_end"] = sym_daily["date"].dt.to_period("M")
    monthly = (
        sym_daily.groupby("month_end")
        .agg(
            month_close=("close", "last"),
            month_high=("high", "max"),
            month_low=("low", "min"),
            month_date=("date", "last"),   # actual last trading day of month
        )
        .reset_index()
        .sort_values("month_end")
    )

    # Prior 3-month high: rolling max of month_high shifted by 1 (exclude current month)
    monthly["prior_3m_high"] = monthly["month_high"].shift(1).rolling(LOOKBACK_MONTHS).max()

    # Entry signal: monthly close > prior 3-month high
    monthly["entry_signal"] = monthly["month_close"] > monthly["prior_3m_high"]

    # ── Prior week low on daily ───────────────────────────────────────────────
    sym_daily["prior_week_low"] = _prior_week_low(sym_daily)

    # Index daily by date for fast lookup
    sym_daily = sym_daily.set_index("date")

    trades = []
    in_position = False
    entry_price = 0.0
    entry_date  = None

    for _, row in monthly.iterrows():
        if pd.isna(row["prior_3m_high"]):
            continue

        month_last_date = pd.Timestamp(row["month_date"])

        # ── Check exit for open position (scan days in this month) ────────────
        if in_position:
            month_days = sym_daily[
                (sym_daily.index >= month_last_date.replace(day=1)) &
                (sym_daily.index <= month_last_date)
            ]
            for day_date, day in month_days.iterrows():
                # Target: 2x
                if day["close"] >= TARGET_MULT * entry_price:
                    trades.append({
                        "entry_date":      entry_date.date(),
                        "exit_date":       day_date.date(),
                        "entry_price":     entry_price,
                        "exit_price":      float(day["close"]),
                        "exit_reason":     "target",
                        "holding_days":    (day_date - entry_date).days,
                        "return_pct":      (day["close"] / entry_price - 1) * 100,
                    })
                    in_position = False
                    break
                # Stop: daily close < prior week low
                pwl = day["prior_week_low"]
                if not pd.isna(pwl) and day["close"] < pwl:
                    trades.append({
                        "entry_date":      entry_date.date(),
                        "exit_date":       day_date.date(),
                        "entry_price":     entry_price,
                        "exit_price":      float(day["close"]),
                        "exit_reason":     "stop",
                        "holding_days":    (day_date - entry_date).days,
                        "return_pct":      (day["close"] / entry_price - 1) * 100,
                    })
                    in_position = False
                    break

        # ── Check entry signal (only if not in position) ──────────────────────
        if not in_position and row["entry_signal"]:
            if month_last_date in sym_daily.index:
                entry_price = float(sym_daily.loc[month_last_date, "close"])
                entry_date  = month_last_date
                in_position = True

    return trades


def _xirr(entry_date: date, exit_date: date, return_pct: float) -> float:
    """
    Annualize the trade return. Capped at ±10x (1000%) to avoid explosion
    on very short holding periods (e.g. 5-day trade at +3% → 650% annualized).
    Minimum holding: 7 days before annualizing; shorter trades use 7d floor.
    """
    holding_days = max((exit_date - entry_date).days, 7)
    holding_years = holding_days / 365.25
    growth = 1 + return_pct / 100
    if growth <= 0:
        return -1.0
    raw = growth ** (1 / holding_years) - 1
    return max(-1.0, min(raw, 10.0))  # cap at ±1000%


def compute_sharpe(trades_df: pd.DataFrame) -> float:
    """
    Monthly Sharpe on a $1-per-trade equal-weight portfolio.
    Maps each trade's return to its exit month, averages returns in months
    with multiple exits, fills zero for months with no exit.
    """
    if trades_df.empty:
        return float("nan")

    trades_df = trades_df.copy()
    trades_df["exit_month"] = pd.to_datetime(trades_df["exit_date"]).dt.to_period("M")

    monthly_ret = trades_df.groupby("exit_month")["return_pct"].mean()

    # Fill months with no trade as 0
    all_months = pd.period_range(
        start=monthly_ret.index.min(),
        end=monthly_ret.index.max(),
        freq="M",
    )
    monthly_ret = monthly_ret.reindex(all_months, fill_value=0.0)

    mean_r = monthly_ret.mean()
    std_r  = monthly_ret.std()
    if std_r == 0:
        return float("nan")
    return (mean_r / std_r) * (12 ** 0.5)   # annualized


def run_backtest(ch) -> pd.DataFrame:
    daily = fetch_daily(ch)
    symbols = daily["symbol"].unique()
    log.info("Running backtest on %d symbols...", len(symbols))

    all_trades = []
    for i, sym in enumerate(symbols, 1):
        sym_data = daily[daily["symbol"] == sym].copy()
        trades = backtest_symbol(sym_data)
        for t in trades:
            t["symbol"] = sym
        all_trades.extend(trades)
        if i % 50 == 0:
            log.info("  [%d/%d] %d trades so far", i, len(symbols), len(all_trades))

    if not all_trades:
        log.warning("No trades generated")
        return pd.DataFrame()

    df = pd.DataFrame(all_trades)
    df["xirr_annualized"] = df.apply(
        lambda r: _xirr(r["entry_date"], r["exit_date"], r["return_pct"]), axis=1
    )
    return df


def print_summary(df: pd.DataFrame):
    if df.empty:
        log.info("No trades to summarize")
        return

    n          = len(df)
    n_target   = (df["exit_reason"] == "target").sum()
    n_stop     = (df["exit_reason"] == "stop").sum()
    win_rate   = n_target / n * 100
    avg_ret    = df["return_pct"].mean()
    med_ret    = df["return_pct"].median()
    avg_hold   = df["holding_days"].mean()
    avg_xirr   = df["xirr_annualized"].mean() * 100
    sharpe     = compute_sharpe(df)

    target_df  = df[df["exit_reason"] == "target"]
    stop_df    = df[df["exit_reason"] == "stop"]

    log.info("═" * 55)
    log.info("  MONTHLY BREAKOUT BACKTEST RESULTS")
    log.info("═" * 55)
    log.info("  Total trades     : %d", n)
    log.info("  Target hit (2x)  : %d  (%.1f%%)", n_target, win_rate)
    log.info("  Stop hit         : %d  (%.1f%%)", n_stop, 100 - win_rate)
    log.info("  Avg return       : %.2f%%", avg_ret)
    log.info("  Median return    : %.2f%%", med_ret)
    log.info("  Avg hold (days)  : %.0f", avg_hold)
    log.info("  Avg XIRR         : %.1f%%", avg_xirr)
    log.info("  Sharpe (monthly) : %.2f", sharpe)
    log.info("  ─" * 28)
    if not target_df.empty:
        log.info("  Target trades — avg hold: %.0fd  avg XIRR: %.1f%%",
                 target_df["holding_days"].mean(),
                 target_df["xirr_annualized"].mean() * 100)
    if not stop_df.empty:
        log.info("  Stop trades   — avg hold: %.0fd  avg ret:  %.2f%%",
                 stop_df["holding_days"].mean(),
                 stop_df["return_pct"].mean())
    log.info("  Date range       : %s → %s",
             df["entry_date"].min(), df["entry_date"].max())
    log.info("═" * 55)


def insert_results(ch, df: pd.DataFrame):
    log.info("Inserting %d trade records...", len(df))
    rows = []
    today = date.today()
    for _, r in df.iterrows():
        rows.append([
            r["symbol"],
            r["entry_date"],
            r["exit_date"],
            float(r["entry_price"]),
            float(r["exit_price"]),
            r["exit_reason"],
            int(r["holding_days"]),
            float(r["return_pct"]),
            float(r["xirr_annualized"]),
            today,
        ])
    ch.insert(
        "analysis.monthly_breakout_trades",
        rows,
        column_names=[
            "symbol", "entry_date", "exit_date", "entry_price", "exit_price",
            "exit_reason", "holding_days", "return_pct", "xirr_annualized", "run_date",
        ],
    )
    log.info("  Done")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print results only, no DB write")
    args = parser.parse_args()

    ch = get_ch()

    # Apply migration if table doesn't exist
    ch.command("""
        CREATE TABLE IF NOT EXISTS analysis.monthly_breakout_trades
        (
            symbol          String,
            entry_date      Date,
            exit_date       Date,
            entry_price     Float64,
            exit_price      Float64,
            exit_reason     String,
            holding_days    Int32,
            return_pct      Float64,
            xirr_annualized Float64,
            run_date        Date DEFAULT today()
        )
        ENGINE = ReplacingMergeTree(run_date)
        ORDER BY (symbol, entry_date)
    """)

    df = run_backtest(ch)
    print_summary(df)

    if not args.dry_run and not df.empty:
        insert_results(ch, df)
    elif args.dry_run:
        log.info("Dry-run — skipping DB insert")


if __name__ == "__main__":
    main()
