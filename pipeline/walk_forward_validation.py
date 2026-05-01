#!/usr/bin/env python3
"""
walk_forward_validation.py
──────────────────────────
Splits backtest data into train/test halves and checks whether
pattern edge survives on unseen data.

Train : 2024-04-01 → 2024-09-30  (first 6 months)
Test  : 2024-10-01 → present      (remaining ~18 months)

A pattern that works in-sample but collapses out-of-sample is
curve-fitted noise. A pattern that holds roughly steady has a
structural edge worth studying further.

Usage:
  python walk_forward_validation.py
  python walk_forward_validation.py --market indian
  python walk_forward_validation.py --market all --horizon 1d
  python walk_forward_validation.py --fii          # FII flow momentum test

Docker:
  docker compose run --rm pipeline python walk_forward_validation.py
"""

import os
import sys
import argparse
from datetime import date

import clickhouse_connect
import pandas as pd
import numpy as np

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

TRAIN_END = date(2024, 9, 30)
TEST_START = date(2024, 10, 1)


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


# ── Pattern walk-forward ──────────────────────────────────

def compute_stats(df: pd.DataFrame, horizon: str) -> dict:
    ret_col = f"return_{horizon}"
    hit_col = f"hit_2pct_{horizon}"
    if df.empty or ret_col not in df.columns:
        return {}
    n = len(df)
    win_rate = (df[ret_col] > 0).mean() * 100
    avg_ret  = df[ret_col].mean()
    med_ret  = df[ret_col].median()
    hit_rate = df[hit_col].mean() * 100 if hit_col in df.columns else float("nan")
    std      = df[ret_col].std()
    sharpe   = (avg_ret / std * (252 ** 0.5)) if std > 0 else 0.0
    return {
        "n":        n,
        "win_rate": win_rate,
        "avg_ret":  avg_ret,
        "med_ret":  med_ret,
        "hit_2pct": hit_rate,
        "sharpe":   sharpe,
    }


def run_pattern_walkforward(ch, market: str, horizon: str):
    where = "" if market == "all" else f"AND market = '{market}'"
    df = ch.query_df(f"""
        SELECT pattern_id, signal_date, market,
               return_1d, return_3d, return_5d,
               hit_2pct_1d, hit_2pct_3d, hit_2pct_5d
        FROM analysis.backtests
        WHERE 1=1 {where}
    """)
    if df.empty:
        print("No data found.")
        return

    df["signal_date"] = pd.to_datetime(df["signal_date"]).dt.date
    train = df[df["signal_date"] <= TRAIN_END]
    test  = df[df["signal_date"] >= TEST_START]

    patterns = sorted(df["pattern_id"].unique())
    pattern_labels = _fetch_labels(ch)

    print(f"\n{'='*90}")
    print(f"  WALK-FORWARD VALIDATION  |  horizon={horizon}  market={market}")
    print(f"  Train: 2024-04-01 → {TRAIN_END}   Test: {TEST_START} → present")
    print(f"{'='*90}")

    header = (f"{'Pattern':<8} {'Label':<30} "
              f"{'Train N':>7} {'Tr WR%':>7} {'Tr Ret%':>8} {'Tr Shrp':>7}  "
              f"{'Test N':>7} {'Te WR%':>7} {'Te Ret%':>8} {'Te Shrp':>7}  "
              f"{'Edge?':>6}")
    print(header)
    print("-" * 90)

    for pid in patterns:
        label = pattern_labels.get(pid, "")[:29]
        tr = compute_stats(train[train["pattern_id"] == pid], horizon)
        te = compute_stats(test[test["pattern_id"] == pid], horizon)
        if not tr or not te:
            continue

        # Edge flag: win_rate > 50% and avg_ret > 0 on BOTH halves, Sharpe not collapsed >50%
        edge = (
            tr["win_rate"] > 50 and te["win_rate"] > 50 and
            tr["avg_ret"] > 0 and te["avg_ret"] > 0 and
            te["sharpe"] >= tr["sharpe"] * 0.5
        )
        flag = "YES" if edge else "---"

        print(f"{pid:<8} {label:<30} "
              f"{tr['n']:>7} {tr['win_rate']:>7.1f} {tr['avg_ret']:>8.2f} {tr['sharpe']:>7.2f}  "
              f"{te['n']:>7} {te['win_rate']:>7.1f} {te['avg_ret']:>8.2f} {te['sharpe']:>7.2f}  "
              f"{flag:>6}")

    print(f"\nNote: Win Rate% = % of trades with positive 5-day return")
    print(f"      Ret% = average return over {horizon} horizon (raw price %)")
    print(f"      Sharpe = annualized Sharpe of per-trade returns")
    print(f"      Edge? = win_rate>50% + positive return + Sharpe not halved vs train\n")


def _fetch_labels(ch) -> dict:
    try:
        rows = ch.query("SELECT pattern_id, label FROM analysis.patterns").result_rows
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


# ── FII flow momentum ─────────────────────────────────────

def run_fii_momentum(ch):
    """
    Test: does sustained FII buying predict Nifty returns?

    For each trading day d in market.fii_dii, compute:
      - fii_3d_sum: sum of FII net equity over past 3 days
      - next Nifty return: 5d and 10d forward returns from market.prices
    """
    # Load FII net flow per day (entity='FII', net_value in crores)
    fii_df = ch.query_df("""
        SELECT date, net_value AS fii_net_equity
        FROM market.fii_dii
        WHERE entity = 'FII'
        ORDER BY date
    """)
    if fii_df.empty:
        print("No FII data found.")
        return

    fii_df["date"] = pd.to_datetime(fii_df["date"]).dt.date

    # Load Nifty prices — use NIFTYBEES ETF as proxy (tracks Nifty 50)
    nifty_df = None
    for sym in ("NIFTYBEES.NS", "^NSEI", "NIFTY"):
        try:
            r = ch.query_df(f"""
                SELECT date, close
                FROM market.ohlcv_daily
                WHERE symbol = '{sym}'
                ORDER BY date
            """)
            if not r.empty:
                nifty_df = r
                print(f"Using Nifty proxy: {sym}")
                break
        except Exception:
            continue

    if nifty_df is None or nifty_df.empty:
        # Try to get index data from a different source
        print("Nifty price data not found in market.prices — using MF index as proxy")
        _run_fii_fii_only(fii_df)
        return

    nifty_df["date"] = pd.to_datetime(nifty_df["date"]).dt.date
    nifty_df = nifty_df.set_index("date").sort_index()

    # Compute rolling FII sums
    fii_df = fii_df.set_index("date").sort_index()
    fii_df["fii_3d"]  = fii_df["fii_net_equity"].rolling(3).sum()
    fii_df["fii_5d"]  = fii_df["fii_net_equity"].rolling(5).sum()
    fii_df["fii_10d"] = fii_df["fii_net_equity"].rolling(10).sum()

    # Forward Nifty returns
    nifty_df["ret_5d"]  = nifty_df["close"].pct_change(5).shift(-5) * 100
    nifty_df["ret_10d"] = nifty_df["close"].pct_change(10).shift(-10) * 100
    nifty_df["ret_20d"] = nifty_df["close"].pct_change(20).shift(-20) * 100

    merged = fii_df.join(nifty_df[["ret_5d", "ret_10d", "ret_20d"]], how="inner").dropna()
    print(f"\nMerged FII + Nifty rows: {len(merged)}")

    print(f"\n{'='*70}")
    print(f"  FII FLOW MOMENTUM EDGE TEST")
    print(f"  FII data: {fii_df.index.min()} → {fii_df.index.max()}")
    print(f"{'='*70}\n")

    for flow_col, flow_label in [("fii_3d", "3-day FII net"), ("fii_5d", "5-day FII net"), ("fii_10d", "10-day FII net")]:
        print(f"  Signal: {flow_label} (crores)")
        _bucket_analysis(merged, flow_col, ["ret_5d", "ret_10d", "ret_20d"])
        print()


def _bucket_analysis(df: pd.DataFrame, signal_col: str, return_cols: list):
    """Split signal into buy/neutral/sell buckets and show forward returns."""
    vals = df[signal_col].dropna()
    if len(vals) < 20:
        print("    Insufficient data.")
        return

    # Buckets: strong buy (top 33%), neutral (middle), strong sell (bottom 33%)
    p33, p66 = vals.quantile(0.33), vals.quantile(0.66)

    buy     = df[df[signal_col] >= p66]
    neutral = df[(df[signal_col] > p33) & (df[signal_col] < p66)]
    sell    = df[df[signal_col] <= p33]

    print(f"    {'Bucket':<12} {'N':>5}  ", end="")
    for rc in return_cols:
        print(f"  {rc:>8}", end="")
    print()
    print(f"    {'-'*12} {'-'*5}  " + "  " + "  ".join(["-"*8]*len(return_cols)))

    for label, subset in [("Buy", buy), ("Neutral", neutral), ("Sell", sell)]:
        print(f"    {label:<12} {len(subset):>5}  ", end="")
        for rc in return_cols:
            mean_ret = subset[rc].mean() if not subset.empty else float("nan")
            print(f"  {mean_ret:>8.2f}", end="")
        print()

    # t-test: buy vs sell
    for rc in return_cols:
        if buy[rc].std() > 0 and sell[rc].std() > 0:
            b = buy[rc].dropna()
            s = sell[rc].dropna()
            diff = b.mean() - s.mean()
            # Welch t-test via numpy
            se = ((b.var() / len(b)) + (s.var() / len(s))) ** 0.5
            t_stat = diff / se if se > 0 else 0
            # rough p-value estimate using normal approximation (n is small but workable)
            import math
            p_approx = 2 * (1 - 0.5 * (1 + math.erf(abs(t_stat) / math.sqrt(2))))
            sig = "**" if p_approx < 0.05 else ("*" if p_approx < 0.10 else "")
            print(f"    Buy-Sell diff {rc}: {diff:+.2f}%  p≈{p_approx:.3f} {sig}")


def _run_fii_fii_only(fii_df: pd.DataFrame):
    """Print basic FII stats when no Nifty data available."""
    print(f"FII data range: {fii_df['date'].min()} → {fii_df['date'].max()}")
    print(f"Total days: {len(fii_df)}")
    print(f"Mean daily FII net equity: {fii_df['fii_net_equity'].mean():.0f} crores")
    print(f"Days positive: {(fii_df['fii_net_equity']>0).sum()}")
    print(f"Days negative: {(fii_df['fii_net_equity']<0).sum()}")
    print("\nNeed market.prices with Nifty data for forward return analysis.")


# ── CLI ───────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Walk-forward validation of trading patterns"
    )
    parser.add_argument("--market", default="indian",
                        choices=["indian", "bse", "us", "crypto", "nasdaq100", "all"],
                        help="Market to analyse (default: indian)")
    parser.add_argument("--horizon", default="5d",
                        choices=["1d", "3d", "5d"],
                        help="Return horizon (default: 5d)")
    parser.add_argument("--fii", action="store_true",
                        help="Run FII flow momentum edge test instead")
    args = parser.parse_args()

    ch = get_ch()

    if args.fii:
        run_fii_momentum(ch)
    else:
        run_pattern_walkforward(ch, args.market, args.horizon)


if __name__ == "__main__":
    main()
