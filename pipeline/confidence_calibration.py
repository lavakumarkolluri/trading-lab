#!/usr/bin/env python3
"""
confidence_calibration.py — Find the confidence threshold that maximises win rate.

For each symbol × strategy × confidence threshold (50→95 in steps of 5):
  - Filter OOS predictions above threshold
  - Show trade count/year, win rate, avg P&L, total P&L

Uses walk-forward out-of-sample predictions (no lookahead) via _walk_forward_cv().

Usage:
    python confidence_calibration.py                    # all symbols + strategies
    python confidence_calibration.py --symbol NIFTY     # one symbol
    python confidence_calibration.py --strategy iron_fly
"""

import argparse
from datetime import date

import numpy as np
import pandas as pd

from ch_utils import ch_client, minio_client
from confidence_scorer import (
    SYMBOLS,
    STRATEGY_TYPES,
    FEATURE_COLS,
    MIN_TRAIN,
    build_dataset,
    _walk_forward_cv,
)


def calibrate(ch, symbol: str, strategy: str) -> pd.DataFrame:
    df = build_dataset(ch, symbol, strategy)
    if df.empty or len(df) < MIN_TRAIN + 5:
        return pd.DataFrame()

    oos = _walk_forward_cv(df, "xgb", FEATURE_COLS)
    if oos.empty:
        return pd.DataFrame()

    oos = oos.copy()
    oos["entry_dt"] = pd.to_datetime(oos["entry_date"])
    date_span_years = (oos["entry_dt"].max() - oos["entry_dt"].min()).days / 365.25
    if date_span_years <= 0:
        return pd.DataFrame()

    rows = []
    for thresh in range(50, 96, 5):
        subset = oos[oos["confidence"] * 100 >= thresh]
        if len(subset) < 3:
            continue
        wins     = int(subset["target"].sum())
        total    = len(subset)
        win_rate = wins / total * 100
        avg_pnl  = float(subset["pnl_pts"].mean()) if "pnl_pts" in subset.columns else float("nan")
        tot_pnl  = float(subset["pnl_pts"].sum())  if "pnl_pts" in subset.columns else float("nan")
        per_yr   = total / date_span_years
        rows.append({
            "symbol":    symbol,
            "strategy":  strategy,
            "threshold": thresh,
            "trades":    total,
            "per_year":  round(per_yr, 1),
            "win_rate":  round(win_rate, 1),
            "avg_pnl":   round(avg_pnl, 1),
            "total_pnl": round(tot_pnl, 1),
        })
    return pd.DataFrame(rows)


def print_table(results: pd.DataFrame):
    if results.empty:
        print("  no data")
        return
    cols = ["threshold", "trades", "per_year", "win_rate", "avg_pnl", "total_pnl"]
    header = f"  {'Thresh':>7}  {'Trades':>7}  {'Per/yr':>7}  {'Win%':>6}  {'AvgP&L':>8}  {'TotalP&L':>10}"
    print(header)
    print("  " + "-" * 60)
    for _, r in results.iterrows():
        flag = " ◀" if r["win_rate"] >= 85 else ""
        print(f"  {int(r['threshold']):>7}  {int(r['trades']):>7}  "
              f"{r['per_year']:>7.1f}  {r['win_rate']:>6.1f}%  "
              f"{r['avg_pnl']:>+8.1f}  {r['total_pnl']:>+10.1f}{flag}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol",   help="Symbol (default: all)")
    parser.add_argument("--strategy", help="Strategy (default: all)")
    args = parser.parse_args()

    ch = ch_client()

    symbols    = [args.symbol]   if args.symbol   else SYMBOLS
    strategies = [args.strategy] if args.strategy else STRATEGY_TYPES

    all_results = []
    for symbol in symbols:
        for strategy in strategies:
            print(f"\n[{symbol}][{strategy}]")
            result = calibrate(ch, symbol, strategy)
            if result.empty:
                print("  insufficient OOS data")
                continue
            print_table(result)
            all_results.append(result)

    if len(all_results) > 1:
        combined = pd.concat(all_results, ignore_index=True)
        best = combined[combined["win_rate"] >= 85].sort_values(
            "per_year", ascending=False
        ).head(10)
        if not best.empty:
            print("\n=== Top configs at ≥85% win rate (by trade frequency) ===")
            for _, r in best.iterrows():
                print(f"  {r['symbol']:<12} {r['strategy']:<16} "
                      f"thresh={int(r['threshold'])}  "
                      f"win={r['win_rate']:.1f}%  "
                      f"{r['per_year']:.1f}/yr  "
                      f"avg={r['avg_pnl']:+.1f}pts")


if __name__ == "__main__":
    main()
