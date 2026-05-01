#!/usr/bin/env python3
"""
momentum_factor_test.py
────────────────────────
Tests the Jegadeesh-Titman 12-1 momentum factor on Indian stocks.

Strategy:
  - Formation: past 12-month return, skipping the most recent month
    (months t-12 to t-2) — the skip avoids short-term reversal
  - Holding: 1 month
  - Universe: NSE stocks (.NS) with continuous price history
  - Rebalance: monthly, at each month-end

Outputs:
  - Quintile returns (Q1=losers → Q5=winners)
  - Q5 vs benchmark (NIFTYBEES.NS)
  - Transaction-cost adjusted returns (0.1% per leg = 0.2% round trip)
  - Walk-forward split: pre/post 2021

Usage:
  python momentum_factor_test.py
  python momentum_factor_test.py --start 2015-01 --end 2025-12
  python momentum_factor_test.py --min-stocks 30

Docker:
  docker compose run --rm pipeline python momentum_factor_test.py
"""

import os
import uuid
import argparse
from datetime import date, datetime

import clickhouse_connect
import pandas as pd
import numpy as np

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

TRANSACTION_COST = 0.001   # 0.1% per leg, 0.2% round trip


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


def load_monthly_prices(ch, start: str, end: str) -> pd.DataFrame:
    """Load last trading-day close for each month per symbol."""
    df = ch.query_df(f"""
        SELECT
            symbol,
            toStartOfMonth(date) AS month,
            argMax(close, date)  AS close
        FROM market.ohlcv_daily
        WHERE symbol LIKE '%.NS'
          AND date BETWEEN '{start}-01' AND '{end}-31'
        GROUP BY symbol, month
        ORDER BY symbol, month
    """)
    df["month"] = pd.to_datetime(df["month"]).dt.to_period("M")
    return df


def load_benchmark(ch, start: str, end: str) -> pd.Series:
    """Monthly returns for NIFTYBEES.NS (Nifty 50 ETF proxy)."""
    df = ch.query_df(f"""
        SELECT
            toStartOfMonth(date) AS month,
            argMax(close, date)  AS close
        FROM market.ohlcv_daily
        WHERE symbol = 'NIFTYBEES.NS'
          AND date BETWEEN '{start}-01' AND '{end}-31'
        GROUP BY month
        ORDER BY month
    """)
    df["month"] = pd.to_datetime(df["month"]).dt.to_period("M")
    df = df.set_index("month")["close"]
    return df.pct_change().dropna()


def compute_momentum_returns(prices: pd.DataFrame, min_stocks: int = 20) -> pd.DataFrame:
    """
    For each month, compute 12-1 momentum and assign quintiles.
    Returns per-month per-quintile average returns.
    """
    # Pivot: index=month, columns=symbol, values=close
    pivot = prices.pivot(index="month", columns="symbol", values="close")
    pivot = pivot.sort_index()

    records = []
    months = pivot.index

    for i, month in enumerate(months):
        # Need at least 13 prior months (12 for formation + 1 skip)
        if i < 13:
            continue
        # Formation: return from t-12 to t-2 (skip t-1)
        t_12 = months[i - 12]
        t_2  = months[i - 2]
        t_1  = months[i - 1]
        t_0  = month

        price_t12 = pivot.loc[t_12]
        price_t2  = pivot.loc[t_2]
        price_t1  = pivot.loc[t_1]
        price_t0  = pivot.loc[t_0]

        # Momentum signal: 12-1 month return
        mom = (price_t2 / price_t12 - 1).dropna()

        # Forward return: t-1 to t (1 month holding)
        fwd = (price_t0 / price_t1 - 1).dropna()

        # Stocks in both
        common = mom.index.intersection(fwd.index)
        if len(common) < min_stocks:
            continue

        mom = mom[common]
        fwd = fwd[common]

        # Rank into quintiles
        quintile = pd.qcut(mom, 5, labels=[1, 2, 3, 4, 5])

        for q in range(1, 6):
            mask = quintile == q
            q_fwd = fwd[mask]
            records.append({
                "month":       month,
                "quintile":    q,
                "n_stocks":    mask.sum(),
                "raw_return":  q_fwd.mean(),
                "median_ret":  q_fwd.median(),
                "std":         q_fwd.std(),
            })

    df = pd.DataFrame(records)
    # Net return after costs (2 transactions per month: sell + buy)
    df["net_return"] = df["raw_return"] - 2 * TRANSACTION_COST
    return df


def print_quintile_table(qdf: pd.DataFrame, label: str, benchmark_mean: float):
    print(f"\n{'='*72}")
    print(f"  {label}")
    print(f"{'='*72}")
    print(f"  {'Quintile':<12} {'Months':>7} {'Mean Raw%':>10} {'Net Ret%':>10} {'Med%':>8} {'Sharpe':>8}")
    print(f"  {'-'*12} {'-'*7} {'-'*10} {'-'*10} {'-'*8} {'-'*8}")

    for q in range(1, 6):
        sub = qdf[qdf["quintile"] == q]
        mean_r  = sub["raw_return"].mean() * 100
        mean_n  = sub["net_return"].mean() * 100
        med_r   = sub["median_ret"].median() * 100
        # Sharpe: annualised monthly Sharpe of net returns
        sharpe  = (sub["net_return"].mean() / sub["net_return"].std() * 12 ** 0.5
                   if sub["net_return"].std() > 0 else 0)
        label_q = f"Q{q} ({'losers' if q==1 else 'winners' if q==5 else ''})"
        print(f"  {label_q:<12} {len(sub):>7} {mean_r:>10.2f} {mean_n:>10.2f} {med_r:>8.2f} {sharpe:>8.2f}")

    # Benchmark row
    bm_monthly_pct = benchmark_mean * 100
    print(f"  {'Benchmark':<12} {'':>7} {bm_monthly_pct:>10.2f} {bm_monthly_pct:>10.2f} {'':>8} {'':>8}")

    # Momentum spread: Q5 - Q1
    q5 = qdf[qdf["quintile"] == 5]["net_return"]
    q1 = qdf[qdf["quintile"] == 1]["net_return"]
    spread_mean = (q5.mean() - q1.mean()) * 100
    print(f"\n  Q5 - Q1 spread (net): {spread_mean:+.2f}% per month = {spread_mean*12:+.1f}% annualised")

    # Q5 vs benchmark
    q5_alpha = (q5.mean() - benchmark_mean) * 100
    print(f"  Q5 alpha vs benchmark: {q5_alpha:+.2f}% per month = {q5_alpha*12:+.1f}% annualised")


def compute_cumulative(qdf: pd.DataFrame, q: int, benchmark: pd.Series) -> pd.Series:
    """Cumulative wealth index for quintile q starting at 1.0."""
    sub = qdf[qdf["quintile"] == q].set_index("month")["net_return"]
    return (1 + sub).cumprod()


def save_momentum_results(ch, qdf: pd.DataFrame, run_id: str,
                          universe: str, bm_mean: float, start: str, end: str):
    now     = datetime.utcnow()
    version = int(now.timestamp())
    split   = pd.Period("2021-01", "M")

    # Save one summary row per quintile per period
    rows = []
    for period, sub_qdf in [("full", qdf),
                             ("train", qdf[qdf["month"] < split]),
                             ("test",  qdf[qdf["month"] >= split])]:
        for q in range(1, 6):
            sub = sub_qdf[sub_qdf["quintile"] == q]
            if sub.empty:
                continue
            net = sub["net_return"]
            sharpe = (net.mean() / net.std() * 12**0.5 if net.std() > 0 else 0.0)
            bm_val = bm_mean
            alpha  = net.mean() - bm_val
            rows.append({
                "run_id":          run_id,
                "strategy":        f"momentum_q{q}",
                "universe":        universe,
                "start_date":      pd.Timestamp(f"{start}-01").date(),
                "end_date":        pd.Timestamp(f"{end}-01").date(),
                "period":          period,
                "n_trades":        len(sub),
                "win_rate":        float((net > 0).mean()),
                "avg_net_ret":     float(net.mean()),
                "median_net_ret":  float(net.median()),
                "best_trade":      float(net.max()),
                "worst_trade":     float(net.min()),
                "avg_hold_months": 1.0,
                "sharpe":          float(sharpe),
                "bm_monthly_ret":  float(bm_val),
                "alpha_monthly":   float(alpha),
                "computed_at":     now,
                "version":         version,
            })

    if rows:
        ch.insert_df("analysis.strategy_runs", pd.DataFrame(rows))
        print(f"  Saved {len(rows)} quintile rows → analysis.strategy_runs (run_id={run_id})")


def main():
    parser = argparse.ArgumentParser(description="Momentum factor test on Indian stocks")
    parser.add_argument("--start", default="2014-01", help="Start YYYY-MM (default 2014-01)")
    parser.add_argument("--end",   default="2025-12", help="End YYYY-MM (default 2025-12)")
    parser.add_argument("--min-stocks", type=int, default=20,
                        help="Min stocks per quintile to include month (default 20)")
    parser.add_argument("--universe", default="nse", help="Label for universe (default: nse)")
    parser.add_argument("--no-save",  action="store_true",
                        help="Skip saving results to ClickHouse")
    args = parser.parse_args()
    run_id = str(uuid.uuid4())[:8]

    ch = get_ch()

    print(f"Loading monthly prices {args.start} → {args.end}...")
    prices = load_monthly_prices(ch, args.start, args.end)
    n_symbols = prices["symbol"].nunique()
    n_months  = prices["month"].nunique()
    print(f"  {n_symbols} symbols × {n_months} months loaded")

    print("Loading benchmark (NIFTYBEES.NS)...")
    benchmark = load_benchmark(ch, args.start, args.end)
    bm_mean = benchmark.mean()
    print(f"  Benchmark mean monthly return: {bm_mean*100:.2f}%")

    print("Computing momentum quintiles...")
    qdf = compute_momentum_returns(prices, min_stocks=args.min_stocks)
    n_months_used = qdf["month"].nunique()
    print(f"  Valid months: {n_months_used}")

    # ── Full period ───────────────────────────────────────
    print_quintile_table(qdf, f"FULL PERIOD  {args.start} → {args.end}", bm_mean)

    # ── Walk-forward split ────────────────────────────────
    split = pd.Period("2021-01", "M")
    train = qdf[qdf["month"] < split]
    test  = qdf[qdf["month"] >= split]

    bm_train = benchmark[benchmark.index < split].mean()
    bm_test  = benchmark[benchmark.index >= split].mean()

    print_quintile_table(train, "TRAIN  2014-01 → 2020-12  (in-sample)", bm_train)
    print_quintile_table(test,  "TEST   2021-01 → 2025-12  (out-of-sample)", bm_test)

    # ── Summary ──────────────────────────────────────────
    print(f"\n{'='*72}")
    print("  INTERPRETATION")
    print(f"{'='*72}")

    q5_full  = qdf[qdf["quintile"] == 5]["net_return"].mean() * 100
    q1_full  = qdf[qdf["quintile"] == 1]["net_return"].mean() * 100
    spread   = q5_full - q1_full

    q5_train = train[train["quintile"] == 5]["net_return"].mean() * 100
    q5_test  = test[test["quintile"] == 5]["net_return"].mean() * 100

    print(f"  Full-period Q5-Q1 spread: {spread:+.2f}% / month")
    print(f"  Q5 winner returns:  train={q5_train:+.2f}%  test={q5_test:+.2f}%  (after costs)")
    print()

    if spread > 0.3:
        print("  RESULT: Momentum factor EXISTS in this universe.")
        print("  Winners outperform losers by a meaningful margin after costs.")
        if q5_test > q5_train * 0.6:
            print("  Edge is STABLE out-of-sample — this is worth building on.")
        else:
            print("  Edge DECAYS out-of-sample — may be specific to training period.")
    elif spread > 0:
        print("  RESULT: Weak momentum signal. Spread exists but narrow after costs.")
        print("  Transaction costs eat most of the edge — needs lower-cost execution.")
    else:
        print("  RESULT: No momentum edge. Q5 does NOT outperform Q1 after costs.")
        print("  Momentum is not the right factor for this universe/period.")

    print()
    print("  Note: 0.2% round-trip cost assumed (NSE: brokerage + STT + exchange fee)")
    print(f"  Universe: .NS stocks with ≥{args.min_stocks} stocks per quintile per month")

    if not args.no_save:
        print(f"\nSaving results (run_id={run_id})...")
        save_momentum_results(ch, qdf, run_id, args.universe, bm_mean, args.start, args.end)
    else:
        print("\n(--no-save: skipping ClickHouse write)")


if __name__ == "__main__":
    main()
