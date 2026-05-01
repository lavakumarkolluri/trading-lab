#!/usr/bin/env python3
"""
breakout_backtest.py
─────────────────────
Tests a monthly price breakout strategy:

  ENTRY : monthly close > highest high of the previous 3 months
  EXIT  : monthly close < previous month's low

Signals are generated at month-end close. Position taken at next
month's open (realistic — you can't trade at the close that triggered).

Includes transaction costs: 0.1% per leg (0.2% round trip).

Walk-forward split: train 2014–2019, test 2020–2025.

Usage:
  python breakout_backtest.py
  python breakout_backtest.py --start 2015-01 --end 2025-12
  python breakout_backtest.py --universe bse

Docker:
  docker compose run --rm pipeline python breakout_backtest.py
"""

import os
import uuid
import argparse
from datetime import datetime

import clickhouse_connect
import pandas as pd
import numpy as np

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

COST_PER_LEG = 0.001   # 0.1% brokerage + STT + fees


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


def load_monthly_ohlc(ch, universe: str, start: str, end: str) -> pd.DataFrame:
    suffix = "%.NS" if universe == "nse" else ("%.BO" if universe == "bse" else "%.%")
    df = ch.query_df(f"""
        SELECT
            symbol,
            toStartOfMonth(date) AS month,
            argMin(open, date)   AS m_open,
            max(high)            AS m_high,
            min(low)             AS m_low,
            argMax(close, date)  AS m_close
        FROM market.ohlcv_daily
        WHERE symbol LIKE '{suffix}'
          AND date BETWEEN '{start}-01' AND '{end}-31'
        GROUP BY symbol, month
        ORDER BY symbol, month
    """)
    df["month"] = pd.to_datetime(df["month"]).dt.to_period("M")
    return df


def load_benchmark(ch, start: str, end: str) -> pd.Series:
    df = ch.query_df(f"""
        SELECT
            toStartOfMonth(date) AS month,
            argMax(close, date)  AS m_close,
            argMin(open, date)   AS m_open
        FROM market.ohlcv_daily
        WHERE symbol = 'NIFTYBEES.NS'
          AND date BETWEEN '{start}-01' AND '{end}-31'
        GROUP BY month ORDER BY month
    """)
    df["month"] = pd.to_datetime(df["month"]).dt.to_period("M")
    df = df.set_index("month")
    # Buy-and-hold: enter at open, compare close to close
    ret = df["m_close"].pct_change().dropna()
    return ret


def run_backtest(ohlc: pd.DataFrame) -> pd.DataFrame:
    """
    For each stock independently, simulate the breakout strategy.

    Timing (no look-ahead):
      Signal fires at END of month M (when we know that month's close).
      Execution happens at OPEN of month M+1 (first trade opportunity after signal).

    ENTRY signal: close[M] > max(high[M-3], high[M-2], high[M-1])
    EXIT  signal: close[M] < low[M-1]

    Returns a DataFrame of all trades.
    """
    trades = []

    for symbol, grp in ohlc.groupby("symbol", sort=False):
        grp = grp.set_index("month").sort_index()

        if len(grp) < 6:
            continue

        months   = grp.index
        in_pos   = False
        entry_px = None
        entry_mo = None
        pending_entry = False   # signal fired last month, enter at this month's open
        pending_exit  = False

        for i in range(4, len(months)):
            mo       = months[i]
            sig_mo   = months[i - 1]   # signal month (previous month's close)
            sig_row  = grp.loc[sig_mo]
            sig_prev = grp.loc[months[i - 2]]
            exec_row = grp.loc[mo]     # execution month (this month's open)

            # Execute pending entry from last month's signal
            if pending_entry:
                entry_px = exec_row["m_open"]
                entry_mo = mo
                in_pos   = True
                pending_entry = False

            # Execute pending exit from last month's signal
            if pending_exit and in_pos:
                exit_px = exec_row["m_open"]
                raw_ret = exit_px / entry_px - 1
                net_ret = raw_ret - 2 * COST_PER_LEG
                trades.append({
                    "symbol":      symbol,
                    "entry_month": entry_mo,
                    "exit_month":  mo,
                    "hold_months": (mo - entry_mo).n,
                    "entry_px":    entry_px,
                    "exit_px":     exit_px,
                    "raw_ret":     raw_ret,
                    "net_ret":     net_ret,
                })
                in_pos        = False
                pending_exit  = False

            # Generate signals on sig_mo's close (executed next month)
            if not in_pos and not pending_entry:
                high_3m = grp.loc[months[i-4:i-1], "m_high"].max()
                if sig_row["m_close"] > high_3m:
                    pending_entry = True

            elif in_pos and not pending_exit:
                # Exit signal: this month's close < previous month's low
                if sig_row["m_close"] < sig_prev["m_low"]:
                    pending_exit = True

        # Close any still-open position at last available close
        if in_pos:
            last_mo = months[-1]
            exit_px = grp.loc[last_mo, "m_close"]
            raw_ret = exit_px / entry_px - 1
            net_ret = raw_ret - 2 * COST_PER_LEG
            trades.append({
                "symbol":      symbol,
                "entry_month": entry_mo,
                "exit_month":  last_mo,
                "hold_months": (last_mo - entry_mo).n,
                "entry_px":    entry_px,
                "exit_px":     exit_px,
                "raw_ret":     raw_ret,
                "net_ret":     net_ret,
                "open":        True,
            })

    return pd.DataFrame(trades)


def print_stats(trades: pd.DataFrame, label: str, bm_mean: float):
    if trades.empty:
        print(f"\n{label}: no trades.")
        return

    closed = trades[trades.get("open", pd.Series(False, index=trades.index)).fillna(False) == False] \
        if "open" in trades.columns else trades

    n          = len(closed)
    win_rate   = (closed["net_ret"] > 0).mean() * 100
    avg_ret    = closed["net_ret"].mean() * 100
    med_ret    = closed["net_ret"].median() * 100
    avg_hold   = closed["hold_months"].mean()
    best       = closed["net_ret"].max() * 100
    worst      = closed["net_ret"].min() * 100
    std        = closed["net_ret"].std()

    # Annualised Sharpe (per-trade, not time-series — approximate)
    monthly_ret = closed["net_ret"] / closed["hold_months"]   # approx monthly rate
    sharpe = (monthly_ret.mean() / monthly_ret.std() * 12 ** 0.5
              if monthly_ret.std() > 0 else 0)

    # Expectancy per month held
    expectancy_pm = avg_ret / avg_hold if avg_hold > 0 else 0

    # Benchmark comparison: monthly return * avg hold months
    bm_equiv = bm_mean * avg_hold * 100

    print(f"\n{'='*65}")
    print(f"  {label}")
    print(f"{'='*65}")
    print(f"  Trades (closed)   : {n}")
    print(f"  Win rate          : {win_rate:.1f}%")
    print(f"  Avg net return    : {avg_ret:+.2f}%  (per trade)")
    print(f"  Median net return : {med_ret:+.2f}%")
    print(f"  Best / Worst      : {best:+.1f}% / {worst:+.1f}%")
    print(f"  Avg hold (months) : {avg_hold:.1f}")
    print(f"  Expectancy/month  : {expectancy_pm:+.2f}%")
    print(f"  Benchmark/hold    : {bm_equiv:+.2f}%  (Nifty over same avg hold)")
    print(f"  Alpha/month       : {expectancy_pm - bm_mean*100:+.2f}%")
    print(f"  Sharpe (approx)   : {sharpe:.2f}")

    # Distribution
    bins = [-999, -20, -10, -5, 0, 5, 10, 20, 50, 999]
    labels = ["<-20%", "-20:-10", "-10:-5", "-5:0", "0:5", "5:10", "10:20", "20:50", ">50%"]
    cats = pd.cut(closed["net_ret"] * 100, bins=bins, labels=labels)
    dist = cats.value_counts().sort_index()
    print(f"\n  Return distribution:")
    for bucket, count in dist.items():
        bar = "█" * min(count, 30)
        print(f"    {bucket:>10s} : {count:>4d}  {bar}")


def save_results(ch, trades: pd.DataFrame, run_id: str,
                 universe: str, bm_mean: float,
                 start: str, end: str):
    """Save trades + per-period summaries to ClickHouse."""
    now = datetime.utcnow()
    version = int(now.timestamp())

    # ── Trades table ─────────────────────────────────────
    rows = []
    for _, t in trades.iterrows():
        rows.append({
            "run_id":      run_id,
            "strategy":    "breakout_3m",
            "universe":    universe,
            "symbol":      t["symbol"],
            "entry_month": t["entry_month"].to_timestamp().date(),
            "exit_month":  t["exit_month"].to_timestamp().date(),
            "hold_months": int(t["hold_months"]),
            "entry_px":    float(t["entry_px"]),
            "exit_px":     float(t["exit_px"]),
            "raw_ret":     float(t["raw_ret"]),
            "net_ret":     float(t["net_ret"]),
            "is_open":     int(bool(t.get("open") == True)),
            "computed_at": now,
            "version":     version,
        })
    if rows:
        ch.insert_df("analysis.strategy_trades", pd.DataFrame(rows))
        print(f"  Saved {len(rows)} trades → analysis.strategy_trades")

    # ── Summary table ─────────────────────────────────────
    train_end  = pd.Period("2019-12", "M")
    test_start = pd.Period("2020-01", "M")
    splits = [
        ("full",  trades,                                          bm_mean, start, end),
        ("train", trades[trades["entry_month"] <= train_end],     None,    start, "2019-12"),
        ("test",  trades[trades["entry_month"] >= test_start],    None,    "2020-01", end),
    ]
    summary_rows = []
    for period, sub, bm, s, e in splits:
        if sub.empty:
            continue
        closed = sub[sub.get("open", pd.Series(False, index=sub.index)).fillna(False) == False] \
            if "open" in sub.columns else sub
        if closed.empty:
            continue
        monthly_ret = closed["net_ret"] / closed["hold_months"]
        sharpe = (monthly_ret.mean() / monthly_ret.std() * 12**0.5
                  if monthly_ret.std() > 0 else 0.0)
        bm_val = bm if bm is not None else bm_mean
        alpha  = (closed["net_ret"] / closed["hold_months"]).mean() - bm_val
        summary_rows.append({
            "run_id":          run_id,
            "strategy":        "breakout_3m",
            "universe":        universe,
            "start_date":      pd.Timestamp(f"{s}-01").date(),
            "end_date":        pd.Timestamp(f"{e}-01").date(),
            "period":          period,
            "n_trades":        len(closed),
            "win_rate":        float((closed["net_ret"] > 0).mean()),
            "avg_net_ret":     float(closed["net_ret"].mean()),
            "median_net_ret":  float(closed["net_ret"].median()),
            "best_trade":      float(closed["net_ret"].max()),
            "worst_trade":     float(closed["net_ret"].min()),
            "avg_hold_months": float(closed["hold_months"].mean()),
            "sharpe":          float(sharpe),
            "bm_monthly_ret":  float(bm_val),
            "alpha_monthly":   float(alpha),
            "computed_at":     now,
            "version":         version,
        })
    if summary_rows:
        ch.insert_df("analysis.strategy_runs", pd.DataFrame(summary_rows))
        print(f"  Saved {len(summary_rows)} summary rows → analysis.strategy_runs")


def main():
    parser = argparse.ArgumentParser(description="Monthly breakout strategy backtest")
    parser.add_argument("--start",    default="2014-01", help="Start YYYY-MM")
    parser.add_argument("--end",      default="2025-12", help="End YYYY-MM")
    parser.add_argument("--universe", default="all",
                        choices=["nse", "bse", "all"],
                        help="Stock universe (default: all)")
    parser.add_argument("--no-save",  action="store_true",
                        help="Skip saving results to ClickHouse")
    args = parser.parse_args()

    run_id = str(uuid.uuid4())[:8]
    ch = get_ch()

    print(f"Run ID: {run_id}")
    print(f"Loading monthly OHLC ({args.universe}) {args.start} → {args.end}...")
    ohlc = load_monthly_ohlc(ch, args.universe, args.start, args.end)
    n_sym = ohlc["symbol"].nunique()
    print(f"  {n_sym} symbols loaded")

    print("Loading benchmark...")
    benchmark = load_benchmark(ch, args.start, args.end)
    bm_mean = benchmark.mean()
    print(f"  Nifty mean monthly return: {bm_mean*100:.2f}%")

    print("Running backtest...")
    trades = run_backtest(ohlc)
    print(f"  {len(trades)} total trades generated")

    if trades.empty:
        print("No trades found. Check date range and universe.")
        return

    # ── Full period ───────────────────────────────────────
    print_stats(trades, f"FULL PERIOD  {args.start} → {args.end}", bm_mean)

    # ── Walk-forward split ────────────────────────────────
    train_end   = pd.Period("2019-12", "M")
    test_start  = pd.Period("2020-01", "M")

    train = trades[trades["entry_month"] <= train_end]
    test  = trades[trades["entry_month"] >= test_start]

    bm_train = benchmark[benchmark.index <= train_end].mean()
    bm_test  = benchmark[benchmark.index >= test_start].mean()

    print_stats(train, "TRAIN  2014–2019  (in-sample)", bm_train)
    print_stats(test,  "TEST   2020–2025  (out-of-sample)", bm_test)

    # ── Top performing symbols ────────────────────────────
    if "open" in trades.columns:
        closed = trades[trades["open"].fillna(False) == False]
    else:
        closed = trades
    top = (closed.groupby("symbol")["net_ret"]
           .agg(["count", "mean", "sum"])
           .query("count >= 3")
           .sort_values("mean", ascending=False)
           .head(10))
    top["mean_pct"] = top["mean"] * 100
    top["sum_pct"]  = top["sum"]  * 100
    print(f"\n{'='*65}")
    print(f"  TOP 10 SYMBOLS by avg trade return (min 3 trades)")
    print(f"{'='*65}")
    print(f"  {'Symbol':<20} {'Trades':>6}  {'Avg%':>8}  {'Total%':>8}")
    print(f"  {'-'*20} {'-'*6}  {'-'*8}  {'-'*8}")
    for sym, row in top.iterrows():
        print(f"  {sym:<20} {int(row['count']):>6}  {row['mean_pct']:>8.1f}  {row['sum_pct']:>8.1f}")

    # ── Save ──────────────────────────────────────────────
    if not args.no_save:
        print(f"\nSaving results (run_id={run_id})...")
        save_results(ch, trades, run_id, args.universe, bm_mean, args.start, args.end)
    else:
        print("\n(--no-save: skipping ClickHouse write)")


if __name__ == "__main__":
    main()
