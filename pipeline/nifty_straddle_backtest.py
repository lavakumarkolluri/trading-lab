#!/usr/bin/env python3
"""
nifty_straddle_backtest.py
───────────────────────────
Backtests a Nifty weekly ATM straddle selling strategy using historical
EOD option chain data.

Strategy:
  ENTRY : Day after each weekly expiry, sell ATM straddle for NEXT weekly expiry
          Strike = closest to spot. Premium = CE_ltp + PE_ltp at that strike.
  EXIT  : (checked each EOD day)
    1. Profit target : straddle value ≤ 50% of entry premium  → buy back
    2. Stop loss     : straddle value ≥ 200% of entry premium → buy back
    3. Expiry        : settle at intrinsic = |spot - strike|

P&L = (entry_premium - exit_cost) × lot_size - transaction_costs
Lot size: 75 (Nifty F&O lot size throughout the period)
Cost: ₹40 per lot per leg (brokerage + STT + exchange) × 4 legs = ₹160 round trip

Usage:
  python nifty_straddle_backtest.py
  python nifty_straddle_backtest.py --target 0.5 --stop 2.0
  python nifty_straddle_backtest.py --status

Docker:
  docker compose run --rm pipeline python nifty_straddle_backtest.py
"""

import os
import uuid
import argparse
import logging
from datetime import date, datetime

import pandas as pd
import numpy as np
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CH_HOST   = os.getenv("CH_HOST", "clickhouse")
CH_PORT   = int(os.getenv("CH_PORT", "8123"))
CH_USER   = os.getenv("CH_USER", "default")
CH_PASS   = os.getenv("CH_PASSWORD", "")

LOT_SIZE       = 75       # Nifty lot size
COST_PER_TRADE = 160      # ₹160 round trip (all 4 legs)
TARGET_RATIO   = 0.50     # exit when straddle decays to 50% of entry
STOP_RATIO     = 2.00     # stop when straddle reaches 200% of entry


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


def load_spot_map(ch) -> dict:
    rows = ch.query(
        "SELECT toDate(timestamp), argMax(nifty_spot, timestamp) "
        "FROM market.nifty_live FINAL GROUP BY toDate(timestamp)"
    ).result_rows
    return {r[0]: float(r[1]) for r in rows if r[1] and r[1] > 0}


def load_chain_prices(ch, expiry: date) -> pd.DataFrame:
    """Load all EOD prices for a given expiry across all dates."""
    df = ch.query_df(f"""
        SELECT toDate(timestamp) AS date, strike, option_type, ltp, oi
        FROM market.options_chain FINAL
        WHERE symbol = 'NIFTY' AND expiry = '{expiry}'
          AND ltp > 0
        ORDER BY date, strike
    """)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def load_all_expiries(ch) -> list[date]:
    rows = ch.query(
        "SELECT DISTINCT expiry FROM market.options_chain "
        "WHERE symbol='NIFTY' ORDER BY expiry"
    ).result_rows
    return [r[0] for r in rows]


def atm_strike(strikes: np.ndarray, spot: float) -> float:
    return float(min(strikes, key=lambda s: abs(s - spot)))


def straddle_value(df_day: pd.DataFrame, strike: float) -> float | None:
    """CE + PE price for a given strike on a given day."""
    ce = df_day[(df_day["strike"] == strike) & (df_day["option_type"] == "CE")]["ltp"]
    pe = df_day[(df_day["strike"] == strike) & (df_day["option_type"] == "PE")]["ltp"]
    if ce.empty or pe.empty:
        return None
    return float(ce.iloc[0]) + float(pe.iloc[0])


def run_backtest(ch, target_ratio: float, stop_ratio: float) -> pd.DataFrame:
    spot_map  = load_spot_map(ch)
    expiries  = load_all_expiries(ch)
    trades    = []

    for idx in range(1, len(expiries)):
        prev_expiry = expiries[idx - 1]
        this_expiry = expiries[idx]

        # Load all chain data for this expiry
        chain = load_chain_prices(ch, this_expiry)
        if chain.empty:
            continue

        chain_dates = sorted(chain["date"].unique())

        # Entry: first date AFTER previous expiry
        entry_candidates = [d for d in chain_dates if d > prev_expiry]
        if not entry_candidates:
            continue
        entry_date = entry_candidates[0]

        spot = spot_map.get(entry_date)
        if not spot:
            continue

        df_entry = chain[chain["date"] == entry_date]
        strikes  = df_entry["strike"].unique()
        if len(strikes) == 0:
            continue

        strike = atm_strike(strikes, spot)
        entry_premium = straddle_value(df_entry, strike)
        if entry_premium is None or entry_premium <= 0:
            continue

        target_val = entry_premium * target_ratio
        stop_val   = entry_premium * stop_ratio

        # Simulate day by day from day after entry
        exit_date   = None
        exit_cost   = None
        exit_reason = "expiry"

        hold_dates = [d for d in chain_dates if d > entry_date]

        for d in hold_dates:
            df_day  = chain[chain["date"] == d]
            current = straddle_value(df_day, strike)

            if current is None:
                # Use intrinsic if on or after expiry
                if d >= this_expiry:
                    spot_exp = spot_map.get(d, spot)
                    current  = abs(spot_exp - strike)
                    exit_date, exit_cost, exit_reason = d, current, "expiry"
                    break
                continue

            if d >= this_expiry:
                # Settle at intrinsic
                spot_exp = spot_map.get(d, spot)
                exit_cost = abs(spot_exp - strike)
                exit_date, exit_reason = d, "expiry"
                break
            elif current <= target_val:
                exit_date, exit_cost, exit_reason = d, current, "target"
                break
            elif current >= stop_val:
                exit_date, exit_cost, exit_reason = d, current, "stop_loss"
                break

        if exit_date is None:
            # Position still open (last expiry in dataset)
            continue

        pnl_points  = entry_premium - exit_cost
        pnl_rupees  = pnl_points * LOT_SIZE - COST_PER_TRADE
        hold_days   = (exit_date - entry_date).days

        trades.append({
            "expiry":          this_expiry,
            "entry_date":      entry_date,
            "exit_date":       exit_date,
            "strike":          strike,
            "spot_at_entry":   spot,
            "entry_premium":   entry_premium,
            "exit_cost":       exit_cost,
            "exit_reason":     exit_reason,
            "pnl_points":      pnl_points,
            "pnl_rupees":      pnl_rupees,
            "hold_days":       hold_days,
        })

    return pd.DataFrame(trades)


def print_stats(trades: pd.DataFrame, label: str):
    if trades.empty:
        print(f"\n{label}: no trades")
        return

    n         = len(trades)
    wins      = (trades["pnl_rupees"] > 0).sum()
    targets   = (trades["exit_reason"] == "target").sum()
    stops     = (trades["exit_reason"] == "stop_loss").sum()
    expiries  = (trades["exit_reason"] == "expiry").sum()
    avg_pnl   = trades["pnl_rupees"].mean()
    med_pnl   = trades["pnl_rupees"].median()
    total_pnl = trades["pnl_rupees"].sum()
    best      = trades["pnl_rupees"].max()
    worst     = trades["pnl_rupees"].min()
    avg_hold  = trades["hold_days"].mean()

    # Annualised daily P&L
    total_days = (trades["exit_date"].max() - trades["entry_date"].min()).days
    daily_pnl  = total_pnl / total_days if total_days > 0 else 0

    # Sharpe on weekly trade returns
    std = trades["pnl_rupees"].std()
    sharpe = (avg_pnl / std * (52**0.5)) if std > 0 else 0

    # Capital required: ~₹1.5L margin per lot for short straddle
    capital    = 150000
    annual_ret = (total_pnl / capital) * (365 / total_days) * 100 if total_days > 0 else 0

    print(f"\n{'='*65}")
    print(f"  {label}")
    print(f"{'='*65}")
    print(f"  Trades         : {n}  (target={targets} stop={stops} expiry={expiries})")
    print(f"  Win rate       : {wins/n*100:.1f}%")
    print(f"  Avg P&L/trade  : ₹{avg_pnl:,.0f}  (median ₹{med_pnl:,.0f})")
    print(f"  Best / Worst   : ₹{best:,.0f} / ₹{worst:,.0f}")
    print(f"  Total P&L      : ₹{total_pnl:,.0f}")
    print(f"  Avg hold       : {avg_hold:.1f} days")
    print(f"  Daily P&L avg  : ₹{daily_pnl:,.0f}/day")
    print(f"  Annualised ret : {annual_ret:.1f}%  (on ₹{capital/1000:.0f}k margin)")
    print(f"  Sharpe         : {sharpe:.2f}")


def save_results(ch, trades: pd.DataFrame, run_id: str, target_ratio: float, stop_ratio: float):
    now     = datetime.utcnow()
    version = int(now.timestamp())

    # Save trades
    rows = []
    for _, t in trades.iterrows():
        rows.append({
            "run_id":      run_id,
            "strategy":    f"nifty_straddle_{int(target_ratio*100)}t_{int(stop_ratio*100)}sl",
            "universe":    "NIFTY",
            "symbol":      "NIFTY",
            "entry_month": pd.Timestamp(t["entry_date"]).to_period("M").to_timestamp().date(),
            "exit_month":  pd.Timestamp(t["exit_date"]).to_period("M").to_timestamp().date(),
            "hold_months": max(1, t["hold_days"] // 30),
            "entry_px":    float(t["entry_premium"]),
            "exit_px":     float(t["exit_cost"]),
            "raw_ret":     float(t["pnl_points"] / t["entry_premium"]) if t["entry_premium"] > 0 else 0,
            "net_ret":     float(t["pnl_rupees"] / 150000),  # normalised to ₹1.5L capital
            "is_open":     0,
            "computed_at": now,
            "version":     version,
        })
    if rows:
        ch.insert_df("analysis.strategy_trades", pd.DataFrame(rows))

    # Save summary rows
    strategy = f"nifty_straddle_{int(target_ratio*100)}t_{int(stop_ratio*100)}sl"
    split    = date(2025, 1, 1)
    summary  = []
    for period, sub in [("full", trades),
                         ("train", trades[trades["entry_date"] < split]),
                         ("test",  trades[trades["entry_date"] >= split])]:
        if sub.empty:
            continue
        net = sub["pnl_rupees"] / 150000
        total_days = (sub["exit_date"].max() - sub["entry_date"].min()).days
        summary.append({
            "run_id":          run_id,
            "strategy":        strategy,
            "universe":        "NIFTY",
            "start_date":      sub["entry_date"].min(),
            "end_date":        sub["exit_date"].max(),
            "period":          period,
            "n_trades":        len(sub),
            "win_rate":        float((sub["pnl_rupees"] > 0).mean()),
            "avg_net_ret":     float(net.mean()),
            "median_net_ret":  float(net.median()),
            "best_trade":      float(net.max()),
            "worst_trade":     float(net.min()),
            "avg_hold_months": float(sub["hold_days"].mean() / 30),
            "sharpe":          float(net.mean() / net.std() * (52**0.5)) if net.std() > 0 else 0,
            "bm_monthly_ret":  0.012,  # ~1.2% Nifty monthly
            "alpha_monthly":   float(net.mean() * 4) - 0.012,
            "computed_at":     now,
            "version":         version,
        })
    if summary:
        ch.insert_df("analysis.strategy_runs", pd.DataFrame(summary))

    log.info(f"Saved {len(rows)} trades + {len(summary)} summary rows (run_id={run_id})")


def show_status(ch):
    rows = ch.query("""
        SELECT strategy, period, n_trades,
               round(win_rate*100,1) as wr,
               round(avg_net_ret*100*52,1) as ann_ret_pct,
               round(sharpe,2),
               start_date, end_date
        FROM analysis.strategy_runs FINAL
        WHERE strategy LIKE 'nifty_straddle%'
        ORDER BY strategy, period
    """).result_rows
    if not rows:
        print("No straddle backtest results saved yet.")
        return
    print(f"\n{'Strategy':<35} {'Period':<6} {'N':>5} {'WR%':>6} {'Ann%':>7} {'Shrp':>6}")
    print("-" * 70)
    for r in rows:
        print(f"{r[0]:<35} {r[1]:<6} {r[2]:>5} {r[3]:>6.1f} {r[4]:>7.1f} {r[5]:>6.2f}")


def main():
    parser = argparse.ArgumentParser(description="Nifty weekly straddle selling backtest")
    parser.add_argument("--target", type=float, default=TARGET_RATIO,
                        help=f"Profit target ratio (default {TARGET_RATIO})")
    parser.add_argument("--stop",   type=float, default=STOP_RATIO,
                        help=f"Stop loss ratio (default {STOP_RATIO})")
    parser.add_argument("--no-save", action="store_true")
    parser.add_argument("--status",  action="store_true")
    args = parser.parse_args()

    ch = get_ch()

    if args.status:
        show_status(ch)
        return

    run_id = str(uuid.uuid4())[:8]
    log.info(f"Run ID: {run_id} | target={args.target:.0%} | stop={args.stop:.0%}")
    log.info("Loading option chain data and running backtest...")

    trades = run_backtest(ch, args.target, args.stop)
    log.info(f"Generated {len(trades)} trades")

    if trades.empty:
        log.error("No trades generated — check data availability")
        return

    # Full period
    print_stats(trades, f"FULL PERIOD  2024-01 → 2026-04")

    # Walk-forward split
    split = date(2025, 1, 1)
    print_stats(trades[trades["entry_date"] < split],  "TRAIN  2024")
    print_stats(trades[trades["entry_date"] >= split], "TEST   2025-2026")

    # Exit reason breakdown
    print(f"\n  Exit breakdown: "
          f"target={( trades['exit_reason']=='target').sum()} "
          f"stop={( trades['exit_reason']=='stop_loss').sum()} "
          f"expiry={( trades['exit_reason']=='expiry').sum()}")

    # Top/bottom trades
    print(f"\n  Top 5 trades:")
    for _, t in trades.nlargest(5, "pnl_rupees").iterrows():
        print(f"    {t['entry_date']} exp={t['expiry']} strike={t['strike']:.0f} "
              f"prem={t['entry_premium']:.0f} → ₹{t['pnl_rupees']:,.0f} ({t['exit_reason']})")
    print(f"\n  Bottom 5 trades:")
    for _, t in trades.nsmallest(5, "pnl_rupees").iterrows():
        print(f"    {t['entry_date']} exp={t['expiry']} strike={t['strike']:.0f} "
              f"prem={t['entry_premium']:.0f} → ₹{t['pnl_rupees']:,.0f} ({t['exit_reason']})")

    if not args.no_save:
        save_results(ch, trades, run_id, args.target, args.stop)


if __name__ == "__main__":
    main()
