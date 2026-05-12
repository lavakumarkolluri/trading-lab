#!/usr/bin/env python3
"""
weekend_theta_backtest.py
─────────────────────────
Strategy 1: Weekend Theta Harvest
  Entry : sell ATM straddle at Friday EOD (15:30 IST)
  Exit A: buy back at Monday EOD  → capture pure weekend decay
  Exit B: hold to Tuesday expiry  → capture full decay + expiry day move

Only Friday → Tuesday expiry triplets are used (3 calendar days gap).
Applies to NIFTY (weekly Tuesday) and BANKNIFTY/FINNIFTY (monthly Tuesday).

Results stored in analysis.weekend_theta_trades.

Usage:
  python weekend_theta_backtest.py              # all symbols, write to CH
  python weekend_theta_backtest.py --dry-run    # report only
  python weekend_theta_backtest.py --symbol NIFTY
"""

import argparse
import logging
import math
import os
from datetime import date, timedelta

import clickhouse_connect
import numpy as np
import pandas as pd
from scipy import stats

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CH_HOST     = os.getenv("CH_HOST", "clickhouse")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

STRIKE_STEPS     = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50}
LOT_SIZES        = {"NIFTY": 75, "BANKNIFTY": 35, "FINNIFTY": 40}
SUPPORTED_SYMBOLS = list(STRIKE_STEPS.keys())


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )


# ── Data helpers ──────────────────────────────────────────────────────────────

def fetch_trading_days(ch, symbol: str) -> list[date]:
    rows = ch.query(
        "SELECT DISTINCT toDate(timestamp) as dt FROM market.options_chain FINAL "
        "WHERE symbol={sym:String} ORDER BY dt",
        parameters={"sym": symbol}
    ).result_rows
    return [r[0] for r in rows]


def fetch_expiries(ch, symbol: str) -> list[date]:
    rows = ch.query(
        "SELECT DISTINCT toDate(expiry) as exp FROM market.options_chain FINAL "
        "WHERE symbol={sym:String} AND expiry <= today() "
        "  AND toDayOfWeek(toDate(expiry)) = 2 "   # Tuesday only
        "ORDER BY exp",
        parameters={"sym": symbol}
    ).result_rows
    return [r[0] for r in rows]


def load_straddle(ch, symbol: str, snap_date: date, expiry: date, step: int) -> dict:
    """
    Return ATM straddle price and spot for a given snapshot date and expiry.
    Uses put-call parity to estimate spot when options_eod_summary is unavailable.
    """
    result = ch.query(
        "SELECT strike, option_type, ltp, oi FROM market.options_chain FINAL "
        "WHERE symbol={sym:String} AND toDate(timestamp)={d:Date} "
        "  AND expiry={exp:Date} AND ltp > 0.5",
        parameters={"sym": symbol, "d": snap_date, "exp": expiry}
    )
    if not result.result_rows:
        return {}

    df = pd.DataFrame(result.result_rows, columns=["strike", "option_type", "ltp", "oi"])
    df["strike"] = df["strike"].astype(float)
    df["ltp"]    = df["ltp"].astype(float)
    df["oi"]     = df["oi"].astype(float)

    ce = df[df["option_type"] == "CE"].set_index("strike")[["ltp", "oi"]]
    pe = df[df["option_type"] == "PE"].set_index("strike")[["ltp", "oi"]]
    common = ce.index.intersection(pe.index)
    if len(common) == 0:
        return {}

    # ATM = strike with smallest |CE - PE|
    atm_strike = float((ce.loc[common, "ltp"] - pe.loc[common, "ltp"]).abs().idxmin())
    ce_ltp = float(ce.loc[atm_strike, "ltp"]) if atm_strike in ce.index else 0.0
    pe_ltp = float(pe.loc[atm_strike, "ltp"]) if atm_strike in pe.index else 0.0
    straddle_price = ce_ltp + pe_ltp

    # Spot via put-call parity
    spot = atm_strike + ce_ltp - pe_ltp

    # PCR
    total_ce_oi = float(df[df["option_type"] == "CE"]["oi"].sum())
    total_pe_oi = float(df[df["option_type"] == "PE"]["oi"].sum())
    pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0.0

    # For NIFTY, override spot from options_eod_summary if available
    if symbol == "NIFTY":
        ctx = ch.query(
            "SELECT nifty_spot, iv_rank, iv_percentile, atm_ce_iv, atm_pe_iv "
            "FROM market.options_eod_summary FINAL "
            "WHERE date={d:Date} AND expiry={exp:Date}",
            parameters={"d": snap_date, "exp": expiry}
        )
        if ctx.result_rows:
            r = ctx.result_rows[0]
            spot    = float(r[0] or spot)
            iv_rank = float(r[1] or 0)
            iv_pct  = float(r[2] or 0)
            atm_iv  = float(r[3] or r[4] or 0)
        else:
            iv_rank = iv_pct = atm_iv = 0.0
    else:
        iv_rank = iv_pct = atm_iv = 0.0

    return {
        "straddle":   round(straddle_price, 2),
        "spot":       round(spot, 2),
        "atm_strike": atm_strike,
        "pcr":        round(pcr, 4),
        "iv_rank":    round(iv_rank, 2),
        "iv_pct":     round(iv_pct, 2),
        "atm_iv":     round(atm_iv, 2),
    }


def straddle_intrinsic(strike: float, spot: float) -> float:
    return abs(spot - strike)


# ── Build (friday, monday, expiry) triplets ───────────────────────────────────

def build_triplets(trading_days: list[date], expiries: list[date]) -> list[tuple]:
    """
    For each Tuesday expiry, find the immediately preceding Friday and Monday
    that are in our trading_days set.
    """
    td_set = set(trading_days)
    triplets = []
    for exp in expiries:
        # Monday before expiry: expiry - 1 calendar day (usually)
        monday = exp - timedelta(days=1)
        while monday.weekday() != 0:   # 0 = Monday
            monday -= timedelta(days=1)
        # Friday before that Monday
        friday = monday - timedelta(days=3)
        while friday.weekday() != 4:   # 4 = Friday
            friday -= timedelta(days=1)

        if friday in td_set and monday in td_set:
            triplets.append((friday, monday, exp))
        else:
            log.debug("No Fri/Mon data for expiry=%s (Fri=%s Mon=%s)", exp, friday, monday)
    return triplets


# ── Per-triplet simulation ────────────────────────────────────────────────────

def simulate_weekend(ch, symbol: str, friday: date, monday: date,
                     expiry: date, step: int) -> dict | None:
    fri_data = load_straddle(ch, symbol, friday, expiry, step)
    mon_data = load_straddle(ch, symbol, monday, expiry, step)
    exp_data = load_straddle(ch, symbol, expiry, expiry, step)

    if not fri_data or fri_data["straddle"] == 0:
        log.debug("No Friday data for %s expiry=%s", symbol, expiry)
        return None
    if not mon_data or mon_data["straddle"] == 0:
        log.debug("No Monday data for %s expiry=%s", symbol, expiry)
        return None

    entry       = fri_data["straddle"]
    spot_fri    = fri_data["spot"]
    atm         = fri_data["atm_strike"]

    # ── Exit A: Monday EOD ────────────────────────────────────────────────────
    exit_mon    = mon_data["straddle"]
    spot_mon    = mon_data["spot"]
    profit_mon_pts = entry - exit_mon
    profit_mon_pct = profit_mon_pts / entry * 100 if entry > 0 else 0.0

    # ── Exit B: Tuesday expiry ────────────────────────────────────────────────
    spot_exp    = exp_data.get("spot", 0) if exp_data else 0
    exit_exp    = straddle_intrinsic(atm, spot_exp) if spot_exp > 0 else exit_mon
    profit_exp_pts = entry - exit_exp
    profit_exp_pct = profit_exp_pts / entry * 100 if entry > 0 else 0.0

    # ── Weekend metrics ───────────────────────────────────────────────────────
    weekend_decay_pts = entry - exit_mon                     # same as profit_mon_pts
    gap_pct = (spot_mon - spot_fri) / spot_fri * 100 if spot_fri > 0 else 0.0
    gap_pts = spot_mon - spot_fri

    return {
        "expiry_date":        expiry,
        "symbol":             symbol,
        "friday_date":        friday,
        "monday_date":        monday,
        "spot_friday":        round(spot_fri, 2),
        "spot_monday":        round(spot_mon, 2),
        "spot_expiry":        round(spot_exp, 2),
        "atm_strike":         atm,
        # Entry
        "straddle_entry":     round(entry, 2),
        # Exit A – Monday EOD
        "straddle_mon_exit":  round(exit_mon, 2),
        "profit_mon_pts":     round(profit_mon_pts, 2),
        "profit_mon_pct":     round(profit_mon_pct, 2),
        "win_mon":            int(profit_mon_pts > 0),
        # Exit B – Expiry
        "straddle_exp_exit":  round(exit_exp, 2),
        "profit_exp_pts":     round(profit_exp_pts, 2),
        "profit_exp_pct":     round(profit_exp_pct, 2),
        "win_exp":            int(profit_exp_pts > 0),
        # Weekend gap
        "gap_pct":            round(gap_pct, 4),
        "gap_pts":            round(gap_pts, 2),
        "weekend_decay_pts":  round(weekend_decay_pts, 2),
        # Context from Friday
        "iv_rank":            fri_data["iv_rank"],
        "atm_iv":             fri_data["atm_iv"],
        "pcr":                fri_data["pcr"],
    }


# ── Report ────────────────────────────────────────────────────────────────────

def print_report(df: pd.DataFrame):
    symbol = df["symbol"].iloc[0]
    n  = len(df)
    sep = "═" * 65

    print(f"\n{sep}")
    print(f"  {symbol} WEEKEND THETA HARVEST  —  {n} Fri→Mon→Tue triplets")
    print(sep)

    # ── Exit A: Monday EOD ────────────────────────────────────────────────────
    wr_mon  = df["win_mon"].mean() * 100
    avg_w   = df.loc[df["win_mon"]==1, "profit_mon_pts"].mean()
    avg_l   = df.loc[df["win_mon"]==0, "profit_mon_pts"].mean()
    exp_mon = wr_mon/100*(avg_w or 0) + (1-wr_mon/100)*(avg_l or 0)
    avg_decay = df["weekend_decay_pts"].mean()
    avg_entry = df["straddle_entry"].mean()
    decay_pct = avg_decay / avg_entry * 100 if avg_entry > 0 else 0

    print(f"\n── EXIT A: Sell Fri EOD → Buy back Mon EOD ──────────────────")
    print(f"  Win rate           : {wr_mon:.1f}%")
    print(f"  Avg profit (pts)   : {exp_mon:+.1f}")
    print(f"  Avg entry premium  : {avg_entry:.1f} pts")
    print(f"  Avg weekend decay  : {avg_decay:+.1f} pts  ({decay_pct:.1f}% of premium)")
    print(f"  Avg win            : {avg_w:+.1f} pts")
    print(f"  Avg loss           : {avg_l:+.1f} pts")

    # ── Exit B: Hold to expiry ────────────────────────────────────────────────
    wr_exp  = df["win_exp"].mean() * 100
    avg_w2  = df.loc[df["win_exp"]==1, "profit_exp_pts"].mean()
    avg_l2  = df.loc[df["win_exp"]==0, "profit_exp_pts"].mean()
    exp_exp = wr_exp/100*(avg_w2 or 0) + (1-wr_exp/100)*(avg_l2 or 0)

    print(f"\n── EXIT B: Sell Fri EOD → Hold to Tuesday Expiry ────────────")
    print(f"  Win rate           : {wr_exp:.1f}%")
    print(f"  Avg profit (pts)   : {exp_exp:+.1f}")
    print(f"  Avg win            : {avg_w2:+.1f} pts")
    print(f"  Avg loss           : {avg_l2:+.1f} pts")

    # ── Gap risk ──────────────────────────────────────────────────────────────
    big_gap = (df["gap_pct"].abs() >= 1.0).mean() * 100
    avg_gap = df["gap_pct"].mean()
    gap_hurt = df.loc[df["gap_pct"].abs() >= 1.0, "win_mon"].mean() * 100

    print(f"\n── MONDAY GAP RISK ───────────────────────────────────────────")
    print(f"  Avg Monday gap     : {avg_gap:+.2f}%")
    print(f"  Gap ≥1% frequency  : {big_gap:.1f}% of Mondays")
    if not df.loc[df["gap_pct"].abs() >= 1.0].empty:
        print(f"  Win rate on big gap: {gap_hurt:.1f}%  (vs {wr_mon:.1f}% overall)")

    # ── Conditional: by gap direction ────────────────────────────────────────
    print(f"\n── BY GAP DIRECTION (Mon open vs Fri close) ─────────────────")
    for label, mask in [("Gap Up >0.5%", df["gap_pct"] > 0.5),
                        ("Flat ±0.5%",   df["gap_pct"].abs() <= 0.5),
                        ("Gap Down <-0.5%", df["gap_pct"] < -0.5)]:
        sub = df[mask]
        if len(sub) < 3:
            continue
        wr = sub["win_mon"].mean() * 100
        ap = sub["profit_mon_pts"].mean()
        print(f"  {label:22s}  N={len(sub):3d}  Win={wr:.0f}%  AvgProfit={ap:+.1f}pts")

    # ── Conditional: by IV rank ───────────────────────────────────────────────
    if df["iv_rank"].sum() > 0:
        print(f"\n── BY IV RANK (Friday close) ─────────────────────────────")
        for label, lo, hi in [("Low  0–30", 0, 30), ("Mid 30–60", 30, 60), ("High 60+", 60, 101)]:
            sub = df[(df["iv_rank"] >= lo) & (df["iv_rank"] < hi)]
            if len(sub) < 3:
                continue
            wr = sub["win_mon"].mean() * 100
            ap = sub["profit_mon_pts"].mean()
            print(f"  {label:16s}  N={len(sub):3d}  Win={wr:.0f}%  AvgProfit={ap:+.1f}pts")

    # ── Comparison ────────────────────────────────────────────────────────────
    print(f"\n── COMPARISON: Exit A (Mon) vs Exit B (Expiry) ──────────────")
    print(f"  Exit Mon  : {wr_mon:.1f}% win,  {exp_mon:+.1f}pts expectancy")
    print(f"  Exit Exp  : {wr_exp:.1f}% win,  {exp_exp:+.1f}pts expectancy")
    better = "Monday exit" if exp_mon > exp_exp else "Hold to expiry"
    print(f"  Better exit: {better}")

    print(f"\n{sep}\n")


# ── CH persistence ────────────────────────────────────────────────────────────

def ensure_table(ch):
    ch.command("""
        CREATE TABLE IF NOT EXISTS analysis.weekend_theta_trades (
            expiry_date       Date,
            symbol            LowCardinality(String),
            friday_date       Date,
            monday_date       Date,
            spot_friday       Float64,
            spot_monday       Float64,
            spot_expiry       Float64,
            atm_strike        Float64,
            straddle_entry    Float64,
            straddle_mon_exit Float64,
            profit_mon_pts    Float64,
            profit_mon_pct    Float64,
            win_mon           Int8,
            straddle_exp_exit Float64,
            profit_exp_pts    Float64,
            profit_exp_pct    Float64,
            win_exp           Int8,
            gap_pct           Float64,
            gap_pts           Float64,
            weekend_decay_pts Float64,
            iv_rank           Float64,
            atm_iv            Float64,
            pcr               Float64,
            run_date          Date DEFAULT today()
        ) ENGINE = ReplacingMergeTree(run_date)
        ORDER BY (symbol, expiry_date)
    """)


def insert_results(ch, rows: list[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    ch.insert_df("analysis.weekend_theta_trades", df)
    log.info("Inserted %d rows into analysis.weekend_theta_trades", len(df))


# ── Main ──────────────────────────────────────────────────────────────────────

def run_symbol(ch, symbol: str, dry_run: bool):
    step = STRIKE_STEPS.get(symbol, 50)
    log.info("=== Weekend Theta Backtest: %s ===", symbol)

    trading_days = fetch_trading_days(ch, symbol)
    expiries     = fetch_expiries(ch, symbol)
    triplets     = build_triplets(trading_days, expiries)
    log.info("  %d expiries → %d Fri/Mon/Tue triplets", len(expiries), len(triplets))

    rows = []
    for i, (fri, mon, exp) in enumerate(triplets, 1):
        row = simulate_weekend(ch, symbol, fri, mon, exp, step)
        if row:
            rows.append(row)
        if i % 20 == 0:
            log.info("  Processed %d/%d triplets (%d valid)", i, len(triplets), len(rows))

    log.info("Valid triplets: %d / %d", len(rows), len(triplets))
    if not rows:
        log.warning("No valid data for %s", symbol)
        return

    df = pd.DataFrame(rows)
    print_report(df)

    if not dry_run:
        insert_results(ch, rows)
    else:
        log.info("Dry run — DB not written")


def main():
    parser = argparse.ArgumentParser(description="Weekend theta harvest backtest")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--symbol", default="NIFTY",
                        help=f"One of: {', '.join(SUPPORTED_SYMBOLS)}")
    parser.add_argument("--all", action="store_true",
                        help="Run all supported symbols")
    args = parser.parse_args()

    ch = get_ch()
    if not args.dry_run:
        ensure_table(ch)

    symbols = SUPPORTED_SYMBOLS if args.all else [args.symbol]
    for sym in symbols:
        run_symbol(ch, sym, args.dry_run)


if __name__ == "__main__":
    main()
