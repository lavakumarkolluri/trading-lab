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
from price_action import compute_price_action, PA_DEFAULTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CH_HOST     = os.getenv("CH_HOST", "clickhouse")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

STRIKE_STEPS     = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50}
LOT_SIZES        = {"NIFTY": 75, "BANKNIFTY": 35, "FINNIFTY": 40}
MIN_OI           = {"NIFTY": 10_000, "BANKNIFTY": 3_000, "FINNIFTY": 3_000}
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


def find_liquid_atm(df: pd.DataFrame, theoretical_atm: float,
                    step: int, symbol: str) -> tuple[float, float, float, bool]:
    """Pick the most liquid strike within ±3 steps of theoretical ATM."""
    min_oi = MIN_OI.get(symbol, 5_000)
    ce = df[df["option_type"] == "CE"].set_index("strike")
    pe = df[df["option_type"] == "PE"].set_index("strike")
    common = ce.index.intersection(pe.index)

    best_strike, best_total_oi, liquidity_ok = theoretical_atm, 0.0, False
    for offset in range(0, 4):
        for sign in ([0] if offset == 0 else [1, -1]):
            k = theoretical_atm + sign * offset * step
            if k not in common:
                continue
            oi_ce = float(ce.loc[k, "oi"]) if "oi" in ce.columns else 0.0
            oi_pe = float(pe.loc[k, "oi"]) if "oi" in pe.columns else 0.0
            total = oi_ce + oi_pe
            if oi_ce >= min_oi and oi_pe >= min_oi and total > best_total_oi:
                best_total_oi = total
                best_strike   = k
                liquidity_ok  = True

    ce_oi = float(ce.loc[best_strike, "oi"]) if best_strike in ce.index and "oi" in ce.columns else 0.0
    pe_oi = float(pe.loc[best_strike, "oi"]) if best_strike in pe.index and "oi" in pe.columns else 0.0
    return best_strike, ce_oi, pe_oi, liquidity_ok


def load_straddle(ch, symbol: str, snap_date: date, expiry: date, step: int) -> dict:
    """
    Return ATM straddle price and spot. Uses liquidity-aware ATM selection:
    picks the strike nearest to theoretical ATM that has OI above threshold on both legs.
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

    # Theoretical ATM via min |CE - PE| (put-call parity)
    theoretical_atm = float((ce.loc[common, "ltp"] - pe.loc[common, "ltp"]).abs().idxmin())
    # Apply liquidity check
    atm_strike, oi_ce, oi_pe, liq_ok = find_liquid_atm(df, theoretical_atm, step, symbol)

    ce_ltp = float(ce.loc[atm_strike, "ltp"]) if atm_strike in ce.index else 0.0
    pe_ltp = float(pe.loc[atm_strike, "ltp"]) if atm_strike in pe.index else 0.0
    straddle_price = ce_ltp + pe_ltp

    # Spot via put-call parity at theoretical ATM (more stable for spot estimation)
    spot = theoretical_atm + (
        float(ce.loc[theoretical_atm, "ltp"]) if theoretical_atm in ce.index else 0
    ) - (
        float(pe.loc[theoretical_atm, "ltp"]) if theoretical_atm in pe.index else 0
    )

    # PCR from full chain OI
    total_ce_oi = float(df[df["option_type"] == "CE"]["oi"].sum())
    total_pe_oi = float(df[df["option_type"] == "PE"]["oi"].sum())
    pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0.0

    # For NIFTY, prefer authoritative spot from options_eod_summary
    iv_rank = iv_pct = atm_iv = 0.0
    if symbol == "NIFTY":
        ctx = ch.query(
            "SELECT nifty_spot, iv_rank, iv_percentile, atm_ce_iv, atm_pe_iv "
            "FROM market.options_eod_summary FINAL "
            "WHERE date={d:Date} AND expiry={exp:Date}",
            parameters={"d": snap_date, "exp": expiry}
        )
        if ctx.result_rows:
            r = ctx.result_rows[0]
            spot   = float(r[0] or spot)
            iv_rank = float(r[1] or 0)
            iv_pct  = float(r[2] or 0)
            atm_iv  = float(r[3] or r[4] or 0)

    if not liq_ok:
        log.debug("Low liquidity for %s %s expiry=%s (CE OI=%.0f PE OI=%.0f)",
                  symbol, snap_date, expiry, oi_ce, oi_pe)

    return {
        "straddle":     round(straddle_price, 2),
        "spot":         round(spot, 2),
        "atm_strike":   atm_strike,
        "pcr":          round(pcr, 4),
        "iv_rank":      round(iv_rank, 2),
        "iv_pct":       round(iv_pct, 2),
        "atm_iv":       round(atm_iv, 2),
        "oi_ce":        round(oi_ce, 0),
        "oi_pe":        round(oi_pe, 0),
        "liquidity_ok": liq_ok,
    }


def straddle_intrinsic(strike: float, spot: float) -> float:
    return abs(spot - strike)


def fetch_friday_vix(ch, friday: date) -> float:
    """Return India VIX for the given Friday from market.nifty_live (0 if unavailable)."""
    result = ch.query(
        "SELECT vix FROM market.nifty_live "
        "WHERE toDate(timestamp)={d:Date} ORDER BY timestamp DESC LIMIT 1",
        parameters={"d": friday}
    )
    return float(result.result_rows[0][0]) if result.result_rows else 0.0


def has_high_impact_event(ch, friday: date, expiry: date) -> int:
    """Return 1 if a HIGH impact event falls between Friday and Tuesday expiry (inclusive)."""
    result = ch.query(
        "SELECT count() FROM market.events "
        "WHERE impact='HIGH' AND event_date >= {d_from:Date} AND event_date <= {d_to:Date}",
        parameters={"d_from": friday, "d_to": expiry}
    )
    return int(result.result_rows[0][0] > 0) if result.result_rows else 0


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
        # Liquidity at entry (Friday)
        "oi_ce":              fri_data["oi_ce"],
        "oi_pe":              fri_data["oi_pe"],
        "liquidity_ok":       int(fri_data["liquidity_ok"]),
        # Price action as of Friday
        **{k: v for k, v in compute_price_action(ch, symbol, friday).items()},
        # Gap risk filters
        "vix_friday":         fetch_friday_vix(ch, friday),
        "event_week":         has_high_impact_event(ch, friday, expiry),
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

    # ── Price action conditional ──────────────────────────────────────────────
    if "pa_available" in df.columns and df["pa_available"].sum() > 3:
        pa_df = df[df["pa_available"] == 1]
        print(f"\n── BY PRICE ACTION (Friday close) — {len(pa_df)} weeks with data ──")
        print(f"  {'Condition':26s}  {'N':>4s}  {'Win%':>6s}  {'AvgDecay':>10s}")

        for label, lo, hi in [("Tight (<0.6×ATR)",  0, 0.6),
                                ("Normal (0.6–1.2×)", 0.6, 1.2),
                                ("Wide (>1.2×ATR)",   1.2, 99)]:
            sub = pa_df[(pa_df["range_vs_atr"] >= lo) & (pa_df["range_vs_atr"] < hi)]
            if len(sub) < 3:
                continue
            wr = sub["win_mon"].mean() * 100
            ap = sub["weekend_decay_pts"].mean()
            print(f"  {label:26s}  {len(sub):>4d}  {wr:>5.1f}%  {ap:>+10.1f}")

        for label, val in [("Inside week bar", 1), ("Not inside week", 0)]:
            sub = pa_df[pa_df["inside_bar"] == val]
            if len(sub) < 3:
                continue
            wr = sub["win_mon"].mean() * 100
            ap = sub["weekend_decay_pts"].mean()
            print(f"  {label:26s}  {len(sub):>4d}  {wr:>5.1f}%  {ap:>+10.1f}")

        for label, val in [("Above MA20", 1), ("Below MA20", 0)]:
            sub = pa_df[pa_df["above_ma20"] == val]
            if len(sub) < 3:
                continue
            wr = sub["win_mon"].mean() * 100
            ap = sub["weekend_decay_pts"].mean()
            print(f"  {label:26s}  {len(sub):>4d}  {wr:>5.1f}%  {ap:>+10.1f}")

    # ── VIX filter impact ─────────────────────────────────────────────────────
    if df["vix_friday"].sum() > 0:
        print(f"\n── BY VIX (Friday close) ─────────────────────────────────────")
        for label, lo, hi in [("<13 Low",  0, 13), ("13–17 Med", 13, 17),
                               ("17–22 High", 17, 22), (">22 Fear", 22, 999)]:
            sub = df[(df["vix_friday"] > lo) & (df["vix_friday"] <= hi) & (df["vix_friday"] > 0)]
            if len(sub) < 3:
                continue
            wr = sub["win_mon"].mean() * 100
            ap = sub["profit_mon_pts"].mean()
            print(f"  {label:16s}  N={len(sub):3d}  Win={wr:.0f}%  AvgProfit={ap:+.1f}pts")

        skip_high_vix = df[df["vix_friday"].between(0.1, 17)]
        if len(skip_high_vix) > 3:
            wr2 = skip_high_vix["win_mon"].mean() * 100
            ap2 = skip_high_vix["profit_mon_pts"].mean()
            print(f"  → Filter VIX≤17 only: N={len(skip_high_vix)}  Win={wr2:.1f}%  AvgProfit={ap2:+.1f}pts")

    # ── Event week filter ─────────────────────────────────────────────────────
    if df["event_week"].sum() > 0:
        print(f"\n── EVENT WEEK FILTER ─────────────────────────────────────────")
        non_event = df[df["event_week"] == 0]
        event_wks = df[df["event_week"] == 1]
        if len(non_event) > 3:
            wr_ne = non_event["win_mon"].mean() * 100
            ap_ne = non_event["profit_mon_pts"].mean()
            print(f"  No event  N={len(non_event):3d}  Win={wr_ne:.1f}%  AvgProfit={ap_ne:+.1f}pts")
        if len(event_wks) > 0:
            wr_ev = event_wks["win_mon"].mean() * 100
            ap_ev = event_wks["profit_mon_pts"].mean()
            print(f"  Event wk  N={len(event_wks):3d}  Win={wr_ev:.1f}%  AvgProfit={ap_ev:+.1f}pts")

    # ── Gap stop simulation ───────────────────────────────────────────────────
    print(f"\n── GAP STOP SIMULATION (skip weeks gap_pct < -1.5%) ─────────")
    no_big_gap = df[df["gap_pct"] >= -1.5]
    big_gap_dn = df[df["gap_pct"] < -1.5]
    if len(no_big_gap) > 3:
        wr_ng = no_big_gap["win_mon"].mean() * 100
        ap_ng = no_big_gap["profit_mon_pts"].mean()
        print(f"  No large gap-dn  N={len(no_big_gap):3d}  Win={wr_ng:.1f}%  AvgProfit={ap_ng:+.1f}pts")
    if len(big_gap_dn) > 0:
        wr_bg = big_gap_dn["win_mon"].mean() * 100
        ap_bg = big_gap_dn["profit_mon_pts"].mean()
        print(f"  Gap-dn >1.5%     N={len(big_gap_dn):3d}  Win={wr_bg:.1f}%  AvgProfit={ap_bg:+.1f}pts  ← skip these")

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
            oi_ce             Float64 DEFAULT 0,
            oi_pe             Float64 DEFAULT 0,
            liquidity_ok      Int8    DEFAULT 0,
            prev_range_pct    Float64 DEFAULT 0,
            range_vs_atr      Float64 DEFAULT 0,
            inside_bar        Int8    DEFAULT 0,
            ma20_dist_pct     Float64 DEFAULT 0,
            ma50_dist_pct     Float64 DEFAULT 0,
            above_ma20        Int8    DEFAULT 0,
            week_range_pct    Float64 DEFAULT 0,
            consec_inside_days Int32  DEFAULT 0,
            pa_available      Int8    DEFAULT 0,
            vix_friday        Float64 DEFAULT 0,
            event_week        Int8    DEFAULT 0,
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
