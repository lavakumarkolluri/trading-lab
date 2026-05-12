#!/usr/bin/env python3
"""
edge_analysis.py
────────────────
Reverse-engineers straddle edge from historical expiries.
Supports NIFTY, BANKNIFTY, FINNIFTY (and any symbol in options_chain).

Two simulation modes:
  A. 1DTE: sell at T-1 EOD, hold to expiry EOD
  B. 0DTE: sell at estimated open on expiry day, hold to expiry EOD

Usage:
  python edge_analysis.py                        # NIFTY, write to CH
  python edge_analysis.py --dry-run              # report only
  python edge_analysis.py --symbol BANKNIFTY
  python edge_analysis.py --symbol FINNIFTY
  python edge_analysis.py --all                  # run all supported symbols
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

STRIKE_STEPS = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50}
DEFAULT_STRIKE_STEP  = 50
TRADING_HOURS        = 6.25          # market hours per day
TRADING_DAYS_YEAR    = 252
LOT_SIZES            = {"NIFTY": 75, "BANKNIFTY": 35, "FINNIFTY": 40}
MIN_OI               = {"NIFTY": 10_000, "BANKNIFTY": 3_000, "FINNIFTY": 3_000}
SUPPORTED_SYMBOLS    = list(STRIKE_STEPS.keys())

# ── Black-Scholes straddle price ──────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    return (1 + math.erf(x / math.sqrt(2))) / 2

def bs_straddle(spot: float, strike: float, T_years: float, iv: float) -> float:
    """ATM straddle price via Black-Scholes (r=0, European)."""
    if T_years <= 0 or iv <= 0 or spot <= 0:
        return 0.0
    d1 = (math.log(spot / strike) + 0.5 * iv**2 * T_years) / (iv * math.sqrt(T_years))
    d2 = d1 - iv * math.sqrt(T_years)
    call = spot * _norm_cdf(d1) - strike * _norm_cdf(d2)
    put  = strike * _norm_cdf(-d2) - spot * _norm_cdf(-d1)
    return call + put

# ── Data loaders ──────────────────────────────────────────────────────────────

def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )

def load_expiries(ch, symbol: str) -> list[date]:
    rows = ch.query(
        "SELECT DISTINCT expiry FROM market.options_chain FINAL "
        "WHERE symbol={sym:String} AND expiry >= '2024-01-01' "
        "ORDER BY expiry",
        parameters={"sym": symbol}
    ).result_rows
    return [r[0] for r in rows]

def load_options_day(ch, symbol: str, snap_date: date, expiry: date) -> pd.DataFrame:
    """Load all strikes for one snapshot day and expiry."""
    result = ch.query(
        "SELECT strike, option_type, ltp FROM market.options_chain FINAL "
        "WHERE symbol={sym:String} AND toDate(timestamp)={d:Date} "
        "  AND expiry={exp:Date} AND ltp > 0.1",
        parameters={"sym": symbol, "d": snap_date, "exp": expiry}
    )
    if not result.result_rows:
        return pd.DataFrame(columns=["strike", "option_type", "ltp"])
    df = pd.DataFrame(result.result_rows, columns=["strike", "option_type", "ltp"])
    df["strike"] = df["strike"].astype(float)
    df["ltp"]    = df["ltp"].astype(float)
    return df

def load_eod_context(ch, symbol: str, snap_date: date, expiry: date) -> dict:
    """
    Load IV rank, PCR, spot, ATM IV.
    For NIFTY: reads options_eod_summary (pre-computed).
    For other symbols: derives from options_chain using put-call parity.
    """
    if symbol == "NIFTY":
        result = ch.query(
            "SELECT nifty_spot, atm_strike, atm_ce_iv, atm_pe_iv, iv_rank, iv_percentile, pcr "
            "FROM market.options_eod_summary FINAL "
            "WHERE date={d:Date} AND expiry={exp:Date}",
            parameters={"d": snap_date, "exp": expiry}
        )
        if not result.result_rows:
            return {}
        r = result.result_rows[0]
        return {
            "spot": float(r[0] or 0),
            "atm_strike": float(r[1] or 0),
            "atm_ce_iv": float(r[2] or 0),
            "atm_pe_iv": float(r[3] or 0),
            "iv_rank": float(r[4] or 0),
            "iv_percentile": float(r[5] or 0),
            "pcr": float(r[6] or 0),
        }

    # ── Derive context from options_chain for BANKNIFTY / FINNIFTY ──────────────
    step = STRIKE_STEPS.get(symbol, 50)
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

    # Theoretical ATM via put-call parity (min |CE - PE|)
    theoretical_atm = float((ce.loc[common, "ltp"] - pe.loc[common, "ltp"]).abs().idxmin())
    # Spot via put-call parity
    ce_ltp = float(ce.loc[theoretical_atm, "ltp"]) if theoretical_atm in ce.index else 0
    pe_ltp = float(pe.loc[theoretical_atm, "ltp"]) if theoretical_atm in pe.index else 0
    spot = theoretical_atm + ce_ltp - pe_ltp

    # Liquidity-aware strike selection
    atm_strike, oi_ce, oi_pe, liq_ok = find_liquid_atm(df, theoretical_atm, step, symbol)

    # PCR from OI
    total_ce_oi = float(df[df["option_type"] == "CE"]["oi"].sum())
    total_pe_oi = float(df[df["option_type"] == "PE"]["oi"].sum())
    pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0.0

    return {
        "spot": round(spot, 2),
        "atm_strike": atm_strike,
        "atm_ce_iv": 0.0,
        "atm_pe_iv": 0.0,
        "iv_rank": 0.0,
        "iv_percentile": 0.0,
        "pcr": round(pcr, 4),
        "oi_ce": oi_ce,
        "oi_pe": oi_pe,
        "liquidity_ok": liq_ok,
    }

def load_fii_net(ch, as_of: date) -> float:
    """3-day FII net flow (crores). Falls back to participant OI proxy."""
    try:
        result = ch.query(
            "SELECT sum(net_value) FROM market.fii_dii FINAL "
            "WHERE entity='FII' AND date >= {d_from:Date} AND date <= {d_to:Date}",
            parameters={"d_from": as_of - timedelta(days=3), "d_to": as_of}
        )
        val = result.result_rows[0][0] if result.result_rows else None
        if val is not None:
            return float(val)
    except Exception:
        pass
    # fallback: participant OI futures net
    try:
        result = ch.query(
            "SELECT sum(fut_index_net) FROM market.participant_oi FINAL "
            "WHERE entity='FII' AND date >= {d_from:Date} AND date <= {d_to:Date}",
            parameters={"d_from": as_of - timedelta(days=3), "d_to": as_of}
        )
        val = result.result_rows[0][0] if result.result_rows else 0
        return float(val or 0) * 0.318  # empirical scale to crores
    except Exception:
        return 0.0

def prev_trading_day(ch, symbol: str, expiry: date) -> date | None:
    """Find the most recent data day strictly before expiry."""
    result = ch.query(
        "SELECT max(toDate(timestamp)) FROM market.options_chain FINAL "
        "WHERE symbol={sym:String} AND expiry={exp:Date} "
        "  AND toDate(timestamp) < {exp2:Date}",
        parameters={"sym": symbol, "exp": expiry, "exp2": expiry}
    )
    rows = result.result_rows
    if not rows or not rows[0][0]:
        return None
    return rows[0][0]

# ── Strike analysis helpers ───────────────────────────────────────────────────

def find_liquid_atm(df: pd.DataFrame, theoretical_atm: float,
                    step: int, symbol: str) -> tuple[float, float, float, bool]:
    """
    Pick the most liquid strike within ±3 steps of theoretical ATM.
    Liquidity = CE OI + PE OI above per-symbol threshold.
    Returns (atm_strike, oi_ce, oi_pe, liquidity_ok).
    Falls back to theoretical ATM if nothing meets the threshold.
    """
    min_oi = MIN_OI.get(symbol, 5_000)
    ce = df[df["option_type"] == "CE"].set_index("strike")
    pe = df[df["option_type"] == "PE"].set_index("strike")
    common = ce.index.intersection(pe.index)

    best_strike = theoretical_atm
    best_total_oi = 0.0
    liquidity_ok = False

    for offset in range(0, 4):           # check ATM, ±1, ±2, ±3 steps
        for sign in ([0] if offset == 0 else [1, -1]):
            k = theoretical_atm + sign * offset * step
            if k not in common:
                continue
            oi_ce = float(ce.loc[k, "oi"]) if "oi" in ce.columns else 0.0
            oi_pe = float(pe.loc[k, "oi"]) if "oi" in pe.columns else 0.0
            total  = oi_ce + oi_pe
            meets  = oi_ce >= min_oi and oi_pe >= min_oi
            if meets and total > best_total_oi:
                best_total_oi = total
                best_strike   = k
                liquidity_ok  = True

    # Read final OI at chosen strike
    oi_ce = float(ce.loc[best_strike, "oi"]) if best_strike in ce.index and "oi" in ce.columns else 0.0
    oi_pe = float(pe.loc[best_strike, "oi"]) if best_strike in pe.index and "oi" in pe.columns else 0.0
    return best_strike, oi_ce, oi_pe, liquidity_ok


def find_atm(df_t1: pd.DataFrame, spot: float, step: int, symbol: str = "NIFTY") -> tuple[float, float, float, bool]:
    """Round spot to nearest step, then apply liquidity check."""
    theoretical = round(spot / step) * step
    available = df_t1["strike"].unique()
    if theoretical not in available:
        theoretical = min(available, key=lambda k: abs(k - spot))
    return find_liquid_atm(df_t1, theoretical, step, symbol)

def straddle_at_strike(df: pd.DataFrame, strike: float) -> float:
    """CE + PE ltp at a given strike. 0 if either leg missing."""
    ce_rows = df[(df["strike"] == strike) & (df["option_type"] == "CE")]["ltp"]
    pe_rows = df[(df["strike"] == strike) & (df["option_type"] == "PE")]["ltp"]
    if ce_rows.empty or pe_rows.empty:
        return 0.0
    return float(ce_rows.iloc[0]) + float(pe_rows.iloc[0])

def straddle_intrinsic(strike: float, spot_at_expiry: float) -> float:
    """Intrinsic value of straddle at expiry = |spot - strike|."""
    return abs(spot_at_expiry - strike)

def best_strike_analysis(df_t1: pd.DataFrame, spot_expiry: float,
                         spot_t1: float, step: int) -> dict:
    """
    For all strikes within ±300 pts of T-1 ATM, compute profit if held to expiry.
    Returns dict with best_strike, best_profit_pts, best_profit_pct.
    """
    atm = round(spot_t1 / step) * step
    candidates = [atm + i * step for i in range(-6, 7)]
    results = []
    for k in candidates:
        entry = straddle_at_strike(df_t1, k)
        if entry < 5:
            continue
        exit_val = straddle_intrinsic(k, spot_expiry)
        profit_pts = entry - exit_val
        profit_pct = profit_pts / entry * 100
        results.append({"strike": k, "entry": entry, "profit_pts": profit_pts,
                         "profit_pct": profit_pct, "dist_from_atm": k - atm})
    if not results:
        return {"best_strike": atm, "best_profit_pts": 0, "best_profit_pct": 0,
                "best_dist_from_atm": 0, "atm_profit_pts": 0, "atm_profit_pct": 0}
    best = max(results, key=lambda x: x["profit_pct"])
    atm_row = next((r for r in results if r["strike"] == atm), None)
    return {
        "best_strike":       best["strike"],
        "best_profit_pts":   best["profit_pts"],
        "best_profit_pct":   best["profit_pct"],
        "best_dist_from_atm": best["dist_from_atm"],
        "atm_profit_pts":    atm_row["profit_pts"] if atm_row else 0,
        "atm_profit_pct":    atm_row["profit_pct"] if atm_row else 0,
    }

# ── Per-expiry simulation ─────────────────────────────────────────────────────

def simulate_expiry(ch, symbol: str, expiry: date, step: int) -> dict | None:
    """
    Simulate both 1DTE and 0DTE straddle for one expiry.
    Returns a row dict or None if data insufficient.
    """
    t1 = prev_trading_day(ch, symbol, expiry)
    if t1 is None:
        log.warning("No T-1 data for %s expiry=%s — skip", symbol, expiry)
        return None

    df_t1 = load_options_day(ch, symbol, t1, expiry)
    df_t  = load_options_day(ch, symbol, expiry, expiry)
    ctx_t1 = load_eod_context(ch, symbol, t1, expiry)
    ctx_t  = load_eod_context(ch, symbol, expiry, expiry)

    if df_t1.empty or ctx_t1.get("spot", 0) == 0:
        log.warning("Incomplete T-1 data for %s expiry=%s — skip", symbol, expiry)
        return None

    spot_t1      = ctx_t1["spot"]
    spot_expiry  = ctx_t.get("spot", 0) if ctx_t else 0
    atm_t1, oi_ce, oi_pe, liq_ok = find_atm(df_t1, spot_t1, step, symbol)
    iv_t1        = ctx_t1.get("atm_ce_iv", 0) or ctx_t1.get("atm_pe_iv", 0)
    day_of_week  = expiry.strftime("%A")

    if not liq_ok:
        log.debug("Low liquidity at ATM for %s expiry=%s (CE OI=%.0f PE OI=%.0f)",
                  symbol, expiry, oi_ce, oi_pe)

    # ── 1DTE scenario ─────────────────────────────────────────────────────────
    entry_1dte   = straddle_at_strike(df_t1, atm_t1)
    # Exit: if we have expiry day data use it; else use intrinsic from spot
    if not df_t.empty and spot_expiry > 0:
        exit_1dte = straddle_at_strike(df_t, atm_t1)
        if exit_1dte == 0:
            exit_1dte = straddle_intrinsic(atm_t1, spot_expiry)
    elif spot_expiry > 0:
        exit_1dte = straddle_intrinsic(atm_t1, spot_expiry)
    else:
        exit_1dte = 0.0

    profit_1dte_pts = entry_1dte - exit_1dte
    profit_1dte_pct = (profit_1dte_pts / entry_1dte * 100) if entry_1dte > 0 else 0.0

    # ── Realized move ─────────────────────────────────────────────────────────
    realized_move_pct = 0.0
    if spot_t1 > 0 and spot_expiry > 0:
        realized_move_pct = abs(spot_expiry - spot_t1) / spot_t1 * 100

    # ── Implied move (from ATM straddle / spot) ───────────────────────────────
    implied_move_pct = (entry_1dte / spot_t1 * 100) if spot_t1 > 0 and entry_1dte > 0 else 0.0
    vrp = implied_move_pct - realized_move_pct

    # ── 0DTE scenario (BS estimate for opening straddle) ──────────────────────
    # T = trading hours left at 9:30 open / (total trading hours × trading days/year)
    T_0dte = TRADING_HOURS / (TRADING_HOURS * TRADING_DAYS_YEAR)  # ≈ 1/252
    estimated_0dte_entry = bs_straddle(spot_t1, atm_t1, T_0dte, iv_t1 / 100) if iv_t1 > 0 else 0.0
    exit_0dte = straddle_intrinsic(atm_t1, spot_expiry) if spot_expiry > 0 else exit_1dte
    profit_0dte_pts = estimated_0dte_entry - exit_0dte
    profit_0dte_pct = (profit_0dte_pts / estimated_0dte_entry * 100) if estimated_0dte_entry > 0 else 0.0

    # ── Best strike analysis ──────────────────────────────────────────────────
    best = best_strike_analysis(df_t1, spot_expiry if spot_expiry > 0 else spot_t1,
                                spot_t1, step)

    # ── Context features ──────────────────────────────────────────────────────
    fii_net = load_fii_net(ch, t1)

    return {
        "expiry_date":            expiry,
        "symbol":                 symbol,
        "day_of_week":            day_of_week,
        "t1_date":                t1,
        "spot_t1":                round(spot_t1, 2),
        "spot_expiry":            round(spot_expiry, 2),
        "atm_strike":             atm_t1,
        # 1DTE metrics
        "entry_1dte":             round(entry_1dte, 2),
        "exit_1dte":              round(exit_1dte, 2),
        "profit_1dte_pts":        round(profit_1dte_pts, 2),
        "profit_1dte_pct":        round(profit_1dte_pct, 2),
        "win_1dte":               int(profit_1dte_pts > 0),
        # 0DTE metrics
        "estimated_0dte_entry":   round(estimated_0dte_entry, 2),
        "profit_0dte_pts":        round(profit_0dte_pts, 2),
        "profit_0dte_pct":        round(profit_0dte_pct, 2),
        "win_0dte":               int(profit_0dte_pts > 0),
        # VRP
        "implied_move_pct":       round(implied_move_pct, 4),
        "realized_move_pct":      round(realized_move_pct, 4),
        "vrp":                    round(vrp, 4),
        # Best strike
        "best_strike":            best["best_strike"],
        "best_profit_pts":        round(best["best_profit_pts"], 2),
        "best_profit_pct":        round(best["best_profit_pct"], 2),
        "best_dist_from_atm":     best["best_dist_from_atm"],
        "atm_profit_pts":         round(best["atm_profit_pts"], 2),
        # Context
        "iv_rank":                round(ctx_t1.get("iv_rank", 0), 2),
        "iv_percentile":          round(ctx_t1.get("iv_percentile", 0), 2),
        "atm_iv":                 round(iv_t1, 2),
        "pcr":                    round(ctx_t1.get("pcr", 0), 4),
        "fii_net_3d":             round(fii_net, 2),
        # Liquidity
        "oi_ce":                  round(oi_ce, 0),
        "oi_pe":                  round(oi_pe, 0),
        "liquidity_ok":           int(liq_ok),
        # Price action (computed from T-1 OHLCV)
        **{k: v for k, v in compute_price_action(ch, symbol, t1).items()},
    }

# ── Report ────────────────────────────────────────────────────────────────────

def _bucket(val: float, edges: list, labels: list) -> str:
    for i, edge in enumerate(edges):
        if val < edge:
            return labels[i]
    return labels[-1]

def print_report(df: pd.DataFrame):
    n = len(df)
    sep = "═" * 65

    symbol = df["symbol"].iloc[0] if "symbol" in df.columns else "UNKNOWN"
    print(f"\n{sep}")
    print(f"  {symbol} STRADDLE EDGE ANALYSIS  —  {n} expiries")
    print(sep)

    # ── 1. VRP ────────────────────────────────────────────────────────────────
    print("\n── 1. VOLATILITY RISK PREMIUM (VRP) ──────────────────────────")
    print(f"  Avg implied move   : {df['implied_move_pct'].mean():.3f}%")
    print(f"  Avg realized move  : {df['realized_move_pct'].mean():.3f}%")
    vrp_mean = df["vrp"].mean()
    vrp_pct  = (vrp_mean / df["implied_move_pct"].mean() * 100) if df["implied_move_pct"].mean() else 0
    print(f"  Avg VRP            : {vrp_mean:+.3f}%  ({vrp_pct:+.1f}% of implied)")
    t_stat, p_val = stats.ttest_1samp(df["vrp"].dropna(), 0)
    sig = "✅ STATISTICALLY SIGNIFICANT" if p_val < 0.05 else "⚠️  Not significant (p={:.2f})".format(p_val)
    print(f"  VRP t-test         : t={t_stat:.2f}, p={p_val:.4f}  →  {sig}")

    # ── 2. Win rates ──────────────────────────────────────────────────────────
    print("\n── 2. WIN RATES ───────────────────────────────────────────────")
    wr_1dte = df["win_1dte"].mean() * 100
    wr_0dte = df["win_0dte"].mean() * 100
    avg_win_1  = df.loc[df["win_1dte"]==1, "profit_1dte_pts"].mean()
    avg_loss_1 = df.loc[df["win_1dte"]==0, "profit_1dte_pts"].mean()
    avg_win_0  = df.loc[df["win_0dte"]==1, "profit_0dte_pts"].mean()
    avg_loss_0 = df.loc[df["win_0dte"]==0, "profit_0dte_pts"].mean()
    exp_1 = wr_1dte/100 * avg_win_1 + (1-wr_1dte/100) * avg_loss_1
    exp_0 = wr_0dte/100 * avg_win_0 + (1-wr_0dte/100) * avg_loss_0

    print(f"  1DTE (sell T-1 EOD): {wr_1dte:.1f}% win  | avg win={avg_win_1:.0f}pts  avg loss={avg_loss_1:.0f}pts  | expectancy={exp_1:+.0f}pts")
    print(f"  0DTE (sell at open): {wr_0dte:.1f}% win  | avg win={avg_win_0:.0f}pts  avg loss={avg_loss_0:.0f}pts  | expectancy={exp_0:+.0f}pts")

    # ── 3. Strike optimization ────────────────────────────────────────────────
    print("\n── 3. STRIKE OPTIMIZATION (1DTE, held to expiry) ─────────────")
    print(f"  ATM straddle       : avg profit {df['atm_profit_pts'].mean():+.1f}pts  ({(df['atm_profit_pts']>0).mean()*100:.0f}% win)")
    best_dist = df.groupby("best_dist_from_atm")["best_profit_pts"].count().reset_index()
    most_common = df["best_dist_from_atm"].value_counts().idxmax()
    print(f"  Best strike offset : {most_common:+.0f}pts from ATM most often ({df['best_dist_from_atm'].value_counts().iloc[0]} expiries)")
    print(f"  Best strike profit : avg {df['best_profit_pts'].mean():+.1f}pts vs ATM avg {df['atm_profit_pts'].mean():+.1f}pts")

    # ── 4. Conditional edge ───────────────────────────────────────────────────
    print("\n── 4. CONDITIONAL EDGE ────────────────────────────────────────")

    # IV rank buckets
    df["iv_bucket"] = df["iv_rank"].apply(
        lambda v: _bucket(v, [20, 40, 60, 80], ["0–20", "20–40", "40–60", "60–80", "80+"])
    )
    print("\n  By IV Rank:")
    print(f"  {'Bucket':10s}  {'N':>4s}  {'Win%':>6s}  {'AvgProfit':>10s}  {'Expectancy':>10s}")
    for bucket in ["0–20", "20–40", "40–60", "60–80", "80+"]:
        sub = df[df["iv_bucket"] == bucket]
        if len(sub) < 3:
            continue
        wr  = sub["win_1dte"].mean() * 100
        ap  = sub["profit_1dte_pts"].mean()
        aw  = sub.loc[sub["win_1dte"]==1, "profit_1dte_pts"].mean()
        al  = sub.loc[sub["win_1dte"]==0, "profit_1dte_pts"].mean()
        exp = wr/100*(aw or 0) + (1-wr/100)*(al or 0)
        print(f"  {bucket:10s}  {len(sub):>4d}  {wr:>5.1f}%  {ap:>+10.1f}  {exp:>+10.1f}")

    # VIX (using atm_iv as proxy)
    df["vix_bucket"] = df["atm_iv"].apply(
        lambda v: _bucket(v, [12, 16, 20, 25], ["<12", "12–16", "16–20", "20–25", ">25"])
    )
    print("\n  By ATM IV (proxy for VIX):")
    print(f"  {'Bucket':10s}  {'N':>4s}  {'Win%':>6s}  {'AvgProfit':>10s}  {'Expectancy':>10s}")
    for bucket in ["<12", "12–16", "16–20", "20–25", ">25"]:
        sub = df[df["vix_bucket"] == bucket]
        if len(sub) < 3:
            continue
        wr  = sub["win_1dte"].mean() * 100
        ap  = sub["profit_1dte_pts"].mean()
        aw  = sub.loc[sub["win_1dte"]==1, "profit_1dte_pts"].mean()
        al  = sub.loc[sub["win_1dte"]==0, "profit_1dte_pts"].mean()
        exp = wr/100*(aw or 0) + (1-wr/100)*(al or 0)
        print(f"  {bucket:10s}  {len(sub):>4d}  {wr:>5.1f}%  {ap:>+10.1f}  {exp:>+10.1f}")

    # PCR
    df["pcr_bucket"] = df["pcr"].apply(
        lambda v: _bucket(v, [0.7, 1.0, 1.3], ["<0.7 Bear", "0.7–1.0", "1.0–1.3", ">1.3 Bull"])
    )
    print("\n  By PCR (Put-Call Ratio):")
    print(f"  {'Bucket':12s}  {'N':>4s}  {'Win%':>6s}  {'AvgProfit':>10s}")
    for bucket in ["<0.7 Bear", "0.7–1.0", "1.0–1.3", ">1.3 Bull"]:
        sub = df[df["pcr_bucket"] == bucket]
        if len(sub) < 3:
            continue
        wr = sub["win_1dte"].mean() * 100
        ap = sub["profit_1dte_pts"].mean()
        print(f"  {bucket:12s}  {len(sub):>4d}  {wr:>5.1f}%  {ap:>+10.1f}")

    # Day of week
    print("\n  By Day of Week (expiry day):")
    print(f"  {'Day':12s}  {'N':>4s}  {'Win%':>6s}  {'AvgProfit':>10s}")
    for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]:
        sub = df[df["day_of_week"] == day]
        if len(sub) < 3:
            continue
        wr = sub["win_1dte"].mean() * 100
        ap = sub["profit_1dte_pts"].mean()
        print(f"  {day:12s}  {len(sub):>4d}  {wr:>5.1f}%  {ap:>+10.1f}")

    # ── 5. Stop/target calibration ────────────────────────────────────────────
    print("\n── 5. STOP / TARGET CALIBRATION (0DTE, % of collected premium) ─")
    pcts = df["profit_0dte_pct"].dropna()
    for target in [20, 30, 40, 50, 60, 70, 80]:
        hit_rate = (pcts >= target).mean() * 100
        print(f"  Target {target:>3d}% of premium  → hit in {hit_rate:.1f}% of expiries")
    print()
    for stop in [-20, -30, -50, -80, -100]:
        breach_rate = (pcts <= stop).mean() * 100
        print(f"  Stop  {stop:>4d}% of premium  → breached in {breach_rate:.1f}% of expiries")

    # ── 6. Feature correlation ────────────────────────────────────────────────
    print("\n── 6. FEATURE CORRELATION WITH 1DTE WIN ──────────────────────")
    features = ["iv_rank", "atm_iv", "pcr", "fii_net_3d", "implied_move_pct", "iv_percentile",
                "prev_range_pct", "range_vs_atr", "ma20_dist_pct", "week_range_pct", "consec_inside_days"]
    print(f"  {'Feature':26s}  {'Pearson r':>10s}  {'p-value':>10s}  {'Signal':>8s}")
    for feat in features:
        if feat not in df.columns:
            continue
        sub = df[[feat, "win_1dte"]].dropna()
        if len(sub) < 10 or sub[feat].std() == 0:
            continue
        r, p = stats.pearsonr(sub[feat], sub["win_1dte"])
        direction = "↑ win" if r > 0 else "↓ win"
        sig = "**" if p < 0.05 else ("*" if p < 0.10 else "")
        print(f"  {feat:26s}  {r:>+10.3f}  {p:>10.4f}  {direction} {sig}")

    # ── Price action conditional edge ─────────────────────────────────────────
    if "pa_available" in df.columns and df["pa_available"].sum() > 5:
        pa_df = df[df["pa_available"] == 1]
        print(f"\n── 6b. PRICE ACTION CONDITIONAL EDGE ({len(pa_df)} expiries with data) ──")

        # Range compression
        print(f"\n  By prior-day range vs ATR (compression ratio):")
        print(f"  {'Bucket':20s}  {'N':>4s}  {'Win%':>6s}  {'AvgProfit':>10s}")
        for label, lo, hi in [("Tight (<0.6×ATR)", 0, 0.6), ("Normal (0.6–1.2×)", 0.6, 1.2),
                                ("Wide (>1.2×ATR)",  1.2, 99)]:
            sub = pa_df[(pa_df["range_vs_atr"] >= lo) & (pa_df["range_vs_atr"] < hi)]
            if len(sub) < 3:
                continue
            wr = sub["win_1dte"].mean() * 100
            ap = sub["profit_1dte_pts"].mean()
            print(f"  {label:20s}  {len(sub):>4d}  {wr:>5.1f}%  {ap:>+10.1f}")

        # Inside bar
        for label, val in [("Inside week bar", 1), ("Not inside week", 0)]:
            sub = pa_df[pa_df["inside_bar"] == val]
            if len(sub) < 3:
                continue
            wr = sub["win_1dte"].mean() * 100
            ap = sub["profit_1dte_pts"].mean()
            print(f"\n  {label}: N={len(sub)}, Win={wr:.1f}%, AvgProfit={ap:+.1f}pts")

        # MA position
        print(f"\n  By MA20 position:")
        for label, val in [("Above MA20", 1), ("Below MA20", 0)]:
            sub = pa_df[pa_df["above_ma20"] == val]
            if len(sub) < 3:
                continue
            wr = sub["win_1dte"].mean() * 100
            ap = sub["profit_1dte_pts"].mean()
            print(f"  {label}: N={len(sub)}, Win={wr:.1f}%, AvgProfit={ap:+.1f}pts")

    # ── 7. Best trading conditions (combined filter) ───────────────────────────
    print("\n── 7. BEST TRADING CONDITIONS (combined) ─────────────────────")
    best_mask = (
        (df["iv_rank"] >= 20) & (df["iv_rank"] <= 70) &
        (df["atm_iv"] >= 12) & (df["atm_iv"] <= 22) &
        (df["pcr"] >= 0.7) & (df["pcr"] <= 1.3)
    )
    worst_mask = ~best_mask
    best_sub  = df[best_mask]
    worst_sub = df[worst_mask]
    if len(best_sub) > 0:
        print(f"  TRADE (IV rank 20–70, ATM IV 12–22, PCR 0.7–1.3):")
        print(f"    N={len(best_sub)}, Win={best_sub['win_1dte'].mean()*100:.1f}%, Avg profit={best_sub['profit_1dte_pts'].mean():+.1f}pts")
    if len(worst_sub) > 0:
        print(f"  SKIP (outside those bounds):")
        print(f"    N={len(worst_sub)}, Win={worst_sub['win_1dte'].mean()*100:.1f}%, Avg profit={worst_sub['profit_1dte_pts'].mean():+.1f}pts")

    print(f"\n{sep}\n")

# ── CH persistence ────────────────────────────────────────────────────────────

def ensure_table(ch):
    ch.command("""
        CREATE TABLE IF NOT EXISTS analysis.edge_analysis (
            expiry_date          Date,
            symbol               LowCardinality(String),
            day_of_week          String,
            t1_date              Date,
            spot_t1              Float64,
            spot_expiry          Float64,
            atm_strike           Float64,
            entry_1dte           Float64,
            exit_1dte            Float64,
            profit_1dte_pts      Float64,
            profit_1dte_pct      Float64,
            win_1dte             Int8,
            estimated_0dte_entry Float64,
            profit_0dte_pts      Float64,
            profit_0dte_pct      Float64,
            win_0dte             Int8,
            implied_move_pct     Float64,
            realized_move_pct    Float64,
            vrp                  Float64,
            best_strike          Float64,
            best_profit_pts      Float64,
            best_profit_pct      Float64,
            best_dist_from_atm   Float64,
            atm_profit_pts       Float64,
            iv_rank              Float64,
            iv_percentile        Float64,
            atm_iv               Float64,
            pcr                  Float64,
            fii_net_3d           Float64,
            oi_ce                Float64 DEFAULT 0,
            oi_pe                Float64 DEFAULT 0,
            liquidity_ok         Int8    DEFAULT 0,
            prev_range_pct       Float64 DEFAULT 0,
            range_vs_atr         Float64 DEFAULT 0,
            inside_bar           Int8    DEFAULT 0,
            ma20_dist_pct        Float64 DEFAULT 0,
            ma50_dist_pct        Float64 DEFAULT 0,
            above_ma20           Int8    DEFAULT 0,
            week_range_pct       Float64 DEFAULT 0,
            consec_inside_days   Int32   DEFAULT 0,
            pa_available         Int8    DEFAULT 0,
            run_date             Date DEFAULT today()
        ) ENGINE = ReplacingMergeTree(run_date)
        ORDER BY (symbol, expiry_date)
    """)

def insert_results(ch, rows: list[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    ch.insert_df("analysis.edge_analysis", df)
    log.info("Inserted %d rows into analysis.edge_analysis", len(df))

# ── Main ──────────────────────────────────────────────────────────────────────

def run_symbol(ch, symbol: str, dry_run: bool):
    step = STRIKE_STEPS.get(symbol, DEFAULT_STRIKE_STEP)
    log.info("=== Edge Analysis: %s (step=%d) ===", symbol, step)

    expiries = load_expiries(ch, symbol)
    log.info("Found %d expiry dates", len(expiries))

    rows = []
    for i, exp in enumerate(expiries, 1):
        row = simulate_expiry(ch, symbol, exp, step)
        if row:
            rows.append(row)
        if i % 20 == 0:
            log.info("  Processed %d/%d expiries (%d valid)", i, len(expiries), len(rows))

    log.info("Valid expiries: %d / %d", len(rows), len(expiries))

    if not rows:
        log.warning("No valid data for %s", symbol)
        return

    df = pd.DataFrame(rows)
    print_report(df)

    if not dry_run:
        insert_results(ch, rows)
        log.info("Results saved to analysis.edge_analysis for %s", symbol)
    else:
        log.info("Dry run — DB not written")


def main():
    parser = argparse.ArgumentParser(description="Straddle edge analysis")
    parser.add_argument("--dry-run", action="store_true", help="Print report only, no DB write")
    parser.add_argument("--symbol", default="NIFTY",
                        help=f"Symbol to analyse: {', '.join(SUPPORTED_SYMBOLS)}")
    parser.add_argument("--all", action="store_true",
                        help="Run analysis for all supported symbols")
    args = parser.parse_args()

    ch = get_ch()
    if not args.dry_run:
        ensure_table(ch)

    symbols = SUPPORTED_SYMBOLS if args.all else [args.symbol]
    for sym in symbols:
        run_symbol(ch, sym, args.dry_run)


if __name__ == "__main__":
    main()
