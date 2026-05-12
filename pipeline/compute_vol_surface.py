#!/usr/bin/env python3
"""
compute_vol_surface.py
──────────────────────
Builds the volatility surface from options_chain (requires iv > 0 — run
compute_greeks.py first for historical data).

For each trading date × symbol:
  1. Load all option rows with iv > 0 from options_chain
  2. Derive spot via put-call parity per (expiry, timestamp)
  3. Compute moneyness = strike / spot; filter to [0.80, 1.20]
  4. Insert into market.vol_surface (strike-level)
  5. Compute and insert into market.vol_term_structure:
       • ATM IV interpolated to 7/14/30/60/90 DTE
       • Term slope (30d - 7d ATM IV)
       • Put-call skew at 1%, 2%, 3%, 5% OTM

Usage:
  python compute_vol_surface.py                        # all symbols, all dates
  python compute_vol_surface.py --symbol NIFTY
  python compute_vol_surface.py --from 2024-01-01 --to 2024-12-31
  python compute_vol_surface.py --workers 4
  python compute_vol_surface.py --dry-run
"""

import argparse
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import clickhouse_connect
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CH_HOST     = os.getenv("CH_HOST", "clickhouse")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

SYMBOLS          = ["NIFTY", "BANKNIFTY", "FINNIFTY"]
MONEYNESS_MIN    = 0.80
MONEYNESS_MAX    = 1.20
MIN_LTP          = 5.0          # filter deep OTM / illiquid strikes by price
TARGET_DTES      = [7, 14, 30, 60, 90]
SKEW_MONEYNESS   = [0.99, 0.98, 0.97, 0.95]   # PE side; CE mirrors at 1.01, 1.02, 1.03, 1.05


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def derive_spot(df: pd.DataFrame) -> dict:
    """Return {expiry: spot} via put-call parity (min |CE-PE|)."""
    spots = {}
    for expiry, grp in df.groupby("expiry"):
        ce = grp[grp["option_type"] == "CE"].set_index("strike")["ltp"]
        pe = grp[grp["option_type"] == "PE"].set_index("strike")["ltp"]
        common = ce.index.intersection(pe.index)
        if common.empty:
            continue
        atm = float((ce.loc[common] - pe.loc[common]).abs().idxmin())
        spots[expiry] = atm + float(ce.loc[atm]) - float(pe.loc[atm])
    return spots


def interp_atm_iv(atm_by_dte: dict, target_dte: int) -> float:
    """
    Linear interpolation of ATM IV at target_dte from available (dte, iv) pairs.
    Returns 0 if insufficient data.
    """
    dtes = sorted(atm_by_dte.keys())
    if not dtes:
        return 0.0
    if target_dte <= dtes[0]:
        return atm_by_dte[dtes[0]]
    if target_dte >= dtes[-1]:
        return atm_by_dte[dtes[-1]]
    for i in range(len(dtes) - 1):
        d1, d2 = dtes[i], dtes[i + 1]
        if d1 <= target_dte <= d2:
            w = (target_dte - d1) / (d2 - d1)
            return atm_by_dte[d1] * (1 - w) + atm_by_dte[d2] * w
    return 0.0


def skew_at_moneyness(surf: pd.DataFrame, pe_m: float, tol: float = 0.015) -> float:
    """
    Mean PE IV in [pe_m - tol, pe_m + tol] minus mean CE IV in mirror band.
    Returns 0 if either side has no data.
    """
    ce_m = 2.0 - pe_m   # mirror: pe_m=0.98 → ce_m=1.02
    pe_iv = surf[(surf["option_type"] == "PE") &
                 surf["moneyness"].between(pe_m - tol, pe_m + tol)]["iv"]
    ce_iv = surf[(surf["option_type"] == "CE") &
                 surf["moneyness"].between(ce_m - tol, ce_m + tol)]["iv"]
    if pe_iv.empty or ce_iv.empty:
        return 0.0
    return round(float(pe_iv.mean()) - float(ce_iv.mean()), 6)


# ── Per-day processing ────────────────────────────────────────────────────────

def process_day(ch, symbol: str, dt: date, dry_run: bool) -> tuple[int, int]:
    """Returns (surface_rows, term_rows) inserted."""
    result = ch.query(
        "SELECT expiry, strike, option_type, ltp, iv, delta "
        "FROM market.options_chain FINAL "
        "WHERE symbol={sym:String} AND toDate(timestamp)={d:Date} "
        "  AND iv > 0 AND ltp >= {min_ltp:Float64} "
        "ORDER BY expiry, strike",
        parameters={"sym": symbol, "d": dt, "min_ltp": MIN_LTP}
    )
    if not result.result_rows:
        return 0, 0

    df = pd.DataFrame(result.result_rows,
                      columns=["expiry", "strike", "option_type", "ltp", "iv", "delta"])
    df["strike"] = df["strike"].astype(float)
    df["ltp"]    = df["ltp"].astype(float)
    df["iv"]     = df["iv"].astype(float)
    df["delta"]  = df["delta"].astype(float)

    spots = derive_spot(df)
    if not spots:
        return 0, 0

    # ── Build surface rows ────────────────────────────────────────────────────
    surface_rows = []
    atm_iv_by_dte = {}   # {dte: atm_iv} for term structure

    for expiry, grp in df.groupby("expiry"):
        spot = spots.get(expiry)
        if not spot or spot <= 0:
            continue
        dte = (expiry - dt).days
        if dte < 0:
            continue

        grp = grp.copy()
        grp["moneyness"] = grp["strike"] / spot
        grp = grp[grp["moneyness"].between(MONEYNESS_MIN, MONEYNESS_MAX)]

        for _, row in grp.iterrows():
            surface_rows.append({
                "date":        dt,
                "symbol":      symbol,
                "expiry":      expiry,
                "dte":         dte,
                "strike":      row["strike"],
                "moneyness":   round(row["moneyness"], 6),
                "option_type": row["option_type"],
                "iv":          row["iv"],
                "delta":       row["delta"],
                "ltp":         row["ltp"],
                "oi":          0,
            })

        # ATM IV for term structure: average CE+PE iv at moneyness closest to 1.0
        atm_band = grp[grp["moneyness"].between(0.99, 1.01)]
        if not atm_band.empty:
            atm_iv_by_dte[dte] = float(atm_band["iv"].mean())

    # ── Term structure + skew ─────────────────────────────────────────────────
    term_rows = []
    if len(atm_iv_by_dte) >= 2:
        interp = {t: interp_atm_iv(atm_iv_by_dte, t) for t in TARGET_DTES}
        iv_7d  = interp[7]
        iv_30d = interp[30]

        # Use a representative spot (earliest expiry)
        rep_spot = next(iter(spots.values()))
        surf_all = pd.DataFrame(surface_rows)

        skews = []
        for pe_m in SKEW_MONEYNESS:
            skews.append(skew_at_moneyness(surf_all, pe_m))

        term_rows.append({
            "date":       dt,
            "symbol":     symbol,
            "spot":       round(rep_spot, 2),
            "atm_iv_7d":  round(iv_7d,        6),
            "atm_iv_14d": round(interp[14],    6),
            "atm_iv_30d": round(iv_30d,        6),
            "atm_iv_60d": round(interp[60],    6),
            "atm_iv_90d": round(interp[90],    6),
            "term_slope": round(iv_30d - iv_7d, 6),
            "skew_1pct":  skews[0],
            "skew_2pct":  skews[1],
            "skew_3pct":  skews[2],
            "skew_5pct":  skews[3],
        })

    if not dry_run:
        if surface_rows:
            ch.insert_df("market.vol_surface", pd.DataFrame(surface_rows))
        if term_rows:
            ch.insert_df("market.vol_term_structure", pd.DataFrame(term_rows))

    return len(surface_rows), len(term_rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol",    default=None)
    parser.add_argument("--from",      dest="from_date", default="2019-01-01")
    parser.add_argument("--to",        dest="to_date",   default=str(date.today()))
    parser.add_argument("--workers",   type=int, default=4)
    parser.add_argument("--dry-run",   action="store_true")
    args = parser.parse_args()

    ch_main = get_ch()
    symbols = [args.symbol] if args.symbol else SYMBOLS
    from_dt = date.fromisoformat(args.from_date)
    to_dt   = date.fromisoformat(args.to_date)

    for symbol in symbols:
        log.info("=== Vol Surface: %s (%s → %s) workers=%d ===",
                 symbol, from_dt, to_dt, args.workers)

        # Dates with iv data not yet in vol_surface
        all_dates = {r[0] for r in ch_main.query(
            "SELECT DISTINCT toDate(timestamp) FROM market.options_chain FINAL "
            "WHERE symbol={sym:String} "
            "  AND toDate(timestamp) BETWEEN {d_from:Date} AND {d_to:Date} "
            "  AND iv > 0",
            parameters={"sym": symbol, "d_from": from_dt, "d_to": to_dt}
        ).result_rows}
        done_dates = {r[0] for r in ch_main.query(
            "SELECT DISTINCT date FROM market.vol_surface FINAL "
            "WHERE symbol={sym:String} "
            "  AND date BETWEEN {d_from:Date} AND {d_to:Date}",
            parameters={"sym": symbol, "d_from": from_dt, "d_to": to_dt}
        ).result_rows}
        dates = sorted(all_dates - done_dates)

        log.info("  %d dates to process", len(dates))
        if not dates:
            log.info("  Nothing to do — already up to date")
            continue

        total_surf = total_term = completed = 0

        def _worker(dt):
            ch = get_ch()
            return dt, process_day(ch, symbol, dt, args.dry_run)

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(_worker, dt): dt for dt in dates}
            for fut in as_completed(futures):
                dt, (n_surf, n_term) = fut.result()
                total_surf += n_surf
                total_term += n_term
                completed  += 1
                if completed % 100 == 0 or completed == len(dates):
                    log.info("  [%d/%d] surf_rows=%d  term_rows=%d",
                             completed, len(dates), total_surf, total_term)

        log.info("  Done: %d surface rows, %d term rows for %s",
                 total_surf, total_term, symbol)


if __name__ == "__main__":
    main()
