#!/usr/bin/env python3
"""
compute_greeks.py
─────────────────
Back-fill Black-Scholes Greeks (IV, delta, theta) for all historical rows
in market.options_chain where delta = 0.

For each trading day × symbol:
  1. Load all options rows (strike, expiry, option_type, ltp)
  2. Derive spot per (expiry, snapshot_timestamp) via put-call parity
  3. Compute IV from LTP via Newton-Raphson
  4. Compute delta and theta from IV
  5. Re-insert rows with updated delta/theta (new version → ReplacingMergeTree keeps latest)

Usage:
  python compute_greeks.py                        # all symbols, all unprocessed dates
  python compute_greeks.py --symbol NIFTY
  python compute_greeks.py --from 2024-01-01 --to 2024-12-31
  python compute_greeks.py --dry-run
"""

import argparse
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import clickhouse_connect
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CH_HOST     = os.getenv("CH_HOST", "clickhouse")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

RISK_FREE_RATE = 0.065
SYMBOLS        = ["NIFTY", "BANKNIFTY", "FINNIFTY"]


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )


# ── Black-Scholes ─────────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _bs_call(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return max(S - K, 0.0)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)


def _bs_put(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return max(K - S, 0.0)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def _implied_vol(S, K, T, r, market_price, is_call):
    if T <= 0 or market_price <= 0 or S <= 0 or K <= 0:
        return 0.0
    intrinsic = max(S - K, 0.0) if is_call else max(K - S, 0.0)
    if market_price <= intrinsic * 1.001:
        return 0.0
    sigma = 0.25
    for _ in range(60):
        price = _bs_call(S, K, T, r, sigma) if is_call else _bs_put(S, K, T, r, sigma)
        d1    = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        vega  = S * math.sqrt(T) * math.exp(-0.5 * d1 ** 2) / math.sqrt(2 * math.pi)
        if vega < 1e-8:
            break
        diff  = price - market_price
        sigma -= diff / vega
        sigma  = max(0.001, min(sigma, 10.0))
        if abs(diff) < 0.01:
            return sigma
    return sigma


def compute_greeks_row(S, K, T, r, ltp, is_call):
    """Return (iv, delta, theta) for one option row."""
    iv = _implied_vol(S, K, T, r, ltp, is_call)
    if iv <= 0 or T <= 0:
        delta = 0.5 if is_call else -0.5
        theta = 0.0
        return iv, delta, theta

    d1  = (math.log(S / K) + (r + 0.5 * iv ** 2) * T) / (iv * math.sqrt(T))
    d2  = d1 - iv * math.sqrt(T)
    nd1 = _norm_cdf(d1)
    pdf_d1 = math.exp(-0.5 * d1 ** 2) / math.sqrt(2 * math.pi)

    delta = nd1 if is_call else nd1 - 1.0

    # Theta (per calendar day, not annualised)
    theta_annual = (
        -(S * pdf_d1 * iv) / (2 * math.sqrt(T))
        - r * K * math.exp(-r * T) * (_norm_cdf(d2) if is_call else _norm_cdf(-d2))
        + (0 if is_call else r * K * math.exp(-r * T))
    )
    theta = theta_annual / 365.0

    return round(iv, 6), round(delta, 6), round(theta, 6)


# ── Per-day processing ────────────────────────────────────────────────────────

def derive_spot(df_day: pd.DataFrame) -> dict:
    """
    Return {(expiry, timestamp): spot} via put-call parity.
    Uses the strike where |CE_ltp - PE_ltp| is minimised.
    """
    spots = {}
    for (expiry, ts), grp in df_day.groupby(["expiry", "timestamp"]):
        ce = grp[grp["option_type"] == "CE"].set_index("strike")["ltp"]
        pe = grp[grp["option_type"] == "PE"].set_index("strike")["ltp"]
        common = ce.index.intersection(pe.index)
        if common.empty:
            continue
        atm = float((ce.loc[common] - pe.loc[common]).abs().idxmin())
        spot = atm + float(ce.loc[atm]) - float(pe.loc[atm])
        spots[(expiry, ts)] = spot
    return spots


def process_day(ch, symbol: str, dt: date, dry_run: bool) -> int:
    result = ch.query(
        "SELECT timestamp, expiry, strike, option_type, ltp, oi, oi_change, volume, version "
        "FROM market.options_chain "
        "WHERE symbol={sym:String} AND toDate(timestamp)={d:Date} AND ltp > 0.1 "
        "ORDER BY timestamp, expiry, strike",
        parameters={"sym": symbol, "d": dt}
    )
    if not result.result_rows:
        return 0

    df = pd.DataFrame(result.result_rows,
                      columns=["timestamp", "expiry", "strike", "option_type",
                               "ltp", "oi", "oi_change", "volume", "version"])
    df["strike"] = df["strike"].astype(float)
    df["ltp"]    = df["ltp"].astype(float)

    spots = derive_spot(df)
    if not spots:
        return 0

    rows_out = []
    for _, row in df.iterrows():
        key = (row["expiry"], row["timestamp"])
        S   = spots.get(key)
        if S is None or S <= 0:
            continue
        K   = row["strike"]
        T   = max((row["expiry"] - dt).days / 365.0, 1 / 365.0)
        is_call = row["option_type"] == "CE"
        iv, delta, theta = compute_greeks_row(S, K, T, RISK_FREE_RATE, row["ltp"], is_call)
        rows_out.append({
            "symbol":      symbol,
            "timestamp":   row["timestamp"],
            "expiry":      row["expiry"],
            "strike":      K,
            "option_type": row["option_type"],
            "ltp":         row["ltp"],
            "iv":          iv,
            "delta":       delta,
            "theta":       theta,
            "oi":          int(row["oi"]),
            "oi_change":   int(row["oi_change"]),
            "volume":      int(row["volume"]),
            "version":     int(row["version"]) + 1,
        })

    if not rows_out:
        return 0

    if not dry_run:
        df_out = pd.DataFrame(rows_out)
        ch.insert_df("market.options_chain", df_out[[
            "symbol", "timestamp", "expiry", "strike", "option_type",
            "ltp", "iv", "delta", "theta", "oi", "oi_change", "volume", "version",
        ]])

    return len(rows_out)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=None,
                        help="Single symbol (default: all)")
    parser.add_argument("--from", dest="from_date", default="2019-01-01")
    parser.add_argument("--to",   dest="to_date",   default=str(date.today()))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--workers", type=int, default=4,
                        help="Parallel threads per symbol (default: 4)")
    args = parser.parse_args()

    ch_main  = get_ch()
    symbols  = [args.symbol] if args.symbol else SYMBOLS
    from_dt  = date.fromisoformat(args.from_date)
    to_dt    = date.fromisoformat(args.to_date)

    for symbol in symbols:
        log.info("=== Computing Greeks: %s (%s → %s) workers=%d ===",
                 symbol, from_dt, to_dt, args.workers)

        dates = [r[0] for r in ch_main.query(
            "SELECT DISTINCT toDate(timestamp) as dt FROM market.options_chain FINAL "
            "WHERE symbol={sym:String} AND toDate(timestamp) BETWEEN {d_from:Date} AND {d_to:Date} "
            "  AND (delta = 0 OR oi = 0) AND ltp > 0.1 ORDER BY dt",
            parameters={"sym": symbol, "d_from": from_dt, "d_to": to_dt}
        ).result_rows]
        log.info("  %d dates to process", len(dates))

        total = 0
        completed = 0

        def _worker(dt):
            ch = get_ch()   # each thread gets its own connection
            return dt, process_day(ch, symbol, dt, args.dry_run)

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(_worker, dt): dt for dt in dates}
            for fut in as_completed(futures):
                dt, n = fut.result()
                total     += n
                completed += 1
                if completed % 50 == 0 or completed == len(dates):
                    log.info("  [%d/%d] last=%s rows_this_batch=%d total=%d",
                             completed, len(dates), dt, n, total)

        log.info("  Done: %d rows updated for %s", total, symbol)


if __name__ == "__main__":
    main()
