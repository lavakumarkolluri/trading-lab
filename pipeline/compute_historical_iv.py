#!/usr/bin/env python3
"""
compute_historical_iv.py
─────────────────────────
Computes historical ATM implied volatility from NSE bhavcopy option prices
using the Black-Scholes model, then updates market.options_eod_summary with:

  atm_ce_iv      — IV of ATM call (%)
  atm_pe_iv      — IV of ATM put  (%)
  iv_rank        — (today_iv - 52wk_low) / (52wk_high - 52wk_low) * 100
  iv_percentile  — % of past 252 days where IV was lower than today

Spot source: market.nifty_live.nifty_spot (^NSEI via yfinance — real index level).

Black-Scholes inversion via Newton-Raphson (≤10 iterations, converges in ~3).
Risk-free rate: 6.5% (approximate India repo rate over the period).

Usage:
  python compute_historical_iv.py
  python compute_historical_iv.py --from 2024-01-01

Docker:
  docker compose run --rm pipeline python compute_historical_iv.py
"""

import os
import math
import logging
import argparse
from datetime import date, datetime

import pandas as pd
import numpy as np
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

RISK_FREE_RATE = 0.065   # India repo rate ~6.5%
MIN_PRICE      = 0.05    # ignore options below this price (stale/illiquid)
MAX_IV         = 2.0     # cap IV at 200% to filter bad data
IV_WINDOW      = 252     # trading days for iv_rank / iv_percentile


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


# ── Black-Scholes ─────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def bs_price(S: float, K: float, T: float, r: float, sigma: float, is_call: bool) -> float:
    """Black-Scholes option price."""
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if is_call else (K - S))
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if is_call:
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def bs_vega(S: float, K: float, T: float, r: float, sigma: float) -> float:
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    return S * _norm_pdf(d1) * math.sqrt(T)


def implied_vol(market_price: float, S: float, K: float, T: float,
                r: float, is_call: bool, tol: float = 1e-6) -> float | None:
    """Newton-Raphson IV solver. Returns None if no solution found."""
    if market_price < MIN_PRICE or T <= 0:
        return None
    intrinsic = max(0.0, (S - K) if is_call else (K - S))
    if market_price < intrinsic:
        return None

    sigma = 0.3   # initial guess: 30% vol
    for _ in range(50):
        price = bs_price(S, K, T, r, sigma, is_call)
        vega  = bs_vega(S, K, T, r, sigma)
        if vega < 1e-10:
            return None
        diff  = price - market_price
        sigma -= diff / vega
        sigma  = max(1e-4, min(sigma, MAX_IV))
        if abs(diff) < tol:
            return sigma
    return sigma if 0 < sigma < MAX_IV else None


# ── Data loading ──────────────────────────────────────────

def load_spot(ch) -> dict:
    """date → nifty_spot from market.nifty_live."""
    rows = ch.query(
        "SELECT toDate(timestamp), argMax(nifty_spot, timestamp) "
        "FROM market.nifty_live FINAL "
        "GROUP BY toDate(timestamp)"
    ).result_rows
    return {r[0]: float(r[1]) for r in rows if r[1] and r[1] > 0}


def load_atm_options(ch, d: date, spot: float) -> pd.DataFrame:
    """
    Load CE and PE rows near ATM for the near-term expiry on date d.
    ATM window: spot ± 2% (captures a few strikes around ATM for robustness).
    """
    lo = spot * 0.98
    hi = spot * 1.02

    # Near-term expiry
    expiry_row = ch.query(
        f"SELECT min(expiry) FROM market.options_chain "
        f"WHERE symbol='NIFTY' AND toDate(timestamp)='{d}' AND expiry >= '{d}'"
    ).result_rows
    if not expiry_row or expiry_row[0][0] is None:
        return pd.DataFrame()
    expiry = expiry_row[0][0]

    df = ch.query_df(
        f"SELECT strike, option_type, ltp "
        f"FROM market.options_chain FINAL "
        f"WHERE symbol='NIFTY' AND toDate(timestamp)='{d}' "
        f"  AND expiry='{expiry}' "
        f"  AND strike BETWEEN {lo} AND {hi} "
        f"  AND ltp > {MIN_PRICE}"
    )
    df["expiry"] = expiry
    return df


def compute_iv_for_date(d: date, spot: float, df: pd.DataFrame) -> dict | None:
    """Compute average ATM IV from CE and PE options near the money."""
    if df.empty or spot <= 0:
        return None

    # T in years (calendar days / 365 — standard for NSE options)
    ce_ivs, pe_ivs = [], []

    for _, row in df.iterrows():
        K      = float(row["strike"])
        price  = float(row["ltp"])
        expiry = row["expiry"]
        T      = (pd.Timestamp(expiry) - pd.Timestamp(d)).days / 365.0
        is_call = row["option_type"] == "CE"

        iv = implied_vol(price, spot, K, T, RISK_FREE_RATE, is_call)
        if iv is None:
            continue
        if is_call:
            ce_ivs.append(iv)
        else:
            pe_ivs.append(iv)

    atm_ce_iv = float(np.median(ce_ivs)) * 100 if ce_ivs else 0.0
    atm_pe_iv = float(np.median(pe_ivs)) * 100 if pe_ivs else 0.0

    if atm_ce_iv == 0 and atm_pe_iv == 0:
        return None

    return {"atm_ce_iv": atm_ce_iv, "atm_pe_iv": atm_pe_iv}


def compute_iv_rank_percentile(iv_series: pd.Series) -> pd.DataFrame:
    """
    Given a time-ordered Series of daily IV values, compute rolling
    iv_rank and iv_percentile over the past IV_WINDOW trading days.
    """
    results = []
    vals = iv_series.values
    dates = iv_series.index

    for i, d in enumerate(dates):
        window = vals[max(0, i - IV_WINDOW + 1): i + 1]
        iv = vals[i]
        lo, hi = window.min(), window.max()
        iv_rank = ((iv - lo) / (hi - lo) * 100) if hi > lo else 50.0
        iv_pct  = float((window < iv).sum()) / len(window) * 100
        results.append({"date": d, "iv_rank": iv_rank, "iv_percentile": iv_pct})

    return pd.DataFrame(results).set_index("date")


# ── Update ────────────────────────────────────────────────

def update_summary(ch, updates: list[dict]):
    """
    Insert updated rows into options_eod_summary.
    ReplacingMergeTree will keep the highest version on merge.
    We read existing rows first to preserve pcr/max_pain etc.
    """
    if not updates:
        return

    dates_str = ", ".join(f"'{u['date']}'" for u in updates)
    existing = ch.query_df(
        f"SELECT * FROM market.options_eod_summary FINAL "
        f"WHERE date IN ({dates_str})"
    )
    existing["date"] = pd.to_datetime(existing["date"]).dt.date

    now     = datetime.utcnow()
    version = int(now.timestamp())
    rows    = []

    for u in updates:
        match = existing[existing["date"] == u["date"]]
        if match.empty:
            continue
        r = match.iloc[0].to_dict()
        r["atm_ce_iv"]     = u["atm_ce_iv"]
        r["atm_pe_iv"]     = u["atm_pe_iv"]
        r["iv_rank"]       = u["iv_rank"]
        r["iv_percentile"] = u["iv_percentile"]
        r["version"]       = version
        rows.append(r)

    if not rows:
        return

    df = pd.DataFrame(rows)
    ch.insert_df("market.options_eod_summary", df[[
        "date", "expiry", "nifty_spot", "total_ce_oi", "total_pe_oi",
        "pcr", "max_pain_strike", "atm_strike", "atm_ce_iv", "atm_pe_iv",
        "iv_rank", "iv_percentile", "version",
    ]])


# ── Main ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compute historical IV and update options_eod_summary")
    parser.add_argument("--from", dest="from_date", default="2024-01-01",
                        help="Start date YYYY-MM-DD (default: 2024-01-01)")
    args = parser.parse_args()

    from_date = date.fromisoformat(args.from_date)
    ch = get_ch()

    log.info("Loading spot prices from market.nifty_live...")
    spot_map = load_spot(ch)
    log.info(f"  {len(spot_map)} dates with spot data ({min(spot_map)} → {max(spot_map)})")

    log.info("Loading dates from market.options_eod_summary...")
    rows = ch.query(
        f"SELECT date FROM market.options_eod_summary FINAL "
        f"WHERE date >= '{from_date}' ORDER BY date"
    ).result_rows
    dates = [r[0] for r in rows]
    log.info(f"  {len(dates)} dates to process")

    # Compute IV for each date
    iv_by_date = {}
    for i, d in enumerate(dates, 1):
        spot = spot_map.get(d)
        if not spot:
            log.warning(f"  [{i}/{len(dates)}] {d}: no spot data — skip")
            continue

        df = load_atm_options(ch, d, spot)
        result = compute_iv_for_date(d, spot, df)
        if result is None:
            log.warning(f"  [{i}/{len(dates)}] {d}: IV not computable — skip")
            continue

        iv_by_date[d] = result
        if i % 50 == 0:
            log.info(f"  [{i}/{len(dates)}] computed — latest: {d} CE_IV={result['atm_ce_iv']:.1f}% PE_IV={result['atm_pe_iv']:.1f}%")

    log.info(f"IV computed for {len(iv_by_date)} dates")

    # Compute iv_rank and iv_percentile from the full IV time series
    log.info("Computing iv_rank and iv_percentile...")
    iv_series = pd.Series(
        {d: (v["atm_ce_iv"] + v["atm_pe_iv"]) / 2 for d, v in sorted(iv_by_date.items())}
    )
    rank_df = compute_iv_rank_percentile(iv_series)

    # Merge and prepare updates
    updates = []
    for d, ivs in iv_by_date.items():
        if d not in rank_df.index:
            continue
        updates.append({
            "date":         d,
            "atm_ce_iv":    ivs["atm_ce_iv"],
            "atm_pe_iv":    ivs["atm_pe_iv"],
            "iv_rank":      float(rank_df.loc[d, "iv_rank"]),
            "iv_percentile": float(rank_df.loc[d, "iv_percentile"]),
        })

    log.info(f"Updating {len(updates)} rows in market.options_eod_summary...")
    # Insert in batches
    batch_size = 100
    for start in range(0, len(updates), batch_size):
        batch = updates[start: start + batch_size]
        update_summary(ch, batch)
        log.info(f"  Batch {start//batch_size + 1}: inserted {len(batch)} rows")

    log.info("Done. Sample results:")
    sample = ch.query(
        "SELECT date, round(atm_ce_iv,1), round(atm_pe_iv,1), "
        "round(iv_rank,1), round(iv_percentile,1) "
        "FROM market.options_eod_summary FINAL "
        "WHERE atm_ce_iv > 0 "
        "ORDER BY date DESC LIMIT 10"
    ).result_rows
    print(f"\n{'Date':>12} {'CE_IV%':>8} {'PE_IV%':>8} {'IV_Rank':>9} {'IV_Pct':>8}")
    print("-" * 50)
    for r in sample:
        print(f"{str(r[0]):>12} {r[1]:>8.1f} {r[2]:>8.1f} {r[3]:>9.1f} {r[4]:>8.1f}")


if __name__ == "__main__":
    main()
