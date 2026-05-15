#!/usr/bin/env python3
"""
options_eod_summary_pipeline.py
─────────────────────────────────
Computes per-date option metrics from market.options_chain EOD data
and populates market.options_eod_summary.

Computed from bhavcopy (OI available, IV not):
  - PCR         : total PE OI / total CE OI for near-term expiry
  - max_pain    : strike minimising total payout to option buyers
  - atm_strike  : strike closest to spot (estimated via put-call parity)
  - nifty_spot  : estimated from ATM put-call parity
  - total_ce_oi / total_pe_oi

Not computable from bhavcopy (left as 0):
  - iv_rank, iv_percentile, atm_ce_iv, atm_pe_iv
    (NSE bhavcopy does not publish IV; intraday scraper captures it going forward)

Usage:
  python options_eod_summary_pipeline.py
  python options_eod_summary_pipeline.py --symbol NIFTY
  python options_eod_summary_pipeline.py --from 2024-01-01

Docker:
  docker compose run --rm pipeline python options_eod_summary_pipeline.py
"""

import os
import sys
import argparse
from datetime import date, datetime

import pandas as pd
import numpy as np

from ch_utils import ch_client as get_ch
from logging_utils import get_logger
log = get_logger(__name__)

SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]   # CRIT-003: all symbols


def fetch_already_computed(ch) -> set:
    rows = ch.query(
        "SELECT DISTINCT (date, symbol) FROM market.options_eod_summary FINAL"
        if _has_symbol_col(ch)
        else "SELECT DISTINCT date FROM market.options_eod_summary FINAL"
    ).result_rows
    return {r[0] for r in rows}


def _has_symbol_col(ch) -> bool:
    cols = [r[0] for r in ch.query("DESCRIBE market.options_eod_summary").result_rows]
    return "symbol" in cols


_VALID_SYMBOL_RE = __import__("re").compile(r"^[A-Z]{2,20}$")


def _validate_symbol(symbol: str) -> str:
    if not _VALID_SYMBOL_RE.match(symbol):
        raise ValueError(f"Invalid symbol: {symbol!r}")
    return symbol


def fetch_dates(ch, symbol: str, from_date: date) -> list[date]:
    _validate_symbol(symbol)
    rows = ch.query(
        "SELECT DISTINCT toDate(timestamp) as d "
        "FROM market.options_chain "
        "WHERE symbol={sym:String} AND toDate(timestamp) >= {fd:Date} "
        "ORDER BY d",
        parameters={"sym": symbol, "fd": from_date},
    ).result_rows
    return [r[0] for r in rows]


def fetch_chain_for_date(ch, symbol: str, d: date) -> pd.DataFrame:
    _validate_symbol(symbol)
    df = ch.query_df(
        "SELECT expiry, strike, option_type, ltp, oi, iv "
        "FROM market.options_chain FINAL "
        "WHERE symbol={sym:String} AND toDate(timestamp)={d:Date} AND oi > 0",
        parameters={"sym": symbol, "d": d},
    )
    return df


def near_term_expiry(df: pd.DataFrame, d: date):
    """Nearest expiry on or after d."""
    d_ts = pd.Timestamp(d)
    expiries = df[df["expiry"] >= d_ts]["expiry"].unique()
    if len(expiries) == 0:
        return None
    return min(expiries)


def estimate_spot(df_expiry: pd.DataFrame) -> float:
    """
    Estimate spot price using put-call parity:
      spot ≈ strike + CE_ltp - PE_ltp  (at any strike, best at ATM)
    Use the strike where |CE_ltp - PE_ltp| is minimised.
    """
    ce = df_expiry[df_expiry["option_type"] == "CE"].set_index("strike")["ltp"]
    pe = df_expiry[df_expiry["option_type"] == "PE"].set_index("strike")["ltp"]
    common = ce.index.intersection(pe.index)
    if common.empty:
        return 0.0
    diff = (ce[common] - pe[common]).abs()
    atm_strike = diff.idxmin()
    spot = atm_strike + ce.get(atm_strike, 0) - pe.get(atm_strike, 0)
    return float(spot)


def atm_strike_from_spot(df_expiry: pd.DataFrame, spot: float) -> float:
    strikes = df_expiry["strike"].unique()
    if len(strikes) == 0:
        return 0.0
    return float(min(strikes, key=lambda s: abs(s - spot)))


def compute_pcr(df_expiry: pd.DataFrame) -> tuple[int, int, float]:
    ce_oi = int(df_expiry[df_expiry["option_type"] == "CE"]["oi"].sum())
    pe_oi = int(df_expiry[df_expiry["option_type"] == "PE"]["oi"].sum())
    pcr = pe_oi / ce_oi if ce_oi > 0 else 0.0
    return ce_oi, pe_oi, pcr


def compute_max_pain(df_expiry: pd.DataFrame) -> float:
    """
    Max pain = strike that minimises total payout from option writers to buyers.

    At expiry price S:
      payout = sum_{K < S} (S-K)*CE_OI[K]  +  sum_{K > S} (K-S)*PE_OI[K]

    We try every unique strike as a candidate expiry price.
    """
    strikes = sorted(df_expiry["strike"].unique())
    if not strikes:
        return 0.0

    ce = df_expiry[df_expiry["option_type"] == "CE"].set_index("strike")["oi"]
    pe = df_expiry[df_expiry["option_type"] == "PE"].set_index("strike")["oi"]

    ce_arr = np.array([ce.get(s, 0) for s in strikes], dtype=np.float64)
    pe_arr = np.array([pe.get(s, 0) for s in strikes], dtype=np.float64)
    s_arr  = np.array(strikes, dtype=np.float64)

    min_pain = np.inf
    pain_strike = strikes[0]

    for i, S in enumerate(strikes):
        ce_pain = float(np.sum(np.maximum(S - s_arr, 0) * ce_arr))
        pe_pain = float(np.sum(np.maximum(s_arr - S, 0) * pe_arr))
        total   = ce_pain + pe_pain
        if total < min_pain:
            min_pain   = total
            pain_strike = S

    return float(pain_strike)


def process_date(ch, symbol: str, d: date) -> dict | None:
    df = fetch_chain_for_date(ch, symbol, d)
    if df.empty:
        return None

    # Deduplicate multiple intraday snapshots: keep last row per (expiry, strike, option_type)
    df = df.groupby(["expiry", "strike", "option_type"], as_index=False).last()

    expiry = near_term_expiry(df, d)
    if expiry is None:
        return None

    df_exp = df[df["expiry"] == expiry]
    if df_exp.empty:
        return None

    spot       = estimate_spot(df_exp)
    atm        = atm_strike_from_spot(df_exp, spot)
    ce_oi, pe_oi, pcr = compute_pcr(df_exp)
    max_pain   = compute_max_pain(df_exp)

    # ATM IV — 0 for bhavcopy (IV not published)
    atm_ce_iv = float(df_exp[(df_exp["strike"] == atm) & (df_exp["option_type"] == "CE")]["iv"].sum())
    atm_pe_iv = float(df_exp[(df_exp["strike"] == atm) & (df_exp["option_type"] == "PE")]["iv"].sum())

    return {
        "symbol":         symbol,
        "date":           d,
        "expiry":         expiry,
        "nifty_spot":     spot,
        "total_ce_oi":    ce_oi,
        "total_pe_oi":    pe_oi,
        "pcr":            pcr,
        "max_pain_strike": max_pain,
        "atm_strike":     atm,
        "atm_ce_iv":      atm_ce_iv,
        "atm_pe_iv":      atm_pe_iv,
        "iv_rank":        0.0,
        "iv_percentile":  0.0,
        "version":        int(datetime.utcnow().timestamp()),
    }


def insert_rows(ch, rows: list[dict]):
    df = pd.DataFrame(rows)
    cols = ["date", "expiry", "nifty_spot", "total_ce_oi", "total_pe_oi",
            "pcr", "max_pain_strike", "atm_strike", "atm_ce_iv", "atm_pe_iv",
            "iv_rank", "iv_percentile", "version"]
    if _has_symbol_col(ch):
        cols = ["symbol"] + cols
    ch.insert_df("market.options_eod_summary", df[cols])


def main():
    parser = argparse.ArgumentParser(description="Compute options EOD summary from chain data")
    parser.add_argument("--symbol", default=None, help="NIFTY or BANKNIFTY (default: both)")
    parser.add_argument("--from",   dest="from_date", default="2024-01-01",
                        help="Start date YYYY-MM-DD (default: 2024-01-01)")
    parser.add_argument("--status", action="store_true", help="Show current summary stats and exit")
    args = parser.parse_args()

    ch = get_ch()

    if args.status:
        rows = ch.query(
            "SELECT toYear(date) as yr, count(), min(date), max(date), "
            "round(avg(pcr),3) as avg_pcr, round(avg(max_pain_strike),0) as avg_mp "
            "FROM market.options_eod_summary FINAL "
            "GROUP BY yr ORDER BY yr"
        ).result_rows
        print(f"\n{'Year':>6} {'Rows':>6} {'First':>12} {'Last':>12} {'Avg PCR':>8} {'Avg MaxPain':>12}")
        print("-" * 60)
        for r in rows:
            print(f"{r[0]:>6} {r[1]:>6} {str(r[2]):>12} {str(r[3]):>12} {r[4]:>8} {r[5]:>12.0f}")
        return

    from_date = date.fromisoformat(args.from_date)
    symbols = [args.symbol] if args.symbol else SYMBOLS

    for symbol in symbols:
        log.info(f"=== Processing {symbol} from {from_date} ===")
        dates = fetch_dates(ch, symbol, from_date)

        # Skip already computed (date, symbol) pairs
        if _has_symbol_col(ch):
            done = ch.query(
                "SELECT DISTINCT date FROM market.options_eod_summary FINAL "
                "WHERE symbol={sym:String} AND date >= {fd:Date}",
                parameters={"sym": symbol, "fd": from_date},
            ).result_rows
        else:
            done = ch.query(
                "SELECT DISTINCT date FROM market.options_eod_summary FINAL "
                "WHERE date >= {fd:Date}",
                parameters={"fd": from_date},
            ).result_rows
        done_dates = {r[0] for r in done}
        dates = [d for d in dates if d not in done_dates]

        log.info(f"  {len(dates)} dates to process")
        if not dates:
            log.info("  Already up to date.")
            continue

        batch = []
        for i, d in enumerate(dates, 1):
            row = process_date(ch, symbol, d)
            if row is None:
                log.warning(f"  [{i}/{len(dates)}] {d}: no data — skip")
                continue
            batch.append(row)
            log.info(
                f"  [{i}/{len(dates)}] {d}: expiry={row['expiry']} "
                f"pcr={row['pcr']:.3f} max_pain={row['max_pain_strike']:.0f} "
                f"spot≈{row['nifty_spot']:.0f}"
            )
            if len(batch) >= 50:
                insert_rows(ch, batch)
                log.info(f"  → Inserted batch of {len(batch)} rows")
                batch = []

        if batch:
            insert_rows(ch, batch)
            log.info(f"  → Inserted final batch of {len(batch)} rows")

    log.info("Done. Running status:")
    rows = ch.query(
        "SELECT count(), min(date), max(date), round(avg(pcr),3) "
        "FROM market.options_eod_summary FINAL"
    ).result_rows[0]
    log.info(f"  {rows[0]} rows | {rows[1]} → {rows[2]} | avg PCR={rows[3]}")


if __name__ == "__main__":
    main()
