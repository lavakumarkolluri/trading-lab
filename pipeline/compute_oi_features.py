#!/usr/bin/env python3
"""
compute_oi_features.py
───────────────────────
Computes OI-based features and IV skew from market.options_chain
and stores them in market.options_eod_summary.

New features added:
  ce_wall_strike    — strike with highest CE OI (resistance)
  pe_wall_strike    — strike with highest PE OI (support)
  iv_skew           — avg(PE IV OTM) - avg(CE IV OTM)  [+ve = fear/put buying]
  fii_fut_net       — FII net futures index position from participant_oi

Also adds to analysis.pattern_features (for Indian/BSE market rows):
  fii_fut_net_3d    — 3-day rolling FII futures net position

Usage:
  python compute_oi_features.py
  python compute_oi_features.py --from 2024-01-01
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

RISK_FREE_RATE = 0.065
MIN_PRICE      = 0.5     # minimum option price for IV calc
OTM_RANGE      = 0.03    # strikes 1-3% OTM for skew calculation


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


# ── BS IV (same as compute_historical_iv) ─────────────────

def _norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def _norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def bs_price(S, K, T, r, sigma, is_call):
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if is_call else (K - S))
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if is_call:
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)

def bs_vega(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    return S * _norm_pdf(d1) * math.sqrt(T)

def implied_vol(price, S, K, T, r, is_call):
    if price < MIN_PRICE or T <= 0:
        return None
    sigma = 0.25
    for _ in range(50):
        p   = bs_price(S, K, T, r, sigma, is_call)
        v   = bs_vega(S, K, T, r, sigma)
        if v < 1e-10:
            return None
        sigma -= (p - price) / v
        sigma  = max(1e-4, min(sigma, 2.0))
        if abs(p - price) < 1e-6:
            return sigma
    return sigma if 0 < sigma < 2.0 else None


# ── OI wall features ──────────────────────────────────────

def compute_oi_walls(df_chain: pd.DataFrame) -> dict:
    """Max CE OI strike (resistance) and max PE OI strike (support)."""
    ce = df_chain[df_chain["option_type"] == "CE"]
    pe = df_chain[df_chain["option_type"] == "PE"]
    ce_wall = float(ce.loc[ce["oi"].idxmax(), "strike"]) if not ce.empty else 0.0
    pe_wall = float(pe.loc[pe["oi"].idxmax(), "strike"]) if not pe.empty else 0.0
    return {"ce_wall_strike": ce_wall, "pe_wall_strike": pe_wall}


# ── IV skew ───────────────────────────────────────────────

def compute_iv_skew(df_chain: pd.DataFrame, spot: float, expiry, d: date) -> float:
    """
    IV skew = median(PE IV at 1-3% OTM) - median(CE IV at 1-3% OTM)
    Positive = puts more expensive than calls = fear/hedging demand.
    """
    T = (pd.Timestamp(expiry) - pd.Timestamp(d)).days / 365.0
    if T <= 0:
        return 0.0

    lo_otm = spot * (1 - OTM_RANGE)
    hi_otm = spot * (1 + OTM_RANGE)

    # OTM puts: strike < spot
    pe_otm = df_chain[
        (df_chain["option_type"] == "PE") &
        (df_chain["strike"] >= lo_otm) &
        (df_chain["strike"] < spot)
    ]
    # OTM calls: strike > spot
    ce_otm = df_chain[
        (df_chain["option_type"] == "CE") &
        (df_chain["strike"] > spot) &
        (df_chain["strike"] <= hi_otm)
    ]

    pe_ivs, ce_ivs = [], []
    for _, row in pe_otm.iterrows():
        iv = implied_vol(float(row["ltp"]), spot, float(row["strike"]), T, RISK_FREE_RATE, False)
        if iv:
            pe_ivs.append(iv)
    for _, row in ce_otm.iterrows():
        iv = implied_vol(float(row["ltp"]), spot, float(row["strike"]), T, RISK_FREE_RATE, True)
        if iv:
            ce_ivs.append(iv)

    if not pe_ivs or not ce_ivs:
        return 0.0
    return float((np.median(pe_ivs) - np.median(ce_ivs)) * 100)


# ── FII futures net ───────────────────────────────────────

def load_fii_futures(ch, from_date: date) -> pd.DataFrame:
    df = ch.query_df(f"""
        SELECT date, fut_index_net
        FROM market.participant_oi
        WHERE entity = 'FII' AND date >= '{from_date}'
        ORDER BY date
    """)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.set_index("date").sort_index()
    df["fii_fut_net_3d"] = df["fut_index_net"].rolling(3).sum()
    return df


# ── Check schema for new columns ──────────────────────────

def ensure_columns(ch):
    existing = [r[0] for r in ch.query("DESCRIBE market.options_eod_summary").result_rows]
    for col, dtype in [("ce_wall_strike", "Float64"), ("pe_wall_strike", "Float64"), ("iv_skew", "Float64")]:
        if col not in existing:
            ch.command(f"ALTER TABLE market.options_eod_summary ADD COLUMN IF NOT EXISTS {col} {dtype} DEFAULT 0")
            log.info(f"  Added column {col} to market.options_eod_summary")


# ── Main ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compute OI walls, IV skew, FII futures features")
    parser.add_argument("--from", dest="from_date", default="2024-01-01")
    args = parser.parse_args()
    from_date = date.fromisoformat(args.from_date)

    ch = get_ch()
    ensure_columns(ch)

    # Load spot and expiry summary
    log.info("Loading options_eod_summary...")
    summary = ch.query_df(f"""
        SELECT date, expiry, nifty_spot, atm_strike
        FROM market.options_eod_summary FINAL
        WHERE date >= '{from_date}'
        ORDER BY date
    """)
    summary["date"]   = pd.to_datetime(summary["date"]).dt.date
    summary["expiry"] = pd.to_datetime(summary["expiry"]).dt.date
    log.info(f"  {len(summary)} dates")

    # Load FII futures
    log.info("Loading FII futures positions...")
    fii_fut = load_fii_futures(ch, from_date)
    log.info(f"  {len(fii_fut)} dates, range {fii_fut.index.min()} → {fii_fut.index.max()}")

    updates = []
    for i, row in summary.iterrows():
        d      = row["date"]
        expiry = row["expiry"]
        spot   = float(row["nifty_spot"])
        if spot <= 0:
            continue

        # Load chain for this date
        df_chain = ch.query_df(f"""
            SELECT strike, option_type, ltp, oi
            FROM market.options_chain FINAL
            WHERE symbol='NIFTY' AND toDate(timestamp)='{d}' AND expiry='{expiry}'
              AND oi > 0
        """)
        if df_chain.empty:
            continue

        walls = compute_oi_walls(df_chain)
        skew  = compute_iv_skew(df_chain, spot, expiry, d)

        updates.append({
            "date":           d,
            "ce_wall_strike": walls["ce_wall_strike"],
            "pe_wall_strike": walls["pe_wall_strike"],
            "iv_skew":        skew,
        })

        if (i + 1) % 50 == 0:
            log.info(f"  [{i+1}/{len(summary)}] {d}: CE_wall={walls['ce_wall_strike']:.0f} PE_wall={walls['pe_wall_strike']:.0f} skew={skew:.1f}%")

    log.info(f"Computed {len(updates)} dates — updating options_eod_summary...")

    # Rebuild full rows with new columns
    version = int(datetime.utcnow().timestamp())
    existing = ch.query_df(f"""
        SELECT * FROM market.options_eod_summary FINAL
        WHERE date >= '{from_date}'
    """)
    existing["date"] = pd.to_datetime(existing["date"]).dt.date

    update_map = {u["date"]: u for u in updates}
    rows = []
    for _, r in existing.iterrows():
        u = update_map.get(r["date"])
        if not u:
            continue
        rd = r.to_dict()
        rd["ce_wall_strike"] = u["ce_wall_strike"]
        rd["pe_wall_strike"] = u["pe_wall_strike"]
        rd["iv_skew"]        = u["iv_skew"]
        rd["version"]        = version
        rows.append(rd)

    if rows:
        df_out = pd.DataFrame(rows)
        # Only insert columns that exist in the table
        cols = [r[0] for r in ch.query("DESCRIBE market.options_eod_summary").result_rows]
        df_out = df_out[[c for c in cols if c in df_out.columns]]
        ch.insert_df("market.options_eod_summary", df_out)
        log.info(f"  Inserted {len(rows)} updated rows")

    # Update pattern_features with fii_fut_net_3d
    log.info("Updating pattern_features with FII futures net...")
    if not fii_fut.empty and "fii_fut_net_3d" in fii_fut.columns:
        # Insert new pattern_feature rows with fii_fut_net_3d populated
        # We reuse the existing rows and update via ReplacingMergeTree
        pf = ch.query_df(f"""
            SELECT * FROM analysis.pattern_features FINAL
            WHERE market IN ('indian', 'bse')
              AND event_date >= '{max(from_date, fii_fut.index.min())}'
        """)
        if not pf.empty:
            pf["event_date"] = pd.to_datetime(pf["event_date"]).dt.date
            # Add fii_fut_net_3d column if not present
            if "fii_fut_net_3d" not in pf.columns:
                pf["fii_fut_net_3d"] = 0.0
            pf["fii_fut_net_3d"] = pf["event_date"].map(
                lambda d: float(fii_fut.loc[d, "fii_fut_net_3d"]) if d in fii_fut.index else 0.0
            )
            pf["version"] = version + 1
            # Check if column exists in pattern_features
            pf_cols = [r[0] for r in ch.query("DESCRIBE analysis.pattern_features").result_rows]
            if "fii_fut_net_3d" not in pf_cols:
                ch.command("ALTER TABLE analysis.pattern_features ADD COLUMN IF NOT EXISTS fii_fut_net_3d Float64 DEFAULT 0")
                log.info("  Added fii_fut_net_3d column to pattern_features")
            pf_insert = pf[[c for c in pf_cols + ["fii_fut_net_3d"] if c in pf.columns]]
            ch.insert_df("analysis.pattern_features", pf_insert)
            log.info(f"  Updated {len(pf_insert)} pattern_feature rows with FII futures data")

    log.info("Done. Sample results:")
    sample = ch.query("""
        SELECT date, round(ce_wall_strike,0), round(pe_wall_strike,0),
               round(iv_skew,2), round(pcr,3)
        FROM market.options_eod_summary FINAL
        WHERE ce_wall_strike > 0
        ORDER BY date DESC LIMIT 8
    """).result_rows
    print(f"\n{'Date':>12} {'CE Wall':>8} {'PE Wall':>8} {'Skew%':>7} {'PCR':>7}")
    print("-" * 50)
    for r in sample:
        print(f"{str(r[0]):>12} {r[1]:>8.0f} {r[2]:>8.0f} {r[3]:>7.2f} {r[4]:>7.3f}")


if __name__ == "__main__":
    main()
