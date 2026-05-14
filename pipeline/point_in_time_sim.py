#!/usr/bin/env python3
"""
point_in_time_sim.py — Simulate what the model would predict for a given past date.

Loads trained models from MinIO, reconstructs the feature vector from historical DB
data as of the given date, scores all 4 strategies, and compares to the actual outcome
stored in analysis.spread_backtest.

Usage:
    python point_in_time_sim.py --as-of 2024-03-14 --symbol NIFTY
    python point_in_time_sim.py --as-of 2024-03-14               # all symbols
"""

import argparse
import json
from datetime import date

import pandas as pd

from ch_utils import ch_client, minio_client
from confidence_scorer import (
    STRATEGY_TYPES,
    FEATURE_COLS,
    INDEX_MAP,
    load_options_chain,
    load_eod_summary,
    load_participant_oi,
    load_vix,
    load_events,
    load_index_ohlcv,
    compute_tech_signals,
    extract_chain_features,
    extract_eod_features,
    extract_poi_features,
    temporal_features,
    extract_event_features,
    load_model,
    _tech_row,
)
from strategy_selector import ML_CONFIDENCE_THRESHOLD


def _nearest_before(index, d):
    past = sorted([x for x in index if x <= d])
    return past[-1] if past else None


def build_feature_row(ch, symbol: str, as_of: date) -> dict | None:
    """Reconstruct the feature vector visible on as_of date for the next expiry."""
    chain       = load_options_chain(ch, symbol)
    eod         = load_eod_summary(ch, symbol)
    poi         = load_participant_oi(ch)
    vix         = load_vix(ch)
    event_dates = load_events(ch)
    index_sym   = INDEX_MAP.get(symbol, "^NSEI")
    ohlcv       = load_index_ohlcv(ch, index_sym)
    tech        = compute_tech_signals(ohlcv) if not ohlcv.empty else pd.DataFrame()

    # Only use data available ≤ as_of (true point-in-time)
    chain = chain[chain.snap_date <= as_of]

    snap_date = _nearest_before(chain["snap_date"].unique(), as_of)
    if snap_date is None:
        print(f"[{symbol}] no chain data on or before {as_of}")
        return None

    future_expiries = sorted([e for e in chain["expiry"].unique() if e > as_of])
    if not future_expiries:
        print(f"[{symbol}] no future expiries after {as_of}")
        return None
    next_expiry = future_expiries[0]

    chain_feats = extract_chain_features(chain, snap_date, next_expiry)
    if chain_feats is None:
        print(f"[{symbol}] could not extract chain features for {next_expiry}")
        return None

    eod_snap = _nearest_before(eod.index, snap_date)
    poi_snap = _nearest_before(poi.index, snap_date)
    vix_snap = _nearest_before(vix.index, snap_date)

    row = {"snap_date": snap_date, "next_expiry": next_expiry}
    row.update(chain_feats)
    row.update(extract_eod_features(eod, eod_snap) if eod_snap else {})
    row.update(extract_poi_features(poi, poi_snap) if poi_snap else {})
    row.update(temporal_features(next_expiry))
    row["vix"] = float(vix.loc[vix_snap, "vix"]) if vix_snap in vix.index else 0.0
    if not tech.empty:
        row.update(_tech_row(tech, snap_date, eod))
    row.update(extract_event_features(event_dates, snap_date, next_expiry))
    return row


def get_actual_outcomes(ch, symbol: str, expiry: date) -> dict:
    """Load actual P&L from spread_backtest for each strategy on this expiry."""
    rows = ch.query(
        "SELECT strategy, pnl_pts, short_n, wing_m FROM analysis.spread_backtest "
        "WHERE symbol={sym:String} AND expiry={exp:Date} "
        "ORDER BY strategy, short_n, wing_m",
        parameters={"sym": symbol, "exp": expiry},
    ).result_rows
    results = {}
    for strategy, pnl_pts, short_n, wing_m in rows:
        if strategy not in results:
            results[strategy] = {"pnl_pts": pnl_pts, "short_n": short_n, "wing_m": wing_m}
    return results


def score_all_strategies(mc, symbol: str, row: dict) -> list[tuple[str, float]]:
    """Score each strategy's model with the given feature row. Returns sorted list."""
    scores = []
    for strategy in STRATEGY_TYPES:
        model, meta = load_model(symbol, mc, strategy)
        if model is None:
            scores.append((strategy, None))
            continue
        feat_cols = meta.get("features", FEATURE_COLS)
        feat_df = pd.DataFrame([row])
        for c in feat_cols:
            if c not in feat_df.columns:
                feat_df[c] = 0.0
        feat_df = feat_df[[c for c in feat_cols if c in feat_df.columns]].fillna(0.0).astype(float)
        confidence = float(model.predict_proba(feat_df)[0, 1]) * 100
        scores.append((strategy, confidence))
    return sorted(scores, key=lambda x: (x[1] or 0), reverse=True)


def print_report(symbol: str, row: dict, scores: list, actuals: dict):
    expiry = row["next_expiry"]
    snap   = row["snap_date"]
    print(f"\n{'='*60}")
    print(f"Point-in-Time Simulation: {symbol}")
    print(f"Entry date: {snap}  |  Expiry: {expiry}")
    print(f"{'='*60}")

    # Market conditions
    print("\nMarket conditions:")
    for key, label in [
        ("vix",            "VIX"),
        ("iv_rank",        "IV Rank"),
        ("iv_percentile",  "IV Percentile"),
        ("atm_ce_iv",      "ATM CE IV"),
        ("rsi14",          "RSI-14"),
        ("atr_percentile", "ATR Percentile"),
        ("supertrend_dir", "Supertrend (+1=bull/-1=bear)"),
        ("pcr_eod",        "PCR (EOD)"),
        ("straddle_premium","Straddle Premium (pts)"),
    ]:
        v = row.get(key)
        if v is not None and not (isinstance(v, float) and v != v):
            print(f"  {label:<35} {v:.2f}")

    # Strategy predictions
    print("\nStrategy predictions:")
    selected = None
    for strategy, conf in scores:
        if conf is None:
            tag = "  (no model)"
            print(f"  {strategy:<18} → no model trained")
            continue
        if conf >= ML_CONFIDENCE_THRESHOLD and selected is None:
            selected = strategy
            tag = "  ← SELECTED"
        else:
            tag = ""
        bar = "█" * int(conf / 5)
        print(f"  {strategy:<18} → {conf:5.1f}%  {bar}{tag}")

    if selected is None:
        print(f"\n  No strategy above {ML_CONFIDENCE_THRESHOLD}% threshold — SKIP")

    # Actual outcomes
    print("\nActual outcomes:")
    if not actuals:
        print("  No spread_backtest rows for this expiry (run backtester first)")
    else:
        for strategy in STRATEGY_TYPES:
            if strategy not in actuals:
                print(f"  {strategy:<18} → no data")
                continue
            a = actuals[strategy]
            pnl = a["pnl_pts"]
            tag = "WIN" if pnl > 0 else "LOSS"
            marker = "✓" if pnl > 0 else "✗"
            print(f"  {strategy:<18} → {pnl:+.1f} pts  [{tag}] {marker}")

    if selected and selected in actuals:
        pnl = actuals[selected]["pnl_pts"]
        print(f"\nSelected '{selected}': actual P&L = {pnl:+.1f} pts "
              f"({'WIN ✓' if pnl > 0 else 'LOSS ✗'})")

    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", required=True, help="Entry date YYYY-MM-DD")
    parser.add_argument("--symbol", help="Symbol (default: all)")
    args = parser.parse_args()

    as_of   = date.fromisoformat(args.as_of)
    ch      = ch_client()
    mc      = minio_client()
    symbols = [args.symbol] if args.symbol else ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]

    for symbol in symbols:
        row = build_feature_row(ch, symbol, as_of)
        if row is None:
            continue
        scores  = score_all_strategies(mc, symbol, row)
        actuals = get_actual_outcomes(ch, symbol, row["next_expiry"])
        print_report(symbol, row, scores, actuals)


if __name__ == "__main__":
    main()
