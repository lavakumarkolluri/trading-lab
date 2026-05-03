#!/usr/bin/env python3
"""
confidence_scorer.py — XGBoost confidence scorer for 0DTE option selling

Strategy: sell ATM straddle on weekly expiry day, hold to settlement.
Entry: ATM straddle premium from previous trading day EOD.
Exit:  settlement value from expiry day EOD.

Features (no lookahead — all from snapshot_date = prev trading day):
  • options_chain    : straddle premium, PCR OI, OI concentration, CE/PE ratio
  • options_eod_summary : IV rank, IV percentile, ATM IV, IV skew, wall distances
  • participant_oi   : FII/client index option net positioning
  • temporal         : day of week, week of month

Target: 1 if straddle P&L > 0, else 0 (binary classification).
Score:  predict_proba × 100 (0–100 confidence).

Walk-forward: train 12 months, test 1 month, slide monthly.

Usage:
    python confidence_scorer.py                    # all symbols, full run
    python confidence_scorer.py --symbol NIFTY     # one symbol
    python confidence_scorer.py --backtest-only    # no production score
    python confidence_scorer.py --score-only       # skip backtest, score today
"""

import io
import json
import logging
import os
import argparse
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import clickhouse_connect
from minio import Minio
from minio.error import S3Error
import xgboost as xgb
from sklearn.metrics import roc_auc_score, accuracy_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

CH_HOST   = os.getenv("CH_HOST", "clickhouse")
CH_PORT   = int(os.getenv("CH_PORT", "8123"))
CH_USER   = os.getenv("CH_USER", "default")
CH_PASS   = os.getenv("CH_PASSWORD", "")
MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER = os.getenv("MINIO_USER", "admin")
MINIO_PASS = os.getenv("MINIO_PASSWORD", "")
MINIO_BUCKET = "trading-data"
MODELS_PREFIX = "models/confidence_scorer"

SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]

# Maps option symbol → index OHLCV symbol in market.ohlcv_daily (market='nse_index')
# FINNIFTY/MIDCPNIFTY use ^NSEI proxy — ^CNXFINANCE/^CNXMIDCAP not on yfinance
INDEX_MAP = {
    "NIFTY":      "^NSEI",
    "BANKNIFTY":  "^NSEBANK",
    "FINNIFTY":   "^NSEI",
    "MIDCPNIFTY": "^NSEI",
}

# XGBoost hyperparams — kept conservative for small datasets (50–150 samples/symbol)
XGB_PARAMS = dict(
    n_estimators=100,
    max_depth=3,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_weight=5,
    objective="binary:logistic",
    eval_metric="auc",
    use_label_encoder=False,
    random_state=42,
)

TRAIN_MONTHS = 12    # walk-forward training window
TEST_MONTHS  = 1     # walk-forward test window
MIN_TRAIN    = 25    # minimum training samples before first fold


# ── Connections ───────────────────────────────────────────────────────────────

def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )

def get_mc():
    return Minio(MINIO_HOST, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)


# ── Data Loading ──────────────────────────────────────────────────────────────

def load_vix(ch) -> pd.DataFrame:
    """Load daily VIX from market.nifty_live — one row per trading date."""
    r = ch.query("""
        SELECT toDate(timestamp) AS date, max(vix) AS vix
        FROM market.nifty_live FINAL
        GROUP BY date
        HAVING vix > 0
        ORDER BY date
    """)
    df = pd.DataFrame(r.result_rows, columns=["date", "vix"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df.set_index("date", inplace=True)
    return df


def load_index_ohlcv(ch, index_symbol: str) -> pd.DataFrame:
    """Load daily OHLCV for a market index from market.ohlcv_daily."""
    r = ch.query(
        "SELECT date, open, high, low, close, volume "
        "FROM market.ohlcv_daily "
        "WHERE market='nse_index' AND symbol={sym:String} "
        "ORDER BY date",
        parameters={"sym": index_symbol},
    )
    df = pd.DataFrame(r.result_rows,
                      columns=["date","open","high","low","close","volume"])
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    return df


def compute_tech_signals(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    Compute regime signals from daily OHLCV. No lookahead — all values
    at date T use only data up to T.

    Returns DataFrame (same index as ohlcv) with columns:
      atr_percentile  — ATR(14) rank over trailing 252 days (0–100); high = volatile
      rsi14           — RSI(14); 40–60 = range-bound, extremes = trending
      hv5             — 5-day historical vol, annualised %
      hv20            — 20-day historical vol, annualised %
      hv_ratio        — hv5/hv20; >1.2 = vol accelerating, <0.8 = calming
      supertrend_dir  — +1 uptrend / -1 downtrend (7-period, 3× ATR)
      cpr_width_pct   — prev week CPR width as % of pivot; narrow = range expected
    """
    hi, lo, cl = ohlcv["high"], ohlcv["low"], ohlcv["close"]

    # True Range
    tr = pd.concat([
        hi - lo,
        (hi - cl.shift(1)).abs(),
        (lo - cl.shift(1)).abs(),
    ], axis=1).max(axis=1)

    # ATR(14) — Wilder smoothing, percentile over trailing year
    atr14 = tr.ewm(alpha=1/14, adjust=False).mean()
    atr_pct = atr14 / cl * 100
    atr_percentile = atr_pct.rolling(252).rank(pct=True) * 100

    # RSI(14)
    delta = cl.diff()
    avg_g = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
    avg_l = (-delta).clip(lower=0).ewm(com=13, adjust=False).mean()
    rsi14 = 100 - (100 / (1 + avg_g / avg_l.replace(0, np.nan)))

    # Realized vol (annualised %)
    ret  = cl.pct_change()
    hv5  = ret.rolling(5).std()  * np.sqrt(252) * 100
    hv20 = ret.rolling(20).std() * np.sqrt(252) * 100
    hv_ratio = hv5 / hv20.replace(0, np.nan)

    # Supertrend(7, 3.0) — iterative to avoid lookahead
    hl2  = (hi + lo) / 2
    atr7 = tr.ewm(alpha=1/7, adjust=False).mean()
    bu   = (hl2 + 3.0 * atr7).values
    bl   = (hl2 - 3.0 * atr7).values
    cl_v = cl.values
    n    = len(cl_v)
    upper = bu.copy()
    lower = bl.copy()
    di    = np.ones(n)
    for i in range(1, n):
        upper[i] = bu[i] if (bu[i] < upper[i-1] or cl_v[i-1] > upper[i-1]) else upper[i-1]
        lower[i] = bl[i] if (bl[i] > lower[i-1] or cl_v[i-1] < lower[i-1]) else lower[i-1]
        if   cl_v[i] > upper[i-1]: di[i] =  1.0
        elif cl_v[i] < lower[i-1]: di[i] = -1.0
        else:                       di[i] =  di[i-1]
    supertrend_dir = pd.Series(di, index=cl.index)

    # Weekly CPR width (previous week → current week)
    wk = ohlcv[["high","low","close"]].resample("W-THU").agg(
        {"high":"max","low":"min","close":"last"}
    )
    pivot      = (wk["high"] + wk["low"] + wk["close"]) / 3
    wk["cpr_w"] = ((pivot + wk["high"])/2 - (pivot + wk["low"])/2) / pivot * 100
    wk["cpr_w"] = wk["cpr_w"].shift(1)   # use PREVIOUS week's CPR
    daily_cpr = wk["cpr_w"].resample("D").ffill().reindex(cl.index, method="ffill")

    return pd.DataFrame({
        "atr_percentile": atr_percentile,
        "rsi14":          rsi14,
        "hv5":            hv5,
        "hv20":           hv20,
        "hv_ratio":       hv_ratio,
        "supertrend_dir": supertrend_dir,
        "cpr_width_pct":  daily_cpr,
    }, index=cl.index)


def load_eod_summary(ch) -> pd.DataFrame:
    """Load full options_eod_summary — market-wide NIFTY indicators."""
    r = ch.query("""
        SELECT date, iv_rank, iv_percentile, atm_ce_iv, atm_pe_iv, iv_skew,
               pcr, nifty_spot, ce_wall_strike, pe_wall_strike
        FROM market.options_eod_summary FINAL
        ORDER BY date
    """)
    df = pd.DataFrame(r.result_rows, columns=[
        "date", "iv_rank", "iv_percentile", "atm_ce_iv", "atm_pe_iv", "iv_skew",
        "pcr_eod", "nifty_spot", "ce_wall_strike", "pe_wall_strike"
    ])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df.set_index("date", inplace=True)
    return df


def load_participant_oi(ch) -> pd.DataFrame:
    """Load participant OI — FII/Client index option positioning."""
    r = ch.query("""
        SELECT date, entity,
               opt_index_call_long, opt_index_call_short,
               opt_index_put_long,  opt_index_put_short
        FROM market.participant_oi FINAL
        ORDER BY date
    """)
    rows = []
    for date_, entity, cl, cs, pl, ps in r.result_rows:
        rows.append({
            "date":   date_,
            "entity": entity,
            "call_net": cl - cs,
            "put_net":  pl - ps,
            "pcr": (pl + ps) / max(cl + cs, 1),
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Pivot to wide: one row per date
    pivoted = df.pivot_table(
        index="date", columns="entity", values=["call_net", "put_net", "pcr"]
    )
    pivoted.columns = [f"{col[1].lower()}_{col[0]}" for col in pivoted.columns]
    pivoted.reset_index(inplace=True)
    pivoted.set_index("date", inplace=True)
    return pivoted


def load_options_chain(ch, symbol: str) -> pd.DataFrame:
    """Load all EOD options chain snapshots for a symbol."""
    r = ch.query(f"""
        SELECT toDate(timestamp) AS snap_date, expiry, strike, option_type,
               ltp, oi, volume
        FROM market.options_chain FINAL
        WHERE symbol = '{symbol}' AND ltp > 0.05
        ORDER BY snap_date, expiry, strike, option_type
    """)
    df = pd.DataFrame(r.result_rows, columns=[
        "snap_date", "expiry", "strike", "option_type", "ltp", "oi", "volume"
    ])
    df["snap_date"] = pd.to_datetime(df["snap_date"]).dt.date
    df["expiry"]    = pd.to_datetime(df["expiry"]).dt.date
    return df


# ── Feature Extraction ────────────────────────────────────────────────────────

def find_atm_strike(ce: pd.Series, pe: pd.Series) -> Optional[float]:
    """Find ATM strike: minimum |CE_ltp - PE_ltp| with both sides liquid."""
    common = ce.index.intersection(pe.index)
    common = common[(ce.loc[common] > 0.5) & (pe.loc[common] > 0.5)]
    if len(common) < 2:
        return None
    diff = (ce.loc[common] - pe.loc[common]).abs()
    return float(diff.idxmin())


def extract_chain_features(
    chain: pd.DataFrame, snap_date: date, expiry: date
) -> Optional[dict]:
    """Extract LTP/OI-based features from options chain snapshot."""
    snap = chain[(chain.snap_date == snap_date) & (chain.expiry == expiry)]
    if snap.empty:
        return None

    ce = snap[snap.option_type == "CE"].set_index("strike")["ltp"]
    pe = snap[snap.option_type == "PE"].set_index("strike")["ltp"]
    ce_oi = snap[snap.option_type == "CE"].set_index("strike")["oi"]
    pe_oi = snap[snap.option_type == "PE"].set_index("strike")["oi"]

    atm = find_atm_strike(ce, pe)
    if atm is None:
        return None

    straddle_premium = float(ce.get(atm, 0) + pe.get(atm, 0))
    if straddle_premium < 1:
        return None

    straddle_pct = straddle_premium / atm * 100
    ce_pe_ratio  = float(ce.get(atm, 1) / max(pe.get(atm, 0.01), 0.01))

    total_ce_oi = float(ce_oi.sum())
    total_pe_oi = float(pe_oi.sum())
    pcr_oi      = total_pe_oi / max(total_ce_oi, 1)

    # OI concentration: top-3 strikes / total
    oi_conc_ce = float(ce_oi.nlargest(3).sum() / max(total_ce_oi, 1))
    oi_conc_pe = float(pe_oi.nlargest(3).sum() / max(total_pe_oi, 1))

    # ATM OI ratio
    atm_ce_oi = float(ce_oi.get(atm, 0))
    atm_pe_oi = float(pe_oi.get(atm, 0))
    atm_oi_ratio = atm_ce_oi / max(atm_pe_oi, 1)

    return {
        "atm_strike":       atm,
        "straddle_premium": straddle_premium,
        "straddle_pct":     straddle_pct,
        "ce_pe_ratio":      ce_pe_ratio,
        "pcr_oi":           pcr_oi,
        "oi_conc_ce":       oi_conc_ce,
        "oi_conc_pe":       oi_conc_pe,
        "atm_oi_ratio":     atm_oi_ratio,
    }


def extract_eod_features(eod: pd.DataFrame, snap_date: date) -> dict:
    """Extract NIFTY market-wide IV/PCR features from EOD summary."""
    if snap_date not in eod.index:
        return {}
    row = eod.loc[snap_date]
    spot = row.nifty_spot
    feats = {
        "iv_rank":        float(row.iv_rank),
        "iv_percentile":  float(row.iv_percentile),
        "atm_ce_iv":      float(row.atm_ce_iv),
        "atm_pe_iv":      float(row.atm_pe_iv),
        "iv_skew":        float(row.iv_skew),
        "pcr_eod":        float(row.pcr_eod),
    }
    if spot > 0:
        feats["ce_wall_pct"] = (row.ce_wall_strike - spot) / spot * 100
        feats["pe_wall_pct"] = (spot - row.pe_wall_strike) / spot * 100
    return feats


def extract_poi_features(poi: pd.DataFrame, snap_date: date) -> dict:
    """Extract participant OI features for snap_date."""
    if snap_date not in poi.index:
        return {}
    row = poi.loc[snap_date]
    return {k: float(v) for k, v in row.items() if pd.notna(v)}


def temporal_features(expiry: date) -> dict:
    return {
        "day_of_week":   expiry.weekday(),      # 0=Mon, 4=Fri
        "week_of_month": (expiry.day - 1) // 7 + 1,
    }


# ── P&L Target ────────────────────────────────────────────────────────────────

def compute_pnl(
    chain: pd.DataFrame, snap_date: date, expiry: date, atm_strike: float
) -> Optional[float]:
    """
    Straddle P&L (points):
      entry = ATM CE + PE at snap_date
      exit  = ATM CE + PE at expiry day (settlement prices)
    """
    def get_ltp(d: date) -> Optional[float]:
        rows = chain[
            (chain.snap_date == d) &
            (chain.expiry == expiry) &
            (chain.strike == atm_strike)
        ]
        ce_rows = rows[rows.option_type == "CE"]["ltp"]
        pe_rows = rows[rows.option_type == "PE"]["ltp"]
        if ce_rows.empty or pe_rows.empty:
            return None
        return float(ce_rows.iloc[0]) + float(pe_rows.iloc[0])

    entry = get_ltp(snap_date)
    exit_ = get_ltp(expiry)

    if entry is None or exit_ is None or entry < 1:
        return None

    return entry - exit_


# ── Dataset Builder ───────────────────────────────────────────────────────────

def _tech_row(tech: pd.DataFrame, snap_date: date, eod: pd.DataFrame) -> dict:
    """Extract tech signal values for snap_date, plus IV/HV5 ratio."""
    snap_ts = pd.Timestamp(snap_date)
    if snap_ts not in tech.index:
        return {}
    t = tech.loc[snap_ts]
    out = {
        "atr_percentile": float(t["atr_percentile"]) if pd.notna(t["atr_percentile"]) else float("nan"),
        "rsi14":          float(t["rsi14"])          if pd.notna(t["rsi14"])          else float("nan"),
        "hv_ratio":       float(t["hv_ratio"])       if pd.notna(t["hv_ratio"])       else float("nan"),
        "supertrend_dir": float(t["supertrend_dir"]) if pd.notna(t["supertrend_dir"]) else float("nan"),
        "cpr_width_pct":  float(t["cpr_width_pct"])  if pd.notna(t["cpr_width_pct"])  else float("nan"),
    }
    hv5 = float(t["hv5"]) if pd.notna(t["hv5"]) and t["hv5"] > 0 else 0.0
    if hv5 > 0 and snap_date in eod.index:
        atm_iv = (float(eod.loc[snap_date, "atm_ce_iv"]) +
                  float(eod.loc[snap_date, "atm_pe_iv"])) / 2
        out["iv_hv5_ratio"] = atm_iv / hv5 if atm_iv > 0 else float("nan")
    else:
        out["iv_hv5_ratio"] = float("nan")
    return out


def build_dataset(ch, symbol: str) -> pd.DataFrame:
    """Build feature matrix + P&L target for all expiry dates of a symbol."""
    log.info(f"[{symbol}] loading data…")
    chain = load_options_chain(ch, symbol)
    eod   = load_eod_summary(ch)
    poi   = load_participant_oi(ch)
    vix   = load_vix(ch)
    index_sym = INDEX_MAP.get(symbol, "^NSEI")
    ohlcv     = load_index_ohlcv(ch, index_sym)
    tech      = compute_tech_signals(ohlcv) if not ohlcv.empty else pd.DataFrame()

    # All weekly expiries that appear in the chain
    expiries = sorted(chain["expiry"].unique())
    log.info(f"[{symbol}] {len(expiries)} expiry dates to process")

    rows = []
    for expiry in expiries:
        # Find the most recent snapshot BEFORE expiry (previous trading day)
        snap_candidates = sorted(chain[
            (chain.expiry == expiry) & (chain.snap_date < expiry)
        ]["snap_date"].unique())
        if not snap_candidates:
            continue
        snap_date = snap_candidates[-1]

        # Extract features
        chain_feats = extract_chain_features(chain, snap_date, expiry)
        if chain_feats is None:
            continue

        atm = chain_feats["atm_strike"]
        pnl = compute_pnl(chain, snap_date, expiry, atm)
        if pnl is None:
            continue

        row = {
            "expiry":     expiry,
            "entry_date": snap_date,
            "pnl_pts":    pnl,
            "pnl_pct":    pnl / chain_feats["straddle_premium"] * 100,
            "entry_premium": chain_feats["straddle_premium"],
            "exit_value":    chain_feats["straddle_premium"] - pnl,
        }
        row.update(chain_feats)
        row.update(extract_eod_features(eod, snap_date))
        row.update(extract_poi_features(poi, snap_date))
        row.update(temporal_features(expiry))
        row["vix"] = float(vix.loc[snap_date, "vix"]) if snap_date in vix.index else float("nan")
        if not tech.empty:
            row.update(_tech_row(tech, snap_date, eod))
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["target"] = (df["pnl_pts"] > 0).astype(int)
    df.sort_values("expiry", inplace=True)
    df.reset_index(drop=True, inplace=True)
    log.info(f"[{symbol}] dataset: {len(df)} rows, win_rate={df.target.mean():.1%}")
    return df


# ── Feature Columns ───────────────────────────────────────────────────────────

FEATURE_COLS = [
    # Options chain
    "straddle_pct", "ce_pe_ratio", "pcr_oi",
    "oi_conc_ce", "oi_conc_pe", "atm_oi_ratio",
    # EOD summary
    "iv_rank", "iv_percentile", "atm_ce_iv", "atm_pe_iv",
    "iv_skew", "pcr_eod", "ce_wall_pct", "pe_wall_pct",
    # Participant OI
    "client_call_net", "client_put_net", "client_pcr",
    "fii_call_net", "fii_put_net", "fii_pcr",
    # Temporal
    "day_of_week", "week_of_month",
    # Volatility regime
    "vix",
    # Technical regime signals (from index OHLCV)
    "atr_percentile", "rsi14", "hv_ratio",
    "supertrend_dir", "cpr_width_pct", "iv_hv5_ratio",
]


def get_features(df: pd.DataFrame) -> pd.DataFrame:
    """Return only the feature columns present in df, fill missing with 0."""
    cols = [c for c in FEATURE_COLS if c in df.columns]
    return df[cols].fillna(0.0).astype(float)


# ── Walk-Forward Validation ───────────────────────────────────────────────────

def walk_forward_train(
    df: pd.DataFrame, symbol: str, ch, mc
) -> pd.DataFrame:
    """
    Walk-forward: for each test month, train on prior 12 months, predict test month.
    Stores results in analysis.confidence_backtest.
    Returns DataFrame of all OOS predictions.
    """
    if len(df) < MIN_TRAIN + 5:
        log.warning(f"[{symbol}] not enough data for walk-forward ({len(df)} rows)")
        return pd.DataFrame()

    df = df.copy()
    df["expiry_dt"] = pd.to_datetime(df["expiry"])

    min_date = df["expiry_dt"].min()
    max_date = df["expiry_dt"].max()

    # First test month starts after MIN_TRAIN samples
    first_test_start = df.iloc[MIN_TRAIN]["expiry_dt"].to_period("M").to_timestamp()

    all_preds = []
    fold = 0
    test_start = first_test_start

    while test_start <= max_date:
        test_end = test_start + pd.DateOffset(months=TEST_MONTHS)
        train_start = test_start - pd.DateOffset(months=TRAIN_MONTHS)

        train_mask = (df["expiry_dt"] >= train_start) & (df["expiry_dt"] < test_start)
        test_mask  = (df["expiry_dt"] >= test_start) & (df["expiry_dt"] < test_end)

        train_df = df[train_mask]
        test_df  = df[test_mask]

        if len(train_df) < 15 or len(test_df) == 0:
            test_start = test_end
            continue

        X_train = get_features(train_df)
        y_train = train_df["target"].values
        X_test  = get_features(test_df)

        # Align columns
        for c in X_train.columns:
            if c not in X_test.columns:
                X_test[c] = 0.0
        X_test = X_test[X_train.columns]

        model = xgb.XGBClassifier(**XGB_PARAMS, verbosity=0)
        model.fit(X_train, y_train)

        proba = model.predict_proba(X_test)[:, 1]

        pred_df = test_df[["expiry", "entry_date", "atm_strike",
                            "entry_premium", "exit_value", "pnl_pts", "pnl_pct", "target"]].copy()
        pred_df["confidence"] = proba
        pred_df["fold"]       = fold
        all_preds.append(pred_df)

        auc = roc_auc_score(test_df["target"].values, proba) if len(set(test_df["target"])) > 1 else float("nan")
        acc = accuracy_score(test_df["target"].values, proba > 0.5)
        log.info(f"[{symbol}] fold={fold} train={len(train_df)} test={len(test_df)} AUC={auc:.3f} acc={acc:.2%}")

        fold += 1
        test_start = test_end

    if not all_preds:
        return pd.DataFrame()

    results = pd.concat(all_preds, ignore_index=True)

    # Write to ClickHouse
    _insert_backtest_results(ch, symbol, results)

    overall_auc = roc_auc_score(results["target"], results["confidence"]) \
        if len(set(results["target"])) > 1 else float("nan")
    log.info(
        f"[{symbol}] walk-forward complete: {len(results)} OOS predictions, "
        f"overall AUC={overall_auc:.3f}, win_rate={results.target.mean():.1%}, "
        f"avg_confidence={results.confidence.mean():.2f}"
    )
    return results


def _insert_backtest_results(ch, symbol: str, results: pd.DataFrame):
    rows = []
    for _, r in results.iterrows():
        rows.append([
            symbol,
            r["expiry"],
            r["entry_date"],
            float(r["atm_strike"]),
            float(r["entry_premium"]),
            float(r["exit_value"]),
            float(r["pnl_pts"]),
            float(r["pnl_pct"]),
            int(r["target"]),
            float(r["confidence"]),
            int(r["fold"]),
        ])
    ch.insert(
        "analysis.confidence_backtest",
        rows,
        column_names=[
            "symbol", "expiry", "entry_date", "atm_strike", "entry_premium",
            "exit_value", "pnl_pts", "pnl_pct", "target", "confidence", "fold",
        ],
    )
    log.info(f"[{symbol}] inserted {len(rows)} backtest rows")


# ── Final Model Training ──────────────────────────────────────────────────────

def train_final_model(df: pd.DataFrame, symbol: str, mc) -> Optional[xgb.XGBClassifier]:
    """Train on all available data and save model to MinIO."""
    if len(df) < MIN_TRAIN:
        log.warning(f"[{symbol}] insufficient data, skipping final model")
        return None

    X = get_features(df)
    y = df["target"].values

    model = xgb.XGBClassifier(**XGB_PARAMS, verbosity=0)
    model.fit(X, y)

    # Save model to MinIO (XGBoost requires a file path, not BytesIO)
    import tempfile, pathlib
    model_key = f"{MODELS_PREFIX}/{symbol}_model.ubj"
    try:
        with tempfile.NamedTemporaryFile(suffix=".ubj", delete=False) as tf:
            tmp_path = tf.name
        model.save_model(tmp_path)
        mc.fput_object(MINIO_BUCKET, model_key, tmp_path,
                       content_type="application/octet-stream")
        pathlib.Path(tmp_path).unlink(missing_ok=True)
        log.info(f"[{symbol}] model saved to MinIO: {model_key}")
    except S3Error as e:
        log.warning(f"[{symbol}] failed to save model: {e}")

    # Save feature columns used
    meta = {"features": list(X.columns), "n_train": len(df), "win_rate": float(y.mean())}
    meta_buf = io.BytesIO(json.dumps(meta).encode())
    mc.put_object(
        MINIO_BUCKET, f"{MODELS_PREFIX}/{symbol}_meta.json",
        meta_buf, length=meta_buf.getbuffer().nbytes,
        content_type="application/json"
    )
    log.info(f"[{symbol}] final model: {len(df)} samples, win_rate={y.mean():.1%}")
    return model


def load_model(symbol: str, mc) -> Optional[xgb.XGBClassifier]:
    """Load trained model from MinIO."""
    import tempfile, pathlib
    model_key = f"{MODELS_PREFIX}/{symbol}_model.ubj"
    try:
        with tempfile.NamedTemporaryFile(suffix=".ubj", delete=False) as tf:
            tmp_path = tf.name
        mc.fget_object(MINIO_BUCKET, model_key, tmp_path)
        model = xgb.XGBClassifier()
        model.load_model(tmp_path)
        pathlib.Path(tmp_path).unlink(missing_ok=True)
        return model
    except S3Error:
        return None


# ── Production Scoring ────────────────────────────────────────────────────────

def score_today(ch, mc, symbol: str) -> None:
    """Score the next upcoming expiry for a symbol using the trained model."""
    model = load_model(symbol, mc)
    if model is None:
        log.warning(f"[{symbol}] no trained model found, skipping scoring")
        return

    chain     = load_options_chain(ch, symbol)
    eod       = load_eod_summary(ch)
    poi       = load_participant_oi(ch)
    vix       = load_vix(ch)
    index_sym = INDEX_MAP.get(symbol, "^NSEI")
    ohlcv     = load_index_ohlcv(ch, index_sym)
    tech      = compute_tech_signals(ohlcv) if not ohlcv.empty else pd.DataFrame()

    today = date.today()
    snap_dates = sorted(chain["snap_date"].unique(), reverse=True)
    if not snap_dates:
        return

    latest_snap = snap_dates[0]

    # Find next expiry after today
    future_expiries = sorted([
        e for e in chain["expiry"].unique() if e > today
    ])
    if not future_expiries:
        log.warning(f"[{symbol}] no future expiries found")
        return
    next_expiry = future_expiries[0]

    chain_feats = extract_chain_features(chain, latest_snap, next_expiry)
    if chain_feats is None:
        log.warning(f"[{symbol}] could not extract chain features for {next_expiry}")
        return

    row = {}
    row.update(chain_feats)
    row.update(extract_eod_features(eod, latest_snap))
    row.update(extract_poi_features(poi, latest_snap))
    row.update(temporal_features(next_expiry))
    row["vix"] = float(vix.loc[latest_snap, "vix"]) if latest_snap in vix.index else 0.0
    if not tech.empty:
        row.update(_tech_row(tech, latest_snap, eod))

    feat_df = pd.DataFrame([row])
    meta_key = f"{MODELS_PREFIX}/{symbol}_meta.json"
    try:
        resp   = mc.get_object(MINIO_BUCKET, meta_key)
        meta   = json.loads(resp.read())
        feat_cols = meta["features"]
    except S3Error:
        feat_cols = FEATURE_COLS

    for c in feat_cols:
        if c not in feat_df.columns:
            feat_df[c] = 0.0
    feat_df = feat_df[feat_cols].fillna(0.0).astype(float)

    confidence = float(model.predict_proba(feat_df)[0, 1]) * 100

    # Expected P&L % (rough estimate: confidence maps to premium retention)
    expected_pnl_pct = (confidence - 50) * 1.5  # linear mapping

    log.info(f"[{symbol}] next_expiry={next_expiry} confidence={confidence:.1f} "
             f"expected_pnl_pct={expected_pnl_pct:.1f}%")

    features_json = json.dumps({k: round(float(v), 4) for k, v in row.items()
                                 if k in feat_cols and isinstance(v, (int, float))})

    ch.insert(
        "analysis.confidence_scores",
        [[today, symbol, next_expiry, confidence, expected_pnl_pct, features_json]],
        column_names=["score_date", "symbol", "next_expiry", "confidence",
                      "expected_pnl_pct", "features_json"],
    )
    log.info(f"[{symbol}] confidence score inserted: {confidence:.1f}/100")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_symbol(symbol: str, ch, mc, backtest_only: bool, score_only: bool):
    if not score_only:
        df = build_dataset(ch, symbol)
        if df.empty:
            log.warning(f"[{symbol}] empty dataset, skipping")
            return

        walk_forward_train(df, symbol, ch, mc)
        train_final_model(df, symbol, mc)

    if not backtest_only:
        score_today(ch, mc, symbol)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", help="Single symbol to run (default: all)")
    parser.add_argument("--backtest-only", action="store_true",
                        help="Run walk-forward only, skip today's scoring")
    parser.add_argument("--score-only", action="store_true",
                        help="Score today only (models must already exist)")
    args = parser.parse_args()

    ch = get_ch()
    mc = get_mc()

    # Ensure bucket exists
    if not mc.bucket_exists(MINIO_BUCKET):
        mc.make_bucket(MINIO_BUCKET)

    symbols = [args.symbol] if args.symbol else SYMBOLS

    for sym in symbols:
        try:
            run_symbol(sym, ch, mc, args.backtest_only, args.score_only)
        except Exception as e:
            log.error(f"[{sym}] failed: {e}", exc_info=True)

    log.info("confidence_scorer done")


if __name__ == "__main__":
    main()
