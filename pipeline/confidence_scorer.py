#!/usr/bin/env python3
"""
confidence_scorer.py — Multi-model confidence scorer for 0DTE option selling

Strategy: sell ATM straddle on weekly expiry day, hold to settlement.
Entry: ATM straddle premium from previous trading day EOD.
Exit:  settlement value from expiry day EOD.

Models compared (--compare flag):
  xgb         — XGBoost per-symbol (baseline, default)
  lr          — Logistic Regression per-symbol (better calibrated for small n)
  scorecard   — Rule-based (no training; thresholds on IV rank, VIX, ATR, HV ratio)
  xgb_pooled  — XGBoost on all symbols combined (~380 rows)
  lr_pooled   — Logistic Regression pooled

Usage:
    python confidence_scorer.py                          # all symbols, XGBoost
    python confidence_scorer.py --compare                # compare all, train best
    python confidence_scorer.py --model-type lr          # force logistic regression
    python confidence_scorer.py --model-type scorecard   # rule-based scoring
    python confidence_scorer.py --symbol NIFTY           # one symbol
    python confidence_scorer.py --backtest-only          # no production score
    python confidence_scorer.py --score-only             # skip backtest, score today
"""

import io
import json
import logging
import os
import pickle
import argparse
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import clickhouse_connect
from minio import Minio
from minio.error import S3Error
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, accuracy_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

GIT_SHA = os.getenv("GIT_SHA", "unknown")


def _record_run(ch, status: str, started_at: datetime, error_msg: str = ""):
    try:
        ch.command(
            """INSERT INTO system_meta.pipeline_runs
               (service, started_at, finished_at, status, git_sha, error_msg)
               VALUES ({svc:String},{start:DateTime},{end:DateTime},{st:String},{sha:String},{err:String})""",
            parameters={"svc": "confidence_scorer", "start": started_at,
                        "end": datetime.utcnow(), "st": status,
                        "sha": GIT_SHA, "err": error_msg},
        )
    except Exception as e:
        log.warning("pipeline_runs write failed: %s", e)


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

INDEX_MAP = {
    "NIFTY":      "^NSEI",
    "BANKNIFTY":  "^NSEBANK",
    "FINNIFTY":   "^NSEI",
    "MIDCPNIFTY": "^NSEI",
}

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

TRAIN_MONTHS = 12
TEST_MONTHS  = 1
MIN_TRAIN    = 25


# ── Connections ───────────────────────────────────────────────────────────────

def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )

def get_mc():
    return Minio(MINIO_HOST, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)


# ── Model abstraction ─────────────────────────────────────────────────────────

class ScorecardClassifier:
    """Rule-based classifier — no training needed. sklearn-compatible interface."""

    def fit(self, X, y):
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        scores = X.apply(self._score, axis=1).values
        p = np.clip(scores / 100.0, 0.0, 1.0)
        return np.column_stack([1 - p, p])

    def _score(self, row: pd.Series) -> float:
        s = 50.0

        # Premium richness: high IV rank → more premium collected → sell edge
        iv_r = row.get("iv_rank", 50)
        s += 12 if iv_r > 70 else (6 if iv_r > 50 else (-8 if iv_r < 25 else 0))

        # Volatility regime: India VIX
        vix = row.get("vix", 15)
        s += 10 if vix < 13 else (4 if vix < 17 else (-8 if vix > 22 else (-16 if vix > 28 else 0)))

        # Historical vol regime: ATR percentile (high = wide ranges, bad for sellers)
        atr_p = row.get("atr_percentile", 50)
        s += 8 if atr_p < 30 else (-8 if atr_p > 70 else 0)

        # Vol acceleration: HV5/HV20 > 1.3 means vol spiking, dangerous for short gamma
        hv_r = row.get("hv_ratio", 1.0)
        s += -10 if hv_r > 1.3 else (6 if hv_r < 0.8 else 0)

        # Options richness: IV/HV5 > 1.5 means options priced richly vs realized
        ivhv = row.get("iv_hv5_ratio", 1.2)
        s += 8 if ivhv > 1.5 else (-6 if ivhv < 0.8 else 0)

        # Known events: data shows 77.4% win rate on event weeks (IV elevated, market stays in range)
        if row.get("event_in_window", 0) == 1:
            s += 4

        return s


def _make_model(model_type: str):
    """Return an untrained model for the given model_type."""
    if model_type in ("xgb", "xgb_pooled"):
        return xgb.XGBClassifier(**XGB_PARAMS, verbosity=0)
    if model_type in ("lr", "lr_pooled"):
        return Pipeline([
            ("sc", StandardScaler()),
            ("lr", LogisticRegression(max_iter=2000, C=0.1,
                                      class_weight="balanced", random_state=42)),
        ])
    if model_type == "scorecard":
        return ScorecardClassifier()
    raise ValueError(f"Unknown model_type: {model_type!r}")


def _model_ext(model_type: str) -> str:
    return "ubj" if model_type in ("xgb", "xgb_pooled") else "pkl"


def _save_model(model, model_type: str, model_key: str, mc):
    import tempfile, pathlib
    if model_type in ("xgb", "xgb_pooled"):
        with tempfile.NamedTemporaryFile(suffix=".ubj", delete=False) as tf:
            tmp = tf.name
        model.save_model(tmp)
        mc.fput_object(MINIO_BUCKET, model_key, tmp,
                       content_type="application/octet-stream")
        pathlib.Path(tmp).unlink(missing_ok=True)
    else:
        buf = io.BytesIO(pickle.dumps(model))
        mc.put_object(MINIO_BUCKET, model_key, buf,
                      length=buf.getbuffer().nbytes,
                      content_type="application/octet-stream")


def _load_model_bytes(model_type: str, model_key: str, mc):
    import tempfile, pathlib
    if model_type == "scorecard":
        return ScorecardClassifier()
    if model_type in ("xgb", "xgb_pooled"):
        with tempfile.NamedTemporaryFile(suffix=".ubj", delete=False) as tf:
            tmp = tf.name
        mc.fget_object(MINIO_BUCKET, model_key, tmp)
        m = xgb.XGBClassifier()
        m.load_model(tmp)
        pathlib.Path(tmp).unlink(missing_ok=True)
        return m
    # lr / lr_pooled
    resp = mc.get_object(MINIO_BUCKET, model_key)
    return pickle.loads(resp.read())


# ── Data Loading ──────────────────────────────────────────────────────────────

def load_vix(ch) -> pd.DataFrame:
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


def load_events(ch) -> list:
    """Return sorted list of HIGH-impact event dates."""
    r = ch.query(
        "SELECT event_date FROM market.events FINAL "
        "WHERE impact = 'HIGH' ORDER BY event_date"
    )
    return sorted({row[0] for row in r.result_rows})


def extract_event_features(event_dates: list, snap_date: date, expiry: date) -> dict:
    window_events = [d for d in event_dates if snap_date <= d <= expiry]
    event_in_window = 1 if window_events else 0
    future = [d for d in event_dates if d >= snap_date]
    days_to_event = min((future[0] - snap_date).days, 30) if future else 30
    return {
        "event_in_window": float(event_in_window),
        "days_to_event":   float(days_to_event),
    }


def load_index_ohlcv(ch, index_symbol: str) -> pd.DataFrame:
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
    hi, lo, cl = ohlcv["high"], ohlcv["low"], ohlcv["close"]

    tr = pd.concat([
        hi - lo,
        (hi - cl.shift(1)).abs(),
        (lo - cl.shift(1)).abs(),
    ], axis=1).max(axis=1)

    atr14 = tr.ewm(alpha=1/14, adjust=False).mean()
    atr_pct = atr14 / cl * 100
    atr_percentile = atr_pct.rolling(252).rank(pct=True) * 100

    delta = cl.diff()
    avg_g = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
    avg_l = (-delta).clip(lower=0).ewm(com=13, adjust=False).mean()
    rsi14 = 100 - (100 / (1 + avg_g / avg_l.replace(0, np.nan)))

    ret  = cl.pct_change()
    hv5  = ret.rolling(5).std()  * np.sqrt(252) * 100
    hv20 = ret.rolling(20).std() * np.sqrt(252) * 100
    hv_ratio = hv5 / hv20.replace(0, np.nan)

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

    wk = ohlcv[["high","low","close"]].resample("W-THU").agg(
        {"high":"max","low":"min","close":"last"}
    )
    pivot      = (wk["high"] + wk["low"] + wk["close"]) / 3
    wk["cpr_w"] = ((pivot + wk["high"])/2 - (pivot + wk["low"])/2) / pivot * 100
    wk["cpr_w"] = wk["cpr_w"].shift(1)
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

    pivoted = df.pivot_table(
        index="date", columns="entity", values=["call_net", "put_net", "pcr"]
    )
    pivoted.columns = [f"{col[1].lower()}_{col[0]}" for col in pivoted.columns]
    pivoted.reset_index(inplace=True)
    pivoted.set_index("date", inplace=True)
    return pivoted


def load_options_chain(ch, symbol: str) -> pd.DataFrame:
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
    common = ce.index.intersection(pe.index)
    common = common[(ce.loc[common] > 0.5) & (pe.loc[common] > 0.5)]
    if len(common) < 2:
        return None
    diff = (ce.loc[common] - pe.loc[common]).abs()
    return float(diff.idxmin())


def extract_chain_features(chain, snap_date, expiry) -> Optional[dict]:
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

    total_ce_oi = float(ce_oi.sum())
    total_pe_oi = float(pe_oi.sum())

    return {
        "atm_strike":       atm,
        "straddle_premium": straddle_premium,
        "straddle_pct":     straddle_premium / atm * 100,
        "ce_pe_ratio":      float(ce.get(atm, 1) / max(pe.get(atm, 0.01), 0.01)),
        "pcr_oi":           total_pe_oi / max(total_ce_oi, 1),
        "oi_conc_ce":       float(ce_oi.nlargest(3).sum() / max(total_ce_oi, 1)),
        "oi_conc_pe":       float(pe_oi.nlargest(3).sum() / max(total_pe_oi, 1)),
        "atm_oi_ratio":     float(ce_oi.get(atm, 0)) / max(float(pe_oi.get(atm, 0)), 1),
    }


def extract_eod_features(eod, snap_date) -> dict:
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


def extract_poi_features(poi, snap_date) -> dict:
    if snap_date not in poi.index:
        return {}
    row = poi.loc[snap_date]
    return {k: float(v) for k, v in row.items() if pd.notna(v)}


def temporal_features(expiry: date) -> dict:
    return {
        "day_of_week":   expiry.weekday(),
        "week_of_month": (expiry.day - 1) // 7 + 1,
    }


# ── P&L Target ────────────────────────────────────────────────────────────────

def compute_pnl(chain, snap_date, expiry, atm_strike) -> Optional[float]:
    def get_ltp(d):
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

def _tech_row(tech, snap_date, eod) -> dict:
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
    log.info(f"[{symbol}] loading data…")
    chain       = load_options_chain(ch, symbol)
    eod         = load_eod_summary(ch)
    poi         = load_participant_oi(ch)
    vix         = load_vix(ch)
    event_dates = load_events(ch)
    index_sym   = INDEX_MAP.get(symbol, "^NSEI")
    ohlcv       = load_index_ohlcv(ch, index_sym)
    tech        = compute_tech_signals(ohlcv) if not ohlcv.empty else pd.DataFrame()

    expiries = sorted(chain["expiry"].unique())
    log.info(f"[{symbol}] {len(expiries)} expiry dates to process")

    rows = []
    for expiry in expiries:
        snap_candidates = sorted(chain[
            (chain.expiry == expiry) & (chain.snap_date < expiry)
        ]["snap_date"].unique())
        if not snap_candidates:
            continue
        snap_date = snap_candidates[-1]

        chain_feats = extract_chain_features(chain, snap_date, expiry)
        if chain_feats is None:
            continue

        atm = chain_feats["atm_strike"]
        pnl = compute_pnl(chain, snap_date, expiry, atm)
        if pnl is None:
            continue

        row = {
            "expiry":        expiry,
            "entry_date":    snap_date,
            "pnl_pts":       pnl,
            "pnl_pct":       pnl / chain_feats["straddle_premium"] * 100,
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
        row.update(extract_event_features(event_dates, snap_date, expiry))
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["target"] = (df["pnl_pts"] > 0).astype(int)
    df.sort_values("expiry", inplace=True)
    df.reset_index(drop=True, inplace=True)
    log.info(f"[{symbol}] dataset: {len(df)} rows, win_rate={df.target.mean():.1%}")
    return df


def build_dataset_pooled(ch) -> pd.DataFrame:
    """Combine all 4 symbol datasets with symbol one-hot columns."""
    dfs = []
    for sym in SYMBOLS:
        df = build_dataset(ch, sym)
        if not df.empty:
            df = df.copy()
            df["symbol_label"] = sym
            for s in SYMBOLS:
                df[f"sym_{s}"] = float(s == sym)
            dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    combined = pd.concat(dfs, ignore_index=True)
    combined.sort_values("expiry", inplace=True)
    return combined.reset_index(drop=True)


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
    # Technical regime signals
    "atr_percentile", "rsi14", "hv_ratio",
    "supertrend_dir", "cpr_width_pct", "iv_hv5_ratio",
    # Event calendar
    "event_in_window", "days_to_event",
]

POOLED_FEATURE_COLS = FEATURE_COLS + [f"sym_{s}" for s in SYMBOLS]


def _get_features(df: pd.DataFrame, feat_cols: list) -> pd.DataFrame:
    cols = [c for c in feat_cols if c in df.columns]
    return df[cols].fillna(0.0).astype(float)


# ── Walk-Forward Validation ───────────────────────────────────────────────────

def _walk_forward_cv(df: pd.DataFrame, model_type: str,
                     feat_cols: list) -> pd.DataFrame:
    """Generic walk-forward CV. Returns OOS predictions (no DB writes)."""
    if len(df) < MIN_TRAIN + 5:
        return pd.DataFrame()

    df = df.copy()
    df["expiry_dt"] = pd.to_datetime(df["expiry"])
    max_date = df["expiry_dt"].max()
    first_test_start = df.iloc[MIN_TRAIN]["expiry_dt"].to_period("M").to_timestamp()

    all_preds = []
    fold = 0
    test_start = first_test_start

    while test_start <= max_date:
        test_end   = test_start + pd.DateOffset(months=TEST_MONTHS)
        train_start = test_start - pd.DateOffset(months=TRAIN_MONTHS)

        train_df = df[(df.expiry_dt >= train_start) & (df.expiry_dt < test_start)]
        test_df  = df[(df.expiry_dt >= test_start)  & (df.expiry_dt < test_end)]

        if len(train_df) < 15 or test_df.empty:
            test_start = test_end
            continue

        cols = [c for c in feat_cols if c in train_df.columns]
        X_tr = train_df[cols].fillna(0).astype(float)
        y_tr = train_df["target"].values
        X_te = test_df[cols].fillna(0).astype(float)
        for c in X_tr.columns:
            if c not in X_te.columns:
                X_te[c] = 0.0
        X_te = X_te[X_tr.columns]

        model = _make_model(model_type)
        model.fit(X_tr, y_tr)
        proba = model.predict_proba(X_te)[:, 1]

        keep = ["expiry", "entry_date", "pnl_pts", "pnl_pct",
                "target", "atm_strike", "entry_premium", "exit_value"]
        if "symbol_label" in test_df.columns:
            keep.append("symbol_label")
        pred_df = test_df[[c for c in keep if c in test_df.columns]].copy()
        pred_df["confidence"] = proba
        pred_df["fold"] = fold
        all_preds.append(pred_df)
        fold += 1
        test_start = test_end

    if not all_preds:
        return pd.DataFrame()
    return pd.concat(all_preds, ignore_index=True)


def walk_forward_train(df, symbol, ch, mc, model_type="xgb") -> pd.DataFrame:
    """Walk-forward CV + write results to analysis.confidence_backtest."""
    results = _walk_forward_cv(df, model_type, FEATURE_COLS)
    if results.empty:
        log.warning(f"[{symbol}] not enough data for walk-forward ({len(df)} rows)")
        return results

    _insert_backtest_results(ch, symbol, results)

    if len(set(results["target"])) > 1:
        auc = roc_auc_score(results["target"], results["confidence"])
        log.info(
            f"[{symbol}] [{model_type}] walk-forward: {len(results)} OOS, "
            f"AUC={auc:.3f}, win_rate={results.target.mean():.1%}, "
            f"WinConf={results[results.target==1].confidence.mean()*100:.1f} "
            f"LossConf={results[results.target==0].confidence.mean()*100:.1f}"
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

def train_final_model(df, symbol, mc, model_type="xgb") -> Optional[object]:
    """Train on all available data and save per-symbol model to MinIO."""
    if len(df) < MIN_TRAIN:
        log.warning(f"[{symbol}] insufficient data, skipping")
        return None

    cols = [c for c in FEATURE_COLS if c in df.columns]
    X = df[cols].fillna(0).astype(float)
    y = df["target"].values

    model = _make_model(model_type)
    model.fit(X, y)

    if model_type != "scorecard":
        ext = _model_ext(model_type)
        model_key = f"{MODELS_PREFIX}/{symbol}_model.{ext}"
        try:
            _save_model(model, model_type, model_key, mc)
            log.info(f"[{symbol}] model saved: {model_key}")
        except S3Error as e:
            log.warning(f"[{symbol}] failed to save model: {e}")
    else:
        model_key = None

    meta = {
        "features":   cols,
        "n_train":    len(df),
        "win_rate":   float(y.mean()),
        "model_type": model_type,
        "is_pooled":  False,
        "model_key":  model_key,
    }
    buf = io.BytesIO(json.dumps(meta).encode())
    mc.put_object(MINIO_BUCKET, f"{MODELS_PREFIX}/{symbol}_meta.json",
                  buf, length=buf.getbuffer().nbytes, content_type="application/json")
    log.info(f"[{symbol}] final model: {model_type}, n={len(df)}, win_rate={y.mean():.1%}")
    return model


def train_final_model_pooled(pooled_df, mc, model_type="xgb_pooled"):
    """Train one pooled model for all symbols and save shared meta per-symbol."""
    if len(pooled_df) < MIN_TRAIN:
        log.warning("Pooled dataset too small, skipping")
        return None

    cols = [c for c in POOLED_FEATURE_COLS if c in pooled_df.columns]
    X = pooled_df[cols].fillna(0).astype(float)
    y = pooled_df["target"].values

    model = _make_model(model_type)
    model.fit(X, y)

    ext = _model_ext(model_type)
    model_key = f"{MODELS_PREFIX}/POOLED_model.{ext}"
    try:
        _save_model(model, model_type, model_key, mc)
        log.info(f"Pooled model saved: {model_key}")
    except S3Error as e:
        log.warning(f"Failed to save pooled model: {e}")

    for sym in SYMBOLS:
        meta = {
            "features":   cols,
            "n_train":    len(pooled_df),
            "win_rate":   float(y.mean()),
            "model_type": model_type,
            "is_pooled":  True,
            "model_key":  model_key,
        }
        buf = io.BytesIO(json.dumps(meta).encode())
        mc.put_object(MINIO_BUCKET, f"{MODELS_PREFIX}/{sym}_meta.json",
                      buf, length=buf.getbuffer().nbytes, content_type="application/json")
    log.info(f"Pooled final model: {model_type}, n={len(pooled_df)}, win_rate={y.mean():.1%}")
    return model


def load_model(symbol: str, mc) -> tuple:
    """Load trained model from MinIO. Returns (model, meta) or (None, None)."""
    meta_key = f"{MODELS_PREFIX}/{symbol}_meta.json"
    try:
        resp = mc.get_object(MINIO_BUCKET, meta_key)
        meta = json.loads(resp.read())
    except S3Error:
        return None, None

    model_type = meta.get("model_type", "xgb")

    if model_type == "scorecard":
        return ScorecardClassifier(), meta

    model_key = meta.get("model_key")
    if not model_key:
        ext = _model_ext(model_type)
        model_key = (f"{MODELS_PREFIX}/POOLED_model.{ext}"
                     if meta.get("is_pooled")
                     else f"{MODELS_PREFIX}/{symbol}_model.{ext}")

    try:
        return _load_model_bytes(model_type, model_key, mc), meta
    except S3Error:
        return None, None


# ── Production Scoring ────────────────────────────────────────────────────────

def score_today(ch, mc, symbol: str) -> None:
    model, meta = load_model(symbol, mc)
    if model is None:
        log.warning(f"[{symbol}] no trained model found, skipping scoring")
        return

    model_type = meta.get("model_type", "xgb")
    is_pooled  = meta.get("is_pooled", False)
    feat_cols  = meta.get("features", FEATURE_COLS)

    chain       = load_options_chain(ch, symbol)
    eod         = load_eod_summary(ch)
    poi         = load_participant_oi(ch)
    vix         = load_vix(ch)
    event_dates = load_events(ch)
    index_sym   = INDEX_MAP.get(symbol, "^NSEI")
    ohlcv       = load_index_ohlcv(ch, index_sym)
    tech        = compute_tech_signals(ohlcv) if not ohlcv.empty else pd.DataFrame()

    today = date.today()
    snap_dates = sorted(chain["snap_date"].unique(), reverse=True)
    if not snap_dates:
        return

    latest_snap = snap_dates[0]

    future_expiries = sorted([e for e in chain["expiry"].unique() if e > today])
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
    row.update(extract_event_features(event_dates, latest_snap, next_expiry))

    if is_pooled:
        for s in SYMBOLS:
            row[f"sym_{s}"] = float(s == symbol)

    feat_df = pd.DataFrame([row])
    for c in feat_cols:
        if c not in feat_df.columns:
            feat_df[c] = 0.0
    feat_df = feat_df[[c for c in feat_cols if c in feat_df.columns]].fillna(0.0).astype(float)

    confidence = float(model.predict_proba(feat_df)[0, 1]) * 100
    expected_pnl_pct = (confidence - 50) * 1.5

    log.info(f"[{symbol}] [{model_type}] next_expiry={next_expiry} "
             f"confidence={confidence:.1f} expected_pnl_pct={expected_pnl_pct:.1f}%")

    features_json = json.dumps({k: round(float(v), 4) for k, v in row.items()
                                 if k in feat_cols and isinstance(v, (int, float))})

    ch.insert(
        "analysis.confidence_scores",
        [[today, symbol, next_expiry, confidence, expected_pnl_pct, features_json]],
        column_names=["score_date", "symbol", "next_expiry", "confidence",
                      "expected_pnl_pct", "features_json"],
    )
    log.info(f"[{symbol}] confidence score inserted: {confidence:.1f}/100")


# ── Model Comparison ──────────────────────────────────────────────────────────

def compare_models(ch, mc) -> tuple:
    """
    Compare XGBoost per-symbol, LR per-symbol, Scorecard, Pooled XGBoost, Pooled LR.
    Returns (best_model_type, is_pooled) for the winner.
    """
    log.info("=== Building per-symbol datasets ===")
    sym_dfs = {}
    for sym in SYMBOLS:
        df = build_dataset(ch, sym)
        if not df.empty:
            sym_dfs[sym] = df

    log.info("=== Building pooled dataset ===")
    pooled_df = build_dataset_pooled(ch)

    def _auc(preds):
        if preds.empty or len(set(preds["target"])) < 2:
            return float("nan")
        return roc_auc_score(preds["target"], preds["confidence"])

    def _per_symbol_auc(model_type, feat_cols):
        aucs = []
        for sym, df in sym_dfs.items():
            p = _walk_forward_cv(df, model_type, feat_cols)
            a = _auc(p)
            log.info(f"  [{sym}] {model_type}: AUC={a:.3f} n_oos={len(p)}")
            if not np.isnan(a):
                aucs.append(a)
        return float(np.mean(aucs)) if aucs else float("nan")

    results: dict[tuple, float] = {}

    log.info("--- XGBoost per-symbol (baseline) ---")
    results[("xgb", False)] = _per_symbol_auc("xgb", FEATURE_COLS)

    log.info("--- Logistic Regression per-symbol ---")
    results[("lr", False)] = _per_symbol_auc("lr", FEATURE_COLS)

    log.info("--- Rule-based Scorecard ---")
    results[("scorecard", False)] = _per_symbol_auc("scorecard", FEATURE_COLS)

    if not pooled_df.empty:
        log.info("--- Pooled XGBoost ---")
        p = _walk_forward_cv(pooled_df, "xgb_pooled", POOLED_FEATURE_COLS)
        results[("xgb_pooled", True)] = _auc(p)
        log.info(f"  [POOLED] xgb_pooled: AUC={results[('xgb_pooled', True)]:.3f} n_oos={len(p)}")

        log.info("--- Pooled Logistic Regression ---")
        p = _walk_forward_cv(pooled_df, "lr_pooled", POOLED_FEATURE_COLS)
        results[("lr_pooled", True)] = _auc(p)
        log.info(f"  [POOLED] lr_pooled: AUC={results[('lr_pooled', True)]:.3f} n_oos={len(p)}")

    sorted_r = sorted(results.items(),
                      key=lambda x: x[1] if not np.isnan(x[1]) else -1,
                      reverse=True)
    best_key, best_auc = sorted_r[0]

    print("\n" + "=" * 58)
    print(f"  {'MODEL TYPE':<22} {'POOLED':<8} {'AVG AUC'}")
    print("  " + "-" * 54)
    for (mt, pooled), auc_v in sorted_r:
        marker = " ← BEST" if (mt, pooled) == best_key else ""
        auc_s = f"{auc_v:.3f}" if not np.isnan(auc_v) else "  n/a"
        print(f"  {mt:<22} {'yes' if pooled else 'no':<8} {auc_s}{marker}")
    print("=" * 58 + "\n")

    best_type, is_pooled = best_key
    log.info(f"Winner: {best_type} (pooled={is_pooled}) AUC={best_auc:.3f}")
    return best_type, is_pooled


# ── Main ──────────────────────────────────────────────────────────────────────

def run_symbol(symbol, ch, mc, backtest_only, score_only, model_type="xgb"):
    if not score_only:
        df = build_dataset(ch, symbol)
        if df.empty:
            log.warning(f"[{symbol}] empty dataset, skipping")
            return
        walk_forward_train(df, symbol, ch, mc, model_type)
        train_final_model(df, symbol, mc, model_type)

    if not backtest_only:
        score_today(ch, mc, symbol)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", help="Single symbol to run (default: all)")
    parser.add_argument("--backtest-only", action="store_true")
    parser.add_argument("--score-only", action="store_true")
    parser.add_argument("--compare", action="store_true",
                        help="Compare all model types, train & save the best")
    parser.add_argument("--model-type", default="xgb",
                        choices=["xgb", "lr", "scorecard", "xgb_pooled", "lr_pooled"],
                        help="Model type for training (ignored when --compare)")
    args = parser.parse_args()

    ch = get_ch()
    mc = get_mc()
    if not mc.bucket_exists(MINIO_BUCKET):
        mc.make_bucket(MINIO_BUCKET)

    started_at = datetime.utcnow()
    try:
        if args.compare:
            best_type, is_pooled = compare_models(ch, mc)

            log.info(f"Training final model: {best_type} (pooled={is_pooled})")
            if is_pooled:
                pooled_df = build_dataset_pooled(ch)
                train_final_model_pooled(pooled_df, mc, best_type)
            else:
                target_syms = [args.symbol] if args.symbol else SYMBOLS
                for sym in target_syms:
                    try:
                        df = build_dataset(ch, sym)
                        if not df.empty:
                            train_final_model(df, sym, mc, best_type)
                    except Exception as e:
                        log.error(f"[{sym}] failed: {e}", exc_info=True)
        else:
            symbols = [args.symbol] if args.symbol else SYMBOLS
            for sym in symbols:
                try:
                    run_symbol(sym, ch, mc, args.backtest_only, args.score_only, args.model_type)
                except Exception as e:
                    log.error(f"[{sym}] failed: {e}", exc_info=True)

        log.info("confidence_scorer done")
        _record_run(ch, "success", started_at)
    except Exception as e:
        log.error(f"confidence_scorer fatal: {e}", exc_info=True)
        _record_run(ch, "failed", started_at, str(e))


if __name__ == "__main__":
    main()
