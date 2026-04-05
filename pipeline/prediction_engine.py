#!/usr/bin/env python3
"""
prediction_engine.py
────────────────────
Scans all symbols daily, computes today's feature vector,
matches against active patterns (★★★+), and writes predictions
to analysis.predictions.

Run after market close (3:30 PM IST) each trading day.

How confidence is computed:
  base_confidence = pattern win_rate (positive_rate from backtests)
  adjusted for:
    • star multiplier  — ★★★★ gets 1.1x, ★★★ gets 1.0x
    • recency boost    — if recency_score > 0.5: +5%
    • VIX penalty      — if vix_level > 20: -10%
  final_confidence = min(95, base × star_mult × recency × vix)
  Only predictions with confidence >= MIN_CONFIDENCE inserted.

predicted_direction:
  Taken from the most recent event direction for that symbol
  (the event that triggered pattern matching). If no recent event,
  defaults to the direction most common in pattern's backtest history.

predicted_min_pct:
  Always 2.0 (the detection threshold used throughout the system).

Status lifecycle:
  'open' → validated by validation_engine.py → 'hit' | 'miss' | 'expired'

Usage:
  python prediction_engine.py              # today's predictions
  python prediction_engine.py --date 2026-03-01  # historical replay
  python prediction_engine.py --symbol RELIANCE.NS  # single symbol debug
  python prediction_engine.py --dry-run   # compute but don't insert
"""

import os
import json
import logging
import argparse
import threading
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import clickhouse_connect

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

MAX_WORKERS      = 8
MIN_CONFIDENCE   = 60.0   # minimum confidence % to emit a prediction
MIN_STARS        = 3      # minimum star rating to use a pattern
PREDICTED_MIN_PCT = 2.0   # minimum expected move %
HISTORY_DAYS     = 300    # OHLCV lookback for feature computation
MIN_HISTORY_ROWS = 30     # minimum rows needed to compute features

# ── Results tracker ────────────────────────────────────
results = {
    "symbols_scanned": 0,
    "predictions_made": 0,
    "skipped": 0,
    "failed": [],
}
results_lock = threading.Lock()


# ── ClickHouse client ──────────────────────────────────
def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ══════════════════════════════════════════════════════
# Load eligible patterns (★★★+)
# ══════════════════════════════════════════════════════

def fetch_eligible_patterns(ch) -> list[dict]:
    """
    Fetch patterns with stars >= MIN_STARS and needs_more_data = 0.
    Joins pattern_ratings with patterns to get conditions + stats.
    """
    result = ch.query(
        "SELECT r.pattern_id, p.label, p.conditions_json, "
        "       r.stars, r.win_rate, r.recency_score "
        "FROM (SELECT pattern_id, stars, win_rate, recency_score "
        "      FROM analysis.pattern_ratings FINAL "
        "      WHERE symbol = '*' "
        "        AND stars >= {min_stars:UInt8} "
        "        AND needs_more_data = 0) AS r "
        "JOIN (SELECT pattern_id, label, conditions_json "
        "      FROM analysis.patterns FINAL "
        "      WHERE is_active = 1) AS p "
        "ON r.pattern_id = p.pattern_id "
        "ORDER BY r.stars DESC",
        parameters={"min_stars": MIN_STARS}
    )

    patterns = []
    for row in result.result_rows:
        try:
            conditions = json.loads(row[2])
            patterns.append({
                "pattern_id":    row[0],
                "label":         row[1],
                "conditions":    conditions,
                "stars":         int(row[3]),
                "win_rate":      float(row[4]),
                "recency_score": float(row[5]),
            })
        except Exception as e:
            log.warning(f"Skipping pattern {row[0]}: {e}")

    return patterns


# ══════════════════════════════════════════════════════
# Feature computation (reused from pattern_feature_extractor)
# ══════════════════════════════════════════════════════

def compute_rsi(close: pd.Series, period: int) -> float:
    if len(close) < period + 1:
        return 0.0
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs       = avg_gain.iloc[-1] / avg_loss.iloc[-1] \
               if avg_loss.iloc[-1] != 0 else 0
    return round(float(100 - (100 / (1 + rs))), 4)


def compute_sma(close: pd.Series, window: int) -> float:
    if len(close) < window:
        return 0.0
    return round(float(close.rolling(window).mean().iloc[-1]), 4)


def compute_crossover(close: pd.Series, window: int) -> int:
    if len(close) < window + 1:
        return 0
    ma        = close.rolling(window).mean()
    prev_diff = close.iloc[-2] - ma.iloc[-2]
    curr_diff = close.iloc[-1] - ma.iloc[-1]
    if prev_diff < 0 and curr_diff >= 0:
        return 1
    if prev_diff > 0 and curr_diff <= 0:
        return -1
    return 0


def compute_price_vs_ma(close: pd.Series, window: int) -> float:
    ma = compute_sma(close, window)
    if ma == 0:
        return 0.0
    return round((close.iloc[-1] - ma) / ma * 100, 4)


def compute_volume_zscore(volume: pd.Series, window: int) -> float:
    if len(volume) < window + 1:
        return 0.0
    mean = volume.rolling(window).mean().iloc[-1]
    std  = volume.rolling(window).std().iloc[-1]
    if std == 0 or np.isnan(std):
        return 0.0
    return round(float((volume.iloc[-1] - mean) / std), 4)


def compute_volume_trend(volume: pd.Series, short=5, long=20) -> str:
    if len(volume) < long:
        return "flat"
    avg_short = volume.iloc[-short:].mean()
    avg_long  = volume.iloc[-long:].mean()
    if avg_long == 0:
        return "flat"
    ratio = avg_short / avg_long
    if ratio > 1.1:
        return "expanding"
    if ratio < 0.9:
        return "contracting"
    return "flat"


def compute_atr(df: pd.DataFrame, period: int = 14) -> float:
    if len(df) < period + 1:
        return 0.0
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(period).mean().iloc[-1]
    return round(float(atr), 4) if not np.isnan(atr) else 0.0


def compute_bb_position(close: pd.Series, window=20, num_std=2.0) -> float:
    if len(close) < window:
        return 0.5
    ma    = close.rolling(window).mean().iloc[-1]
    std   = close.rolling(window).std().iloc[-1]
    upper = ma + num_std * std
    lower = ma - num_std * std
    width = upper - lower
    if width == 0:
        return 0.5
    return round(float(np.clip((close.iloc[-1] - lower) / width, 0, 1)), 4)


def compute_return_nd(close: pd.Series, n: int) -> float:
    if len(close) < n + 1:
        return 0.0
    past = close.iloc[-(n+1)]
    curr = close.iloc[-1]
    return round((curr - past) / past * 100, 4) if past > 0 else 0.0


def compute_drawdown(close: pd.Series, window=252) -> float:
    if len(close) < 2:
        return 0.0
    lookback = close.iloc[-min(window, len(close)):]
    high_52w  = lookback.max()
    curr      = close.iloc[-1]
    return round((high_52w - curr) / high_52w * 100, 4) if high_52w > 0 else 0.0


def build_feature_vector(df_hist: pd.DataFrame,
                         vix_level: float,
                         vix_regime: str,
                         nifty_5d: float,
                         fii_net_3d: float) -> dict | None:
    """
    Build today's feature vector from OHLCV history.
    df_hist must be sorted ascending, ending on today (or last trading day).
    Returns None if insufficient data.
    """
    if len(df_hist) < MIN_HISTORY_ROWS:
        return None

    close  = df_hist["close"]
    volume = df_hist["volume"]

    return {
        "rsi_14":          compute_rsi(close, 14),
        "rsi_5":           compute_rsi(close, 5),
        "sma_20_cross":    compute_crossover(close, 20),
        "sma_50_cross":    compute_crossover(close, 50),
        "ema_20_cross":    compute_crossover(
            close.ewm(span=20, adjust=False).mean(), 1
        ),
        "price_vs_sma20":  compute_price_vs_ma(close, 20),
        "price_vs_sma50":  compute_price_vs_ma(close, 50),
        "price_vs_sma200": compute_price_vs_ma(close, 200),
        "volume_z5":       compute_volume_zscore(volume, 5),
        "volume_z20":      compute_volume_zscore(volume, 20),
        "volume_trend":    compute_volume_trend(volume),
        "atr_14":          compute_atr(df_hist, 14),
        "atr_pct":         round(
            compute_atr(df_hist, 14) / close.iloc[-1] * 100, 4
        ) if close.iloc[-1] > 0 else 0.0,
        "bb_position":     compute_bb_position(close),
        "vix_level":       vix_level,
        "vix_regime":      vix_regime,
        "nifty_trend_5d":  nifty_5d,
        "fii_net_3d":      fii_net_3d,
        "return_5d":       compute_return_nd(close, 5),
        "return_20d":      compute_return_nd(close, 20),
        "drawdown_pct":    compute_drawdown(close, 252),
    }


# ══════════════════════════════════════════════════════
# Condition evaluator (same as pattern_builder)
# ══════════════════════════════════════════════════════

def evaluate_condition(value, condition: dict) -> bool:
    if "lt"      in condition: return value < condition["lt"]
    if "lte"     in condition: return value <= condition["lte"]
    if "gt"      in condition: return value > condition["gt"]
    if "gte"     in condition: return value >= condition["gte"]
    if "eq"      in condition: return value == condition["eq"]
    if "between" in condition:
        lo, hi = condition["between"]
        return lo <= value <= hi
    if "in"      in condition: return value in condition["in"]
    return False


def matches_pattern(features: dict, conditions: dict) -> bool:
    for feature, condition in conditions.items():
        if feature not in features:
            return False
        if not evaluate_condition(features[feature], condition):
            return False
    return True


# ══════════════════════════════════════════════════════
# Confidence computation
# ══════════════════════════════════════════════════════

def compute_confidence(pattern: dict,
                       features: dict) -> float:
    """
    Compute prediction confidence % for a matched pattern.

    base  = pattern win_rate × 100
    × star_mult  (★★★★ = 1.10, ★★★ = 1.00)
    × recency_mult (recency > 0.5 → +5%)
    × vix_mult   (vix > 20 → -10%)
    capped at 95.0
    """
    base = pattern["win_rate"] * 100

    star_mult   = 1.10 if pattern["stars"] >= 4 else 1.00
    recency_mult = 1.05 if pattern["recency_score"] > 0.5 else 1.00
    vix_level   = features.get("vix_level", 0)
    vix_mult    = 0.90 if vix_level > 20 else 1.00

    confidence = base * star_mult * recency_mult * vix_mult
    return round(min(95.0, confidence), 2)


# ══════════════════════════════════════════════════════
# Data fetching helpers
# ══════════════════════════════════════════════════════

def fetch_all_symbols(ch) -> list[tuple[str, str]]:
    result = ch.query(
        "SELECT DISTINCT symbol, market "
        "FROM market.ohlcv_daily FINAL "
        "ORDER BY market, symbol"
    )
    return [(row[0], row[1]) for row in result.result_rows]


def fetch_ohlcv(ch, symbol: str, market: str,
                as_of_date: date) -> pd.DataFrame:
    fetch_from = as_of_date - timedelta(days=HISTORY_DAYS)

    result = ch.query(
        "SELECT date, open, high, low, close, volume "
        "FROM market.ohlcv_daily FINAL "
        "WHERE symbol = {sym:String} AND market = {mkt:String} "
        "  AND date >= {d_from:Date} AND date <= {d_to:Date} "
        "ORDER BY date",
        parameters={
            "sym":    symbol,
            "mkt":    market,
            "d_from": fetch_from,
            "d_to":   as_of_date,
        }
    )

    if not result.result_rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        result.result_rows,
        columns=["date", "open", "high", "low", "close", "volume"]
    )
    df["date"]   = pd.to_datetime(df["date"]).dt.date
    df["volume"] = df["volume"].astype(float)
    return df.sort_values("date").reset_index(drop=True)


def fetch_context(ch, as_of_date: date) -> tuple[float, str, float, float]:
    """
    Fetch VIX level, regime, Nifty 5d return, FII net 3d for a given date.
    Returns (vix_level, vix_regime, nifty_5d, fii_net_3d).
    """
    # VIX from nifty_live
    vix_level  = 0.0
    vix_regime = ""
    try:
        r = ch.query(
            "SELECT avg(vix) FROM market.nifty_live FINAL "
            "WHERE toDate(timestamp) = {d:Date}",
            parameters={"d": as_of_date}
        )
        v = r.result_rows[0][0]
        if v:
            vix_level = float(v)
    except Exception:
        pass

    # Derive regime if not available
    if not vix_regime:
        if vix_level <= 0:      vix_regime = ""
        elif vix_level < 13:    vix_regime = "low"
        elif vix_level <= 18:   vix_regime = "normal"
        elif vix_level <= 25:   vix_regime = "elevated"
        else:                   vix_regime = "extreme"

    # Nifty 5d
    nifty_5d = 0.0
    try:
        r = ch.query(
            "SELECT date, close FROM market.ohlcv_daily FINAL "
            "WHERE symbol = 'NIFTYBEES.NS' AND market = 'indian' "
            "  AND date <= {d:Date} "
            "ORDER BY date DESC LIMIT 6",
            parameters={"d": as_of_date}
        )
        rows = r.result_rows
        if len(rows) >= 2:
            latest = rows[0][1]
            oldest = rows[-1][1]
            if oldest > 0:
                nifty_5d = round((latest - oldest) / oldest * 100, 4)
    except Exception:
        pass

    # FII net 3d
    fii_net_3d = 0.0
    try:
        from_d = as_of_date - timedelta(days=5)
        r = ch.query(
            "SELECT sum(net_value) FROM market.fii_dii FINAL "
            "WHERE entity = 'FII' "
            "  AND date >= {d_from:Date} AND date < {d_to:Date}",
            parameters={"d_from": from_d, "d_to": as_of_date}
        )
        v = r.result_rows[0][0]
        fii_net_3d = float(v) if v else 0.0
    except Exception:
        pass

    return vix_level, vix_regime, nifty_5d, fii_net_3d


def fetch_recent_event_directions(ch, as_of_date: date,
                                  lookback_days: int = 30) -> dict:
    """
    Get the most recent event direction per symbol in last lookback_days.
    Returns {symbol: 'UP'|'DOWN'}
    """
    from_date = as_of_date - timedelta(days=lookback_days)
    result = ch.query(
        "SELECT symbol, argMax(direction, date) as last_direction "
        "FROM analysis.detected_events FINAL "
        "WHERE date >= {d_from:Date} AND date <= {d_to:Date} "
        "GROUP BY symbol",
        parameters={"d_from": from_date, "d_to": as_of_date}
    )
    return {row[0]: row[1] for row in result.result_rows}


# ══════════════════════════════════════════════════════
# Insert
# ══════════════════════════════════════════════════════

PREDICTION_COLS = [
    "prediction_date", "target_date", "symbol", "market",
    "pattern_id", "stars", "confidence_pct",
    "predicted_direction", "predicted_min_pct",
    "features_json", "status", "created_at", "version",
]


def insert_predictions(ch, rows: list[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)[PREDICTION_COLS]
    ch.insert_df("analysis.predictions", df)


# ══════════════════════════════════════════════════════
# Per-symbol worker
# ══════════════════════════════════════════════════════

def process_symbol(symbol: str, market: str,
                   patterns: list[dict],
                   as_of_date: date,
                   target_date: date,
                   vix_level: float,
                   vix_regime: str,
                   nifty_5d: float,
                   fii_net_3d: float,
                   direction_map: dict,
                   dry_run: bool,
                   idx: int, total: int) -> list[dict]:
    ch = get_ch_client()
    predictions = []

    try:
        df_ohlcv = fetch_ohlcv(ch, symbol, market, as_of_date)

        if df_ohlcv.empty or len(df_ohlcv) < MIN_HISTORY_ROWS:
            with results_lock:
                results["skipped"] += 1
            return []

        features = build_feature_vector(
            df_ohlcv, vix_level, vix_regime, nifty_5d, fii_net_3d
        )

        if features is None:
            with results_lock:
                results["skipped"] += 1
            return []

        now_ts = datetime.now()
        version_ts = int(now_ts.timestamp())

        for pattern in patterns:
            if matches_pattern(features, pattern["conditions"]):
                confidence = compute_confidence(pattern, features)

                if confidence < MIN_CONFIDENCE:
                    continue

                direction = direction_map.get(symbol, "UP")

                predictions.append({
                    "prediction_date":    as_of_date,
                    "target_date":        target_date,
                    "symbol":             symbol,
                    "market":             market,
                    "pattern_id":         pattern["pattern_id"],
                    "stars":              pattern["stars"],
                    "confidence_pct":     confidence,
                    "predicted_direction": direction,
                    "predicted_min_pct":  PREDICTED_MIN_PCT,
                    "features_json":      json.dumps({
                        k: v for k, v in features.items()
                        if k in [
                            "rsi_14", "volume_z5", "price_vs_sma20",
                            "bb_position", "return_5d", "vix_level",
                            "sma_20_cross", "volume_trend"
                        ]
                    }),
                    "status":             "open",
                    "created_at":         now_ts,
                    "version":            version_ts,
                })

        with results_lock:
            results["symbols_scanned"] += 1
            results["predictions_made"] += len(predictions)

        if predictions and not dry_run:
            insert_predictions(ch, predictions)

    except Exception as e:
        log.error(f"  FAILED [{symbol}]: {e}")
        with results_lock:
            results["failed"].append(f"{symbol} → {e}")

    return predictions


# ══════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════

def print_summary(all_predictions: list[dict], dry_run: bool):
    print("\n" + "=" * 65)
    print("PREDICTION ENGINE — SUMMARY")
    print("=" * 65)

    if dry_run:
        print("⚠️  DRY RUN — predictions NOT inserted into DB")

    print(f"📊 Symbols scanned    : {results['symbols_scanned']:,}")
    print(f"🎯 Predictions made   : {results['predictions_made']:,}")
    print(f"⏭️  Skipped            : {results['skipped']:,}")
    print(f"❌ Failed             : {len(results['failed'])}")

    if all_predictions:
        print(f"\nTop predictions by confidence:")
        print(f"{'Symbol':<20} {'Pattern':<8} {'Stars':<8} {'Conf%':>6} {'Dir':>5}")
        print("-" * 55)
        top = sorted(all_predictions,
                     key=lambda x: x["confidence_pct"],
                     reverse=True)[:20]
        for p in top:
            stars_str = "★" * p["stars"] + "☆" * (5 - p["stars"])
            print(
                f"{p['symbol']:<20} "
                f"{p['pattern_id']:<8} "
                f"{stars_str:<8} "
                f"{p['confidence_pct']:>5.1f}% "
                f"{p['predicted_direction']:>5}"
            )
    print("=" * 65)


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Prediction Engine — generates daily trading predictions"
    )
    parser.add_argument("--date",    type=str, default=None,
                        help="Prediction date YYYY-MM-DD (default: today)")
    parser.add_argument("--symbol",  type=str, default=None,
                        help="Debug: process single symbol")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute but don't insert predictions")
    args = parser.parse_args()

    # Determine dates
    if args.date:
        as_of_date = date.fromisoformat(args.date)
    else:
        as_of_date = date.today()
    # Next trading day — skip weekends entirely
    target_date = as_of_date + timedelta(days=1)
    while target_date.weekday() >= 5:   # 5=Saturday, 6=Sunday
        target_date += timedelta(days=1)

    log.info("=== Prediction Engine Starting ===")
    log.info(f"Prediction date : {as_of_date}")
    log.info(f"Target date     : {target_date}")
    log.info(f"Dry run         : {args.dry_run}")
    log.info(f"Started         : {datetime.now()}")

    ch = get_ch_client()
    log.info("ClickHouse connected ✅")

    # ── Load eligible patterns ─────────────────────────
    patterns = fetch_eligible_patterns(ch)
    if not patterns:
        log.warning("No eligible patterns (★★★+). Run star_rater first.")
        return

    log.info(f"Eligible patterns: {len(patterns)}")
    for p in patterns:
        log.info(f"  [{p['pattern_id']}] {'★'*p['stars']} {p['label']}")

    # ── Load market context (one fetch, shared) ────────
    log.info("Fetching market context...")
    vix_level, vix_regime, nifty_5d, fii_net_3d = fetch_context(ch, as_of_date)
    log.info(f"VIX={vix_level} | regime={vix_regime or 'unknown'} | "
             f"nifty_5d={nifty_5d}% | fii_net_3d={fii_net_3d}")

    # ── Load recent event directions ───────────────────
    log.info("Fetching recent event directions...")
    direction_map = fetch_recent_event_directions(ch, as_of_date)
    log.info(f"Direction map: {len(direction_map)} symbols")

    # ── Load symbols ───────────────────────────────────
    if args.symbol:
        r = ch.query(
            "SELECT DISTINCT market FROM market.ohlcv_daily FINAL "
            "WHERE symbol = {sym:String} LIMIT 1",
            parameters={"sym": args.symbol}
        )
        if not r.result_rows:
            log.error(f"Symbol {args.symbol} not found.")
            return
        all_symbols = [(args.symbol, r.result_rows[0][0])]
    else:
        all_symbols = fetch_all_symbols(ch)

    total = len(all_symbols)
    log.info(f"Symbols to scan : {total:,}")
    log.info(f"Workers         : {MAX_WORKERS}")

    all_predictions = []
    lock = threading.Lock()

    def _worker(item):
        idx, (symbol, market) = item
        preds = process_symbol(
            symbol, market, patterns,
            as_of_date, target_date,
            vix_level, vix_regime, nifty_5d, fii_net_3d,
            direction_map, args.dry_run,
            idx, total
        )
        if preds:
            with lock:
                all_predictions.extend(preds)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(_worker, enumerate(all_symbols, 1))

    print_summary(all_predictions, args.dry_run)
    log.info(f"Finished : {datetime.now()}")


if __name__ == "__main__":
    main()