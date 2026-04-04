#!/usr/bin/env python3
"""
pattern_feature_extractor.py
────────────────────────────
Computes a 20-dimension feature vector for every detected event in
analysis.detected_events where processed = 0.

ALL features are computed on (event_date - 1) — the last trading day
BEFORE the event. This is critical: computing on the event day itself
leaks forward information and produces backtest results that look
great but fail in production.

Feature groups:
  Momentum   — RSI-14, RSI-5, SMA/EMA crossovers, price vs MAs
  Volume     — z-score vs 5d and 20d, trend classification
  Volatility — ATR-14%, Bollinger Band position
  Context    — VIX level + regime, Nifty 5d return, FII net 3d
  Return     — symbol 5d + 20d return, drawdown from 52w high

After inserting into analysis.pattern_features, marks the source
row in analysis.detected_events as processed = 1.

Incremental logic:
  • Queries detected_events WHERE processed = 0 in one shot
  • Skips symbols with insufficient history (< 30 rows before event)
  • Parallel: each thread gets its own CH connection
  • Idempotent: ReplacingMergeTree deduplicates on (symbol, event_date)

Usage:
  python pattern_feature_extractor.py           # process all unprocessed events
  python pattern_feature_extractor.py --full    # recompute all events (ignore processed flag)
  python pattern_feature_extractor.py --symbol RELIANCE.NS
  python pattern_feature_extractor.py --market indian
"""

import os
import sys
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

MAX_WORKERS       = 8
BATCH_SIZE        = 100       # log progress every N events
HISTORY_DAYS      = 300       # days of OHLCV to fetch per symbol for rolling windows
MIN_HISTORY_ROWS  = 30        # skip if fewer rows available before event date
FEATURE_VERSION   = "v1"

# ── Results tracker ────────────────────────────────────
results = {
    "processed": 0,
    "skipped":   0,
    "failed":    [],
}
results_lock = threading.Lock()


# ── ClickHouse client ──────────────────────────────────
def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ══════════════════════════════════════════════════════
# Data fetching
# ══════════════════════════════════════════════════════

def fetch_unprocessed_events(ch, symbol=None, market=None,
                             full=False) -> pd.DataFrame:
    """
    Fetch events from analysis.detected_events that need feature extraction.
    full=True  → reprocess all events regardless of processed flag
    full=False → only where processed = 0
    """
    conditions = []
    params     = {}

    if not full:
        conditions.append("processed = 0")

    if symbol:
        conditions.append("symbol = {sym:String}")
        params["sym"] = symbol

    if market:
        conditions.append("market = {mkt:String}")
        params["mkt"] = market

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    result = ch.query(
        f"SELECT symbol, market, date "
        f"FROM analysis.detected_events FINAL "
        f"{where} "
        f"ORDER BY symbol, date",
        parameters=params
    )

    if not result.result_rows:
        return pd.DataFrame(columns=["symbol", "market", "date"])

    df = pd.DataFrame(result.result_rows, columns=["symbol", "market", "date"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def fetch_ohlcv_history(ch, symbol: str, market: str,
                        up_to_date: date) -> pd.DataFrame:
    """
    Fetch OHLCV for one symbol from (up_to_date - HISTORY_DAYS) to up_to_date - 1.
    We fetch up to (event_date - 1) only — no lookahead.
    """
    fetch_from = up_to_date - timedelta(days=HISTORY_DAYS)
    fetch_to   = up_to_date - timedelta(days=1)

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
            "d_to":   fetch_to,
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


def fetch_fii_net(ch, as_of_date: date, days: int = 3) -> float:
    """
    Sum of FII net_value over last `days` days before as_of_date.
    Returns 0.0 if market.fii_dii has no data.
    """
    from_date = as_of_date - timedelta(days=days + 5)
    try:
        result = ch.query(
            "SELECT sum(net_value) "
            "FROM market.fii_dii FINAL "
            "WHERE entity = 'FII' "
            "  AND date >= {d_from:Date} AND date < {d_to:Date}",
            parameters={"d_from": from_date, "d_to": as_of_date}
        )
        val = result.result_rows[0][0]
        return float(val) if val is not None else 0.0
    except Exception:
        return 0.0


def fetch_nifty_return_5d(ch, as_of_date: date) -> float:
    """
    5-day return of NIFTYBEES.NS ending on (as_of_date - 1).
    Returns 0.0 if not available.
    """
    fetch_from = as_of_date - timedelta(days=15)
    fetch_to   = as_of_date - timedelta(days=1)
    try:
        result = ch.query(
            "SELECT date, close "
            "FROM market.ohlcv_daily FINAL "
            "WHERE symbol = 'NIFTYBEES.NS' AND market = 'indian' "
            "  AND date >= {d_from:Date} AND date <= {d_to:Date} "
            "ORDER BY date DESC LIMIT 6",
            parameters={"d_from": fetch_from, "d_to": fetch_to}
        )
        rows = result.result_rows
        if len(rows) < 2:
            return 0.0
        latest = rows[0][1]
        oldest = rows[-1][1]
        if oldest <= 0:
            return 0.0
        return round((latest - oldest) / oldest * 100, 4)
    except Exception:
        return 0.0


# ══════════════════════════════════════════════════════
# Feature computation — pure functions
# ══════════════════════════════════════════════════════

def compute_rsi(close: pd.Series, period: int) -> float:
    """Wilder's RSI. Returns last value or 0."""
    if len(close) < period + 1:
        return 0.0
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False,
                        min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False,
                        min_periods=period).mean()
    rs       = avg_gain.iloc[-1] / avg_loss.iloc[-1] \
               if avg_loss.iloc[-1] != 0 else 0
    return round(float(100 - (100 / (1 + rs))), 4)


def compute_sma(close: pd.Series, window: int) -> float:
    """Last SMA value or 0."""
    if len(close) < window:
        return 0.0
    return round(float(close.rolling(window).mean().iloc[-1]), 4)


def compute_ema(close: pd.Series, span: int) -> float:
    """Last EMA value or 0."""
    if len(close) < span:
        return 0.0
    return round(float(
        close.ewm(span=span, adjust=False, min_periods=span).mean().iloc[-1]
    ), 4)


def compute_crossover(close: pd.Series, window: int) -> int:
    """
    +1 if close crossed above MA on last bar (was below, now above)
    -1 if close crossed below MA
     0 if no cross
    """
    if len(close) < window + 1:
        return 0
    ma       = close.rolling(window).mean()
    prev_diff = close.iloc[-2] - ma.iloc[-2]
    curr_diff = close.iloc[-1] - ma.iloc[-1]
    if prev_diff < 0 and curr_diff >= 0:
        return 1
    if prev_diff > 0 and curr_diff <= 0:
        return -1
    return 0


def compute_price_vs_ma(close: pd.Series, window: int) -> float:
    """% price is above/below MA. Positive = above."""
    ma = compute_sma(close, window)
    if ma == 0:
        return 0.0
    return round((close.iloc[-1] - ma) / ma * 100, 4)


def compute_volume_zscore(volume: pd.Series, window: int) -> float:
    """Z-score of latest volume vs rolling mean/std."""
    if len(volume) < window + 1:
        return 0.0
    rolling_mean = volume.rolling(window).mean()
    rolling_std  = volume.rolling(window).std()
    mean = rolling_mean.iloc[-1]
    std  = rolling_std.iloc[-1]
    if std == 0 or np.isnan(std):
        return 0.0
    return round(float((volume.iloc[-1] - mean) / std), 4)


def compute_volume_trend(volume: pd.Series, short=5, long=20) -> str:
    """
    'expanding' if short-window avg > long-window avg by >10%
    'contracting' if short < long by >10%
    'flat' otherwise
    """
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
    """True Range ATR. Returns last value or 0."""
    if len(df) < period + 1:
        return 0.0
    high  = df["high"]
    low   = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(period).mean().iloc[-1]
    return round(float(atr), 4) if not np.isnan(atr) else 0.0


def compute_bb_position(close: pd.Series, window: int = 20,
                        num_std: float = 2.0) -> float:
    """
    Bollinger Band position: 0.0 = at lower band, 1.0 = at upper band.
    Returns 0.5 if bands are flat (no volatility).
    """
    if len(close) < window:
        return 0.5
    ma   = close.rolling(window).mean().iloc[-1]
    std  = close.rolling(window).std().iloc[-1]
    upper = ma + num_std * std
    lower = ma - num_std * std
    band_width = upper - lower
    if band_width == 0:
        return 0.5
    pos = (close.iloc[-1] - lower) / band_width
    return round(float(np.clip(pos, 0.0, 1.0)), 4)


def compute_return_nd(close: pd.Series, n: int) -> float:
    """n-day return ending at last row. Returns 0 if insufficient data."""
    if len(close) < n + 1:
        return 0.0
    past = close.iloc[-(n+1)]
    curr = close.iloc[-1]
    if past <= 0:
        return 0.0
    return round((curr - past) / past * 100, 4)


def compute_drawdown(close: pd.Series, window: int = 252) -> float:
    """% below rolling 52-week high. Positive value = below high."""
    if len(close) < 2:
        return 0.0
    lookback = close.iloc[-min(window, len(close)):]
    high_52w  = lookback.max()
    curr      = close.iloc[-1]
    if high_52w <= 0:
        return 0.0
    return round((high_52w - curr) / high_52w * 100, 4)


# ══════════════════════════════════════════════════════
# Master feature builder
# ══════════════════════════════════════════════════════

def compute_features(df_hist: pd.DataFrame,
                     symbol: str,
                     market: str,
                     event_date: date,
                     vix_level: float,
                     vix_regime: str,
                     nifty_5d: float,
                     fii_net_3d: float) -> dict | None:
    """
    Compute full feature vector from OHLCV history up to (event_date - 1).
    Returns None if insufficient data.
    """
    if len(df_hist) < MIN_HISTORY_ROWS:
        return None

    close  = df_hist["close"]
    volume = df_hist["volume"]

    features = {
        "event_date":     event_date,
        "symbol":         symbol,
        "market":         market,

        # ── Momentum ──────────────────────────────────
        "rsi_14":         compute_rsi(close, 14),
        "rsi_5":          compute_rsi(close, 5),
        "sma_20_cross":   compute_crossover(close, 20),
        "sma_50_cross":   compute_crossover(close, 50),
        "ema_20_cross":   compute_crossover(
            close.ewm(span=20, adjust=False).mean(), 1
        ),
        "price_vs_sma20":  compute_price_vs_ma(close, 20),
        "price_vs_sma50":  compute_price_vs_ma(close, 50),
        "price_vs_sma200": compute_price_vs_ma(close, 200),

        # ── Volume ────────────────────────────────────
        "volume_z5":      compute_volume_zscore(volume, 5),
        "volume_z20":     compute_volume_zscore(volume, 20),
        "volume_trend":   compute_volume_trend(volume),

        # ── Volatility ────────────────────────────────
        "atr_14":         compute_atr(df_hist, 14),
        "atr_pct":        round(
            compute_atr(df_hist, 14) / close.iloc[-1] * 100, 4
        ) if close.iloc[-1] > 0 else 0.0,
        "bb_position":    compute_bb_position(close),

        # ── Market context ────────────────────────────
        "vix_level":      vix_level,
        "vix_regime":     vix_regime,
        "nifty_trend_5d": nifty_5d,
        "fii_net_3d":     fii_net_3d,

        # ── Return context ────────────────────────────
        "return_5d":      compute_return_nd(close, 5),
        "return_20d":     compute_return_nd(close, 20),
        "drawdown_pct":   compute_drawdown(close, 252),

        # ── Housekeeping ──────────────────────────────
        "feature_version": FEATURE_VERSION,
        "computed_at":     datetime.now(),
        "version":         int(datetime.now().timestamp()),
    }

    return features


# ══════════════════════════════════════════════════════
# Insert helpers
# ══════════════════════════════════════════════════════

FEATURE_COLS = [
    "event_date", "symbol", "market",
    "rsi_14", "rsi_5",
    "sma_20_cross", "sma_50_cross", "ema_20_cross",
    "price_vs_sma20", "price_vs_sma50", "price_vs_sma200",
    "volume_z5", "volume_z20", "volume_trend",
    "atr_14", "atr_pct", "bb_position",
    "vix_level", "vix_regime", "nifty_trend_5d", "fii_net_3d",
    "return_5d", "return_20d", "drawdown_pct",
    "feature_version", "computed_at", "version",
]


def insert_features(ch, rows: list[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)[FEATURE_COLS]
    ch.insert_df("analysis.pattern_features", df)


def mark_events_processed(ch, symbol: str, dates: list[date]):
    """
    Mark detected_events rows as processed = 1 using ALTER UPDATE mutation.
    Batched per symbol for efficiency.
    """
    if not dates:
        return
    date_list = ", ".join(f"'{d}'" for d in dates)
    ch.command(
        f"ALTER TABLE analysis.detected_events UPDATE processed = 1 "
        f"WHERE symbol = '{symbol}' AND date IN ({date_list}) "
        f"SETTINGS mutations_sync = 0"   # async — don't block the pipeline
    )


# ══════════════════════════════════════════════════════
# VIX / regime cache (fetched once, shared read-only)
# ══════════════════════════════════════════════════════

def fetch_vix_cache(ch) -> dict:
    """
    Fetch all VIX + regime data from market.nifty_live as a date → dict map.
    Falls back gracefully if table is empty.
    """
    try:
        result = ch.query(
            "SELECT toDate(timestamp) as d, avg(vix) as avg_vix "
            "FROM market.nifty_live FINAL "
            "GROUP BY d ORDER BY d"
        )
        return {
            row[0]: {"vix_level": float(row[1]) if row[1] else 0.0}
            for row in result.result_rows
        }
    except Exception:
        return {}


def fetch_regime_cache(ch) -> dict:
    """date → vix_regime string from analysis.market_regime."""
    try:
        result = ch.query(
            "SELECT date, vix_regime FROM analysis.market_regime FINAL"
        )
        return {row[0]: row[1] for row in result.result_rows}
    except Exception:
        return {}


def get_vix_for_date(vix_cache: dict, regime_cache: dict,
                     d: date) -> tuple[float, str]:
    vix    = vix_cache.get(d, {}).get("vix_level", 0.0)
    regime = regime_cache.get(d, "")

    # Derive regime from VIX level if not stored
    if not regime and vix > 0:
        if vix < 13:
            regime = "low"
        elif vix <= 18:
            regime = "normal"
        elif vix <= 25:
            regime = "elevated"
        else:
            regime = "extreme"

    return vix, regime


# ══════════════════════════════════════════════════════
# Per-symbol worker
# ══════════════════════════════════════════════════════

def process_symbol_events(symbol: str,
                          market: str,
                          event_dates: list[date],
                          vix_cache: dict,
                          regime_cache: dict,
                          nifty_5d_cache: dict,
                          fii_net_cache: dict,
                          idx: int,
                          total: int):
    ch = get_ch_client()
    feature_rows  = []
    processed_dates = []

    try:
        # Fetch one contiguous OHLCV block for this symbol
        # wide enough to cover all event dates + their lookback windows
        earliest_event = min(event_dates)
        df_ohlcv = fetch_ohlcv_history(ch, symbol, market, earliest_event)

        if df_ohlcv.empty:
            with results_lock:
                results["skipped"] += len(event_dates)
            return

        for event_date in event_dates:
            # Slice history up to event_date - 1 only (no lookahead)
            cutoff   = event_date - timedelta(days=1)
            df_slice = df_ohlcv[df_ohlcv["date"] <= cutoff].copy()

            if len(df_slice) < MIN_HISTORY_ROWS:
                with results_lock:
                    results["skipped"] += 1
                continue

            vix_level, vix_regime = get_vix_for_date(
                vix_cache, regime_cache, event_date
            )
            nifty_5d  = nifty_5d_cache.get(event_date, 0.0)
            fii_net3d = fii_net_cache.get(event_date, 0.0)

            feat = compute_features(
                df_slice, symbol, market, event_date,
                vix_level, vix_regime, nifty_5d, fii_net3d
            )

            if feat is None:
                with results_lock:
                    results["skipped"] += 1
                continue

            feature_rows.append(feat)
            processed_dates.append(event_date)

        if feature_rows:
            insert_features(ch, feature_rows)
            mark_events_processed(ch, symbol, processed_dates)

        with results_lock:
            results["processed"] += len(feature_rows)
            results["skipped"]   += len(event_dates) - len(feature_rows)
            if results["processed"] % BATCH_SIZE == 0:
                log.info(
                    f"  Progress {idx}/{total} symbols | "
                    f"✅ {results['processed']} features | "
                    f"⏭️  {results['skipped']} skipped | "
                    f"❌ {len(results['failed'])} failed"
                )

    except Exception as e:
        log.error(f"  FAILED [{symbol}]: {e}")
        with results_lock:
            results["failed"].append(f"{symbol} → {e}")


# ══════════════════════════════════════════════════════
# Nifty 5d + FII cache builders
# ══════════════════════════════════════════════════════

def build_nifty_5d_cache(ch, all_dates: list[date]) -> dict:
    """
    Batch-compute Nifty 5d return for all unique event dates.
    One query, O(1) lookups later.
    """
    if not all_dates:
        return {}

    fetch_from = min(all_dates) - timedelta(days=20)
    fetch_to   = max(all_dates)

    try:
        result = ch.query(
            "SELECT date, close FROM market.ohlcv_daily FINAL "
            "WHERE symbol = 'NIFTYBEES.NS' AND market = 'indian' "
            "  AND date >= {d_from:Date} AND date <= {d_to:Date} "
            "ORDER BY date",
            parameters={"d_from": fetch_from, "d_to": fetch_to}
        )
        if not result.result_rows:
            return {}

        nifty = pd.DataFrame(result.result_rows, columns=["date", "close"])
        nifty["date"] = pd.to_datetime(nifty["date"]).dt.date
        nifty = nifty.sort_values("date").reset_index(drop=True)

        cache = {}
        for d in all_dates:
            # Get last 6 rows up to (d - 1)
            sub = nifty[nifty["date"] < d].tail(6)
            if len(sub) < 2:
                cache[d] = 0.0
            else:
                oldest = sub["close"].iloc[0]
                latest = sub["close"].iloc[-1]
                cache[d] = round((latest - oldest) / oldest * 100, 4) \
                           if oldest > 0 else 0.0
        return cache
    except Exception as e:
        log.warning(f"Could not build Nifty 5d cache: {e}")
        return {}


def build_fii_net_cache(ch, all_dates: list[date]) -> dict:
    """
    Batch-compute FII net 3-day sum for all unique event dates.
    Returns {date: fii_net_3d_sum}.
    """
    if not all_dates:
        return {}

    fetch_from = min(all_dates) - timedelta(days=10)
    fetch_to   = max(all_dates)

    try:
        result = ch.query(
            "SELECT date, net_value FROM market.fii_dii FINAL "
            "WHERE entity = 'FII' "
            "  AND date >= {d_from:Date} AND date <= {d_to:Date} "
            "ORDER BY date",
            parameters={"d_from": fetch_from, "d_to": fetch_to}
        )
        if not result.result_rows:
            return {d: 0.0 for d in all_dates}

        fii = pd.DataFrame(result.result_rows, columns=["date", "net_value"])
        fii["date"] = pd.to_datetime(fii["date"]).dt.date
        fii = fii.sort_values("date").reset_index(drop=True)

        cache = {}
        for d in all_dates:
            sub = fii[fii["date"] < d].tail(3)
            cache[d] = round(float(sub["net_value"].sum()), 2) \
                       if not sub.empty else 0.0
        return cache
    except Exception as e:
        log.warning(f"Could not build FII cache: {e}")
        return {d: 0.0 for d in all_dates}


# ══════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════

def print_summary():
    print("\n" + "=" * 60)
    print("PATTERN FEATURE EXTRACTOR — SUMMARY")
    print("=" * 60)
    print(f"✅ Features computed : {results['processed']}")
    print(f"⏭️  Skipped           : {results['skipped']} (insufficient history)")
    print(f"❌ Failed            : {len(results['failed'])}")
    if results["failed"]:
        for f in results["failed"][:20]:
            print(f"   {f}")
        if len(results["failed"]) > 20:
            print(f"   ... and {len(results['failed']) - 20} more")
    print("=" * 60)


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Pattern Feature Extractor — builds feature vectors for detected events"
    )
    parser.add_argument("--full",   action="store_true",
                        help="Recompute features for ALL events (ignore processed flag)")
    parser.add_argument("--symbol", type=str, default=None,
                        help="Process events for a single symbol")
    parser.add_argument("--market", type=str, default=None,
                        help="Process events for a single market")
    args = parser.parse_args()

    log.info("=== Pattern Feature Extractor Starting ===")
    log.info(f"Mode    : {'FULL RECOMPUTE' if args.full else 'INCREMENTAL'}")
    log.info(f"Started : {datetime.now()}")

    ch = get_ch_client()
    log.info("ClickHouse connected ✅")

    # Fetch all unprocessed events
    log.info("Fetching unprocessed events from analysis.detected_events...")
    df_events = fetch_unprocessed_events(
        ch,
        symbol=args.symbol,
        market=args.market,
        full=args.full
    )

    if df_events.empty:
        log.info("No unprocessed events found. Nothing to do.")
        return

    log.info(f"Events to process : {len(df_events)}")

    # Build shared context caches (one query each, shared read-only)
    all_dates = sorted(df_events["date"].unique().tolist())
    log.info(f"Date range        : {all_dates[0]} → {all_dates[-1]}")

    log.info("Building VIX cache...")
    vix_cache    = fetch_vix_cache(ch)
    regime_cache = fetch_regime_cache(ch)
    log.info(f"VIX cache         : {len(vix_cache)} dates")

    log.info("Building Nifty 5d return cache...")
    nifty_5d_cache = build_nifty_5d_cache(ch, all_dates)
    log.info(f"Nifty 5d cache    : {len(nifty_5d_cache)} dates")

    log.info("Building FII net cache...")
    fii_net_cache = build_fii_net_cache(ch, all_dates)
    log.info(f"FII net cache     : {len(fii_net_cache)} dates")

    # Group events by symbol for efficient OHLCV fetching
    # (one OHLCV fetch per symbol, covers all its event dates)
    symbol_groups = {}
    for _, row in df_events.iterrows():
        key = (row["symbol"], row["market"])
        symbol_groups.setdefault(key, []).append(row["date"])

    total = len(symbol_groups)
    log.info(f"Symbols to process: {total}")
    log.info(f"Workers           : {MAX_WORKERS}")

    def _worker(item):
        idx, ((symbol, market), event_dates) = item
        process_symbol_events(
            symbol, market, event_dates,
            vix_cache, regime_cache,
            nifty_5d_cache, fii_net_cache,
            idx, total
        )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(_worker, enumerate(symbol_groups.items(), 1))

    print_summary()
    log.info(f"Finished : {datetime.now()}")


if __name__ == "__main__":
    main()
