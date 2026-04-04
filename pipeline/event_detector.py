#!/usr/bin/env python3
"""
event_detector.py
─────────────────
Scans market.ohlcv_daily for single-day price moves >= EVENT_THRESHOLD_PCT
and writes them to analysis.detected_events.

What counts as an event:
  • |pct_change| >= EVENT_THRESHOLD_PCT  (default 2.0%)
  • pct_change = (close - prev_close) / prev_close * 100

Event classification:
  gap_up        — open >= prev_close * GAP_PCT and close > prev_close
  gap_down      — open <= prev_close * (2 - GAP_PCT) and close < prev_close
  intraday_surge  — close up >= threshold but open was within GAP_PCT of prev_close
  intraday_crash  — close down >= threshold but open was within GAP_PCT of prev_close

VIX enrichment:
  Joins with analysis.market_regime (if populated) on the event date.
  Falls back to 0 / '' if market_regime table has no data for that date.

Incremental logic:
  • Fetches max(date) per symbol from analysis.detected_events in ONE query
  • On first run (no watermark): scans last LOOKBACK_DAYS of history
  • On subsequent runs: scans only rows after last detected date
  • Uses FINAL on ohlcv_daily reads for strict dedup

Usage:
  python event_detector.py                  # incremental (default)
  python event_detector.py --full           # reprocess all history
  python event_detector.py --symbol RELIANCE.NS  # single symbol debug
  python event_detector.py --threshold 3.0  # custom threshold
  python event_detector.py --lookback 365   # override lookback window
"""

import os
import sys
import logging
import argparse
import threading
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor

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

EVENT_THRESHOLD_PCT = 2.0    # minimum |pct_change| to qualify as an event
GAP_OPEN_PCT        = 1.005  # multiplier: open >= prev_close * 1.005 = gap up
LOOKBACK_DAYS       = 730    # days of history to scan on first run (~2 years)
MAX_WORKERS         = 8      # parallel threads
BATCH_SIZE          = 50     # log progress every N symbols

# ── Results tracker ────────────────────────────────────
results = {
    "events_found":  0,
    "symbols_scanned": 0,
    "skipped":       0,
    "failed":        [],
}
results_lock = threading.Lock()


# ── ClickHouse client ──────────────────────────────────
def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ══════════════════════════════════════════════════════
# Preflight
# ══════════════════════════════════════════════════════

def assert_tables_exist(ch):
    for table in ["market.ohlcv_daily", "analysis.detected_events"]:
        db, tbl = table.split(".")
        count = ch.query(
            "SELECT count() FROM system.tables "
            "WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": db, "tbl": tbl}
        ).result_rows[0][0]
        if not count:
            raise RuntimeError(
                f"Table {table} missing — run migrations first: "
                f"docker compose run --rm migrate"
            )
    log.info("Tables verified ✅")


# ══════════════════════════════════════════════════════
# Watermark fetch
# ══════════════════════════════════════════════════════

def fetch_all_watermarks(ch) -> dict:
    """
    Max(date) per symbol already in analysis.detected_events.
    Returns {symbol: last_date}. One query, no N+1.
    """
    result = ch.query(
        "SELECT symbol, max(date) "
        "FROM analysis.detected_events FINAL "
        "GROUP BY symbol"
    )
    return {row[0]: row[1] for row in result.result_rows}


def fetch_all_symbols(ch) -> list[tuple[str, str]]:
    """All distinct (symbol, market) pairs from ohlcv_daily."""
    result = ch.query(
        "SELECT DISTINCT symbol, market "
        "FROM market.ohlcv_daily FINAL "
        "ORDER BY market, symbol"
    )
    return [(row[0], row[1]) for row in result.result_rows]


# ══════════════════════════════════════════════════════
# VIX / regime enrichment
# ══════════════════════════════════════════════════════

def fetch_regime_map(ch, from_date: date) -> dict:
    """
    Fetch vix + regime for all dates >= from_date from analysis.market_regime.
    Returns {date: (vix_level, regime_string)}.
    Falls back gracefully if table is empty.
    """
    try:
        result = ch.query(
            "SELECT date, vix_regime "
            "FROM analysis.market_regime FINAL "
            "WHERE date >= {d:Date} "
            "ORDER BY date",
            parameters={"d": from_date}
        )
        return {row[0]: row[1] for row in result.result_rows}
    except Exception as e:
        log.warning(f"Could not fetch market_regime (table may be empty): {e}")
        return {}


# ══════════════════════════════════════════════════════
# Event classification
# ══════════════════════════════════════════════════════

def classify_event(row: pd.Series) -> str:
    """
    Classify the type of move based on open vs prev_close and close vs prev_close.

    gap_up         — opened significantly higher AND closed higher
    gap_down       — opened significantly lower AND closed lower
    intraday_surge — opened near prev_close but rallied to close up
    intraday_crash — opened near prev_close but sold off to close down
    """
    open_      = row["open"]
    close      = row["close"]
    prev_close = row["prev_close"]

    if prev_close <= 0:
        return "unknown"

    open_ratio = open_ / prev_close
    is_up      = close > prev_close

    if is_up:
        if open_ratio >= GAP_OPEN_PCT:
            return "gap_up"
        return "intraday_surge"
    else:
        if open_ratio <= (2 - GAP_OPEN_PCT):   # e.g. <= 0.995
            return "gap_down"
        return "intraday_crash"


# ══════════════════════════════════════════════════════
# Core detection per symbol
# ══════════════════════════════════════════════════════

def fetch_ohlcv_for_symbol(ch, symbol: str, market: str,
                           from_date: date) -> pd.DataFrame:
    """
    Fetch OHLCV for one symbol from from_date - 1 (need prev_close for the
    first row in window) through today.
    """
    # Pull one extra day before from_date so we can compute prev_close for it
    fetch_from = from_date - timedelta(days=5)   # buffer for weekends/holidays

    result = ch.query(
        "SELECT date, open, high, low, close, volume "
        "FROM market.ohlcv_daily FINAL "
        "WHERE symbol = {sym:String} AND market = {mkt:String} "
        "  AND date >= {d:Date} "
        "ORDER BY date",
        parameters={"sym": symbol, "mkt": market, "d": fetch_from}
    )

    if not result.result_rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        result.result_rows,
        columns=["date", "open", "high", "low", "close", "volume"]
    )
    df["date"]   = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = symbol
    df["market"] = market
    return df


def detect_events_for_symbol(df: pd.DataFrame,
                              from_date: date,
                              threshold_pct: float,
                              regime_map: dict) -> pd.DataFrame:
    """
    Given OHLCV for one symbol, compute pct_change vs prev_close
    and return rows where |pct_change| >= threshold_pct.

    from_date: only return events on or after this date
               (earlier rows were fetched only to compute prev_close)
    """
    if df.empty or len(df) < 2:
        return pd.DataFrame()

    df = df.sort_values("date").reset_index(drop=True)

    # prev_close = previous row's close
    df["prev_close"] = df["close"].shift(1)
    df = df.dropna(subset=["prev_close"])
    df = df[df["prev_close"] > 0]

    # pct_change
    df["pct_change"] = (df["close"] - df["prev_close"]) / df["prev_close"] * 100

    # Filter to threshold
    events = df[df["pct_change"].abs() >= threshold_pct].copy()

    # Filter to from_date window (discard warm-up rows)
    events = events[events["date"] >= from_date]

    if events.empty:
        return pd.DataFrame()

    # Classify
    events["direction"]  = events["pct_change"].apply(
        lambda x: "UP" if x > 0 else "DOWN"
    )
    events["event_type"] = events.apply(classify_event, axis=1)
    events["threshold_pct"] = threshold_pct

    # VIX / regime enrichment
    events["market_regime"] = events["date"].map(
        lambda d: regime_map.get(d, "")
    )
    events["vix_on_day"] = 0.0    # populated from nifty_live in a future pass

    # version for ReplacingMergeTree
    events["version"] = int(datetime.now().timestamp())
    events["processed"] = 0

    return events[[
        "date", "symbol", "market",
        "open", "high", "low", "close", "volume",
        "prev_close", "pct_change", "direction", "event_type",
        "threshold_pct", "vix_on_day", "market_regime",
        "processed", "version"
    ]]


# ══════════════════════════════════════════════════════
# Insert
# ══════════════════════════════════════════════════════

def insert_events(ch, df: pd.DataFrame):
    """Insert detected events into analysis.detected_events."""
    if df.empty:
        return
    ch.insert_df("analysis.detected_events", df)


# ══════════════════════════════════════════════════════
# Per-symbol worker
# ══════════════════════════════════════════════════════

def process_symbol(symbol: str, market: str,
                   last_date,
                   threshold_pct: float,
                   regime_map: dict,
                   full_recompute: bool,
                   idx: int, total: int):
    ch = get_ch_client()

    try:
        # Determine scan start date
        if full_recompute or last_date is None:
            from_date = date.today() - timedelta(days=LOOKBACK_DAYS)
        else:
            from_date = last_date + timedelta(days=1)

        if from_date > date.today():
            with results_lock:
                results["skipped"] += 1
            return

        # Fetch OHLCV
        df_raw = fetch_ohlcv_for_symbol(ch, symbol, market, from_date)

        if df_raw.empty:
            with results_lock:
                results["skipped"] += 1
            return

        # Detect events
        df_events = detect_events_for_symbol(
            df_raw, from_date, threshold_pct, regime_map
        )

        if df_events.empty:
            with results_lock:
                results["skipped"] += 1
            return

        # Insert
        insert_events(ch, df_events)

        with results_lock:
            results["events_found"]    += len(df_events)
            results["symbols_scanned"] += 1
            if idx % BATCH_SIZE == 0:
                log.info(
                    f"  Progress {idx}/{total} | "
                    f"📊 {results['symbols_scanned']} symbols | "
                    f"🎯 {results['events_found']} events | "
                    f"❌ {len(results['failed'])} failed"
                )

    except Exception as e:
        log.error(f"  FAILED [{symbol}]: {e}")
        with results_lock:
            results["failed"].append(f"{symbol} → {e}")


# ══════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════

def print_summary(threshold_pct: float):
    print("\n" + "=" * 60)
    print("EVENT DETECTOR — SUMMARY")
    print("=" * 60)
    print(f"Threshold        : {threshold_pct}%")
    print(f"📊 Symbols scanned : {results['symbols_scanned']}")
    print(f"🎯 Events found    : {results['events_found']}")
    print(f"⏭️  Skipped         : {results['skipped']} (up to date or no data)")
    print(f"❌ Failed          : {len(results['failed'])}")
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
    parser = argparse.ArgumentParser(description="Event Detector — finds ≥N% moves")
    parser.add_argument("--full",      action="store_true",
                        help="Reprocess full LOOKBACK_DAYS history for all symbols")
    parser.add_argument("--symbol",    type=str, default=None,
                        help="Process a single symbol (debug)")
    parser.add_argument("--threshold", type=float, default=EVENT_THRESHOLD_PCT,
                        help=f"Event threshold %% (default: {EVENT_THRESHOLD_PCT})")
    parser.add_argument("--lookback",  type=int, default=LOOKBACK_DAYS,
                        help=f"Days of history on first run (default: {LOOKBACK_DAYS})")
    args = parser.parse_args()

    threshold_pct = args.threshold
    lookback      = args.lookback

    log.info("=== Event Detector Starting ===")
    log.info(f"Mode      : {'FULL RECOMPUTE' if args.full else 'INCREMENTAL'}")
    log.info(f"Threshold : {threshold_pct}%")
    log.info(f"Started   : {datetime.now()}")

    ch = get_ch_client()
    log.info("ClickHouse connected ✅")

    assert_tables_exist(ch)

    # Fetch regime map once — shared read-only across threads
    regime_from = date.today() - timedelta(days=lookback)
    log.info("Fetching market regime data...")
    regime_map = fetch_regime_map(ch, regime_from)
    log.info(f"Regime data: {len(regime_map)} dates loaded")

    if args.symbol:
        # ── Single symbol debug mode ───────────────────
        log.info(f"Processing single symbol: {args.symbol}")
        watermarks = fetch_all_watermarks(ch)
        last_date  = None if args.full else watermarks.get(args.symbol)

        # Find this symbol's market
        result = ch.query(
            "SELECT DISTINCT market FROM market.ohlcv_daily FINAL "
            "WHERE symbol = {sym:String} LIMIT 1",
            parameters={"sym": args.symbol}
        )
        if not result.result_rows:
            log.error(f"Symbol {args.symbol} not found in ohlcv_daily")
            sys.exit(1)
        market = result.result_rows[0][0]

        process_symbol(
            args.symbol, market, last_date,
            threshold_pct, regime_map, args.full,
            idx=1, total=1
        )

    else:
        # ── All symbols — parallel ─────────────────────
        log.info("Fetching all symbols from ohlcv_daily...")
        all_symbols = fetch_all_symbols(ch)
        total       = len(all_symbols)
        log.info(f"Symbols to process : {total}")

        if args.full:
            log.warning("Full recompute — scanning entire lookback window.")
            watermarks = {}
        else:
            log.info("Fetching watermarks from detected_events...")
            watermarks = fetch_all_watermarks(ch)
            log.info(f"Got watermarks for {len(watermarks)} already-processed symbols")

        def _worker(item):
            idx, (symbol, market) = item
            last_date = watermarks.get(symbol)
            process_symbol(
                symbol, market, last_date,
                threshold_pct, regime_map, args.full,
                idx=idx, total=total
            )

        log.info(f"Running with {MAX_WORKERS} parallel workers...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(_worker, enumerate(all_symbols, 1))

    print_summary(threshold_pct)
    log.info(f"Finished : {datetime.now()}")


if __name__ == "__main__":
    main()
