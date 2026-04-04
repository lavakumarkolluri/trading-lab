#!/usr/bin/env python3
"""
backtest_engine.py
──────────────────
Computes forward returns for every row in analysis.pattern_event_map
and writes individual results to analysis.backtests.

After processing all matches, aggregates per-pattern stats
(win_rate, avg_return, Sharpe, max_drawdown) and updates
analysis.patterns.

Forward return definition:
  signal_date = the event date (day the pattern fired, i.e. event_date)
  entry_price = close on signal_date (next-day open simulation is future work)
  return_Nd   = (close[signal_date + N trading days] - entry_price)
                / entry_price * 100

  Because our OHLCV data is calendar-indexed (not trading-day-indexed),
  we look for the Nth available trading day after signal_date, not a
  fixed calendar offset. This avoids weekend/holiday distortion.

  hit_2pct_Nd = 1 if return_Nd >= 2.0 in the direction of the event
                (UP events: return >= +2%, DOWN events: return <= -2%)

  max_adverse_3d = worst close vs entry over days 1..3
                   (proxy for max drawdown during hold period)

Incremental logic:
  • Fetches all (pattern_id, symbol, signal_date) already in backtests
  • Only processes new matches not yet backtested
  • --full flag reprocesses everything
  • One OHLCV fetch per symbol (covers all signal dates for that symbol)

Usage:
  python backtest_engine.py                  # incremental
  python backtest_engine.py --full           # reprocess all matches
  python backtest_engine.py --pattern P001   # single pattern debug
  python backtest_engine.py --symbol RELIANCE.NS
"""

import os
import json
import uuid
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
INSERT_CHUNK_SIZE = 1000
EVENT_THRESHOLD   = 2.0    # % threshold used to classify a return as "hit"
MIN_SAMPLE_SIZE   = 10     # minimum matches to compute meaningful stats

# ── Results tracker ────────────────────────────────────
results = {
    "processed":  0,
    "skipped":    0,
    "failed":     [],
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

def fetch_matches(ch, pattern_id=None, symbol=None) -> pd.DataFrame:
    """
    Fetch all rows from analysis.pattern_event_map.
    Optionally filter by pattern_id or symbol.
    """
    conditions = []
    params     = {}

    if pattern_id:
        conditions.append("pattern_id = {pid:String}")
        params["pid"] = pattern_id
    if symbol:
        conditions.append("symbol = {sym:String}")
        params["sym"] = symbol

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    result = ch.query(
        f"SELECT pattern_id, symbol, market, event_date "
        f"FROM analysis.pattern_event_map FINAL "
        f"{where} "
        f"ORDER BY symbol, event_date",
        parameters=params
    )

    if not result.result_rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        result.result_rows,
        columns=["pattern_id", "symbol", "market", "event_date"]
    )
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
    return df


def fetch_already_backtested(ch) -> set[tuple]:
    """
    Fetch (pattern_id, symbol, signal_date) already in analysis.backtests.
    Used to skip re-processing in incremental mode.
    """
    result = ch.query(
        "SELECT pattern_id, symbol, signal_date "
        "FROM analysis.backtests FINAL"
    )
    return {(row[0], row[1], row[2]) for row in result.result_rows}


def fetch_ohlcv_for_symbol(ch, symbol: str, market: str,
                           from_date: date, to_date: date) -> pd.DataFrame:
    """
    Fetch OHLCV for a symbol covering from_date to to_date + 10 days buffer
    (to ensure forward return windows are available for the latest signals).
    """
    buffer_to = to_date + timedelta(days=15)

    result = ch.query(
        "SELECT date, open, high, low, close, volume "
        "FROM market.ohlcv_daily FINAL "
        "WHERE symbol = {sym:String} AND market = {mkt:String} "
        "  AND date >= {d_from:Date} AND date <= {d_to:Date} "
        "ORDER BY date",
        parameters={
            "sym":    symbol,
            "mkt":    market,
            "d_from": from_date,
            "d_to":   buffer_to,
        }
    )

    if not result.result_rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        result.result_rows,
        columns=["date", "open", "high", "low", "close", "volume"]
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df.sort_values("date").reset_index(drop=True)


def fetch_event_directions(ch) -> dict:
    """
    Fetch direction (UP/DOWN) for all detected events.
    Returns {(symbol, date): direction}
    """
    result = ch.query(
        "SELECT symbol, date, direction "
        "FROM analysis.detected_events FINAL"
    )
    return {(row[0], row[1]): row[2] for row in result.result_rows}


def fetch_feature_snapshots(ch) -> dict:
    """
    Fetch feature snapshots for all (symbol, event_date) pairs.
    Returns {(symbol, event_date): json_string}
    Stored in backtest rows for post-mortem replay.
    """
    result = ch.query(
        "SELECT symbol, event_date, "
        "       rsi_14, volume_z5, price_vs_sma20, "
        "       bb_position, return_5d, vix_level "
        "FROM analysis.pattern_features FINAL"
    )
    snapshots = {}
    for row in result.result_rows:
        key = (row[0], row[1])
        snapshots[key] = json.dumps({
            "rsi_14":        row[2],
            "volume_z5":     row[3],
            "price_vs_sma20": row[4],
            "bb_position":   row[5],
            "return_5d":     row[6],
            "vix_level":     row[7],
        })
    return snapshots


# ══════════════════════════════════════════════════════
# Forward return computation
# ══════════════════════════════════════════════════════

def get_nth_trading_day_close(df_ohlcv: pd.DataFrame,
                               signal_date: date,
                               n: int) -> float | None:
    """
    Find the close price N trading days after signal_date.
    Returns None if not enough data available (near end of dataset).
    """
    future = df_ohlcv[df_ohlcv["date"] > signal_date]
    if len(future) < n:
        return None
    return float(future.iloc[n - 1]["close"])


def compute_forward_returns(df_ohlcv: pd.DataFrame,
                            signal_date: date,
                            entry_price: float,
                            direction: str) -> dict:
    """
    Compute forward returns for N = 1, 3, 5 trading days.
    Also computes max_adverse_3d (worst close vs entry over days 1..3).

    Returns dict with all return fields.
    """
    result = {
        "return_1d":      0.0,
        "return_3d":      0.0,
        "return_5d":      0.0,
        "hit_2pct_1d":    0,
        "hit_2pct_3d":    0,
        "hit_2pct_5d":    0,
        "max_adverse_3d": 0.0,
    }

    if entry_price <= 0:
        return result

    future = df_ohlcv[df_ohlcv["date"] > signal_date].head(5)

    closes = future["close"].tolist()

    def pct(close):
        return (close - entry_price) / entry_price * 100

    # Return at each horizon
    if len(closes) >= 1:
        r1 = pct(closes[0])
        result["return_1d"] = round(r1, 4)
        if direction == "UP":
            result["hit_2pct_1d"] = 1 if r1 >= EVENT_THRESHOLD else 0
        else:
            result["hit_2pct_1d"] = 1 if r1 <= -EVENT_THRESHOLD else 0

    if len(closes) >= 3:
        r3 = pct(closes[2])
        result["return_3d"] = round(r3, 4)
        if direction == "UP":
            result["hit_2pct_3d"] = 1 if r3 >= EVENT_THRESHOLD else 0
        else:
            result["hit_2pct_3d"] = 1 if r3 <= -EVENT_THRESHOLD else 0

        # Max adverse: worst close over days 1..3 vs entry
        adverse = min(pct(c) for c in closes[:3])
        result["max_adverse_3d"] = round(adverse, 4)

    if len(closes) >= 5:
        r5 = pct(closes[4])
        result["return_5d"] = round(r5, 4)
        if direction == "UP":
            result["hit_2pct_5d"] = 1 if r5 >= EVENT_THRESHOLD else 0
        else:
            result["hit_2pct_5d"] = 1 if r5 <= -EVENT_THRESHOLD else 0

    return result


# ══════════════════════════════════════════════════════
# Pattern stats aggregation
# ══════════════════════════════════════════════════════

def compute_pattern_stats(df_bt: pd.DataFrame) -> dict:
    """
    Aggregate backtest rows for one pattern into summary stats.
    df_bt: all backtest rows for this pattern.
    Returns dict matching analysis.patterns columns.
    """
    n = len(df_bt)
    if n == 0:
        return {}

    r1 = df_bt["return_1d"]
    r3 = df_bt["return_3d"]
    r5 = df_bt["return_5d"]

    win_rate_1d = round(float(df_bt["hit_2pct_1d"].mean()) * 100, 2)
    win_rate_3d = round(float(df_bt["hit_2pct_3d"].mean()) * 100, 2)
    win_rate_5d = round(float(df_bt["hit_2pct_5d"].mean()) * 100, 2)

    avg_return_1d = round(float(r1.mean()), 4)
    avg_return_3d = round(float(r3.mean()), 4)
    avg_return_5d = round(float(r5.mean()), 4)

    # Sharpe proxy: annualized (assuming ~252 trading days)
    # Using 1d returns as the base
    std_1d = float(r1.std())
    sharpe_1d = round(
        (float(r1.mean()) / std_1d * (252 ** 0.5)) if std_1d > 0 else 0.0,
        4
    )

    max_drawdown_3d = round(float(df_bt["max_adverse_3d"].min()), 4)

    return {
        "total_matches":   n,
        "win_rate_1d":     win_rate_1d,
        "win_rate_3d":     win_rate_3d,
        "win_rate_5d":     win_rate_5d,
        "avg_return_1d":   avg_return_1d,
        "avg_return_3d":   avg_return_3d,
        "avg_return_5d":   avg_return_5d,
        "sharpe_1d":       sharpe_1d,
        "max_drawdown_3d": max_drawdown_3d,
    }


# ══════════════════════════════════════════════════════
# Insert helpers
# ══════════════════════════════════════════════════════

BACKTEST_COLS = [
    "pattern_id", "symbol", "market", "signal_date",
    "entry_price",
    "return_1d", "return_3d", "return_5d",
    "hit_2pct_1d", "hit_2pct_3d", "hit_2pct_5d",
    "max_adverse_3d",
    "feature_snapshot", "run_id", "computed_at", "version",
]


def insert_backtest_rows(ch, rows: list[dict]):
    if not rows:
        return
    for i in range(0, len(rows), INSERT_CHUNK_SIZE):
        chunk = rows[i:i + INSERT_CHUNK_SIZE]
        df = pd.DataFrame(chunk)[BACKTEST_COLS]
        ch.insert_df("analysis.backtests", df)


def update_pattern_stats(ch, pattern_id: str, stats: dict):
    """
    Update analysis.patterns with aggregated backtest stats.
    Only updates stat columns — not version (ReplacingMergeTree key).
    """
    if not stats:
        return

    ch.command(
        f"ALTER TABLE analysis.patterns UPDATE "
        f"  total_matches   = {stats['total_matches']}, "
        f"  win_rate_1d     = {stats['win_rate_1d']}, "
        f"  win_rate_3d     = {stats['win_rate_3d']}, "
        f"  win_rate_5d     = {stats['win_rate_5d']}, "
        f"  avg_return_1d   = {stats['avg_return_1d']}, "
        f"  avg_return_3d   = {stats['avg_return_3d']}, "
        f"  avg_return_5d   = {stats['avg_return_5d']}, "
        f"  sharpe_1d       = {stats['sharpe_1d']}, "
        f"  max_drawdown_3d = {stats['max_drawdown_3d']}, "
        f"  last_backtested = today() "
        f"WHERE pattern_id = '{pattern_id}' "
        f"SETTINGS mutations_sync = 0"
    )


# ══════════════════════════════════════════════════════
# Per-symbol worker
# ══════════════════════════════════════════════════════

def process_symbol(symbol: str,
                   market: str,
                   matches: pd.DataFrame,
                   already_backtested: set[tuple],
                   direction_map: dict,
                   snapshot_map: dict,
                   run_id: str,
                   full: bool,
                   idx: int,
                   total: int):
    """
    Process all pattern matches for one symbol.
    Fetches OHLCV once, then computes forward returns for each match.
    """
    ch = get_ch_client()

    try:
        # Filter to new matches only (incremental)
        if not full:
            matches = matches[
                ~matches.apply(
                    lambda r: (r["pattern_id"], r["symbol"], r["event_date"])
                    in already_backtested,
                    axis=1
                )
            ]

        if matches.empty:
            with results_lock:
                results["skipped"] += 1
            return

        # Fetch OHLCV covering all signal dates + forward windows
        from_date = matches["event_date"].min()
        to_date   = matches["event_date"].max()

        df_ohlcv = fetch_ohlcv_for_symbol(
            ch, symbol, market, from_date, to_date
        )

        if df_ohlcv.empty:
            with results_lock:
                results["skipped"] += len(matches)
            return

        # Build date → close lookup for fast access
        close_map = dict(zip(df_ohlcv["date"], df_ohlcv["close"]))

        backtest_rows = []
        now_ts        = datetime.now()
        version_ts    = int(now_ts.timestamp())

        for _, match in matches.iterrows():
            signal_date = match["event_date"]
            pattern_id  = match["pattern_id"]

            entry_price = close_map.get(signal_date)
            if entry_price is None or entry_price <= 0:
                continue

            direction = direction_map.get((symbol, signal_date), "UP")
            snapshot  = snapshot_map.get((symbol, signal_date), "{}")

            fwd = compute_forward_returns(
                df_ohlcv, signal_date, entry_price, direction
            )

            backtest_rows.append({
                "pattern_id":       pattern_id,
                "symbol":           symbol,
                "market":           market,
                "signal_date":      signal_date,
                "entry_price":      entry_price,
                "return_1d":        fwd["return_1d"],
                "return_3d":        fwd["return_3d"],
                "return_5d":        fwd["return_5d"],
                "hit_2pct_1d":      fwd["hit_2pct_1d"],
                "hit_2pct_3d":      fwd["hit_2pct_3d"],
                "hit_2pct_5d":      fwd["hit_2pct_5d"],
                "max_adverse_3d":   fwd["max_adverse_3d"],
                "feature_snapshot": snapshot,
                "run_id":           run_id,
                "computed_at":      now_ts,
                "version":          version_ts,
            })

        if backtest_rows:
            insert_backtest_rows(ch, backtest_rows)

        with results_lock:
            results["processed"] += len(backtest_rows)
            if results["processed"] % 1000 == 0:
                log.info(
                    f"  Progress {idx}/{total} symbols | "
                    f"✅ {results['processed']} backtests | "
                    f"⏭️  {results['skipped']} skipped | "
                    f"❌ {len(results['failed'])} failed"
                )

    except Exception as e:
        log.error(f"  FAILED [{symbol}]: {e}")
        with results_lock:
            results["failed"].append(f"{symbol} → {e}")


# ══════════════════════════════════════════════════════
# Summary printer
# ══════════════════════════════════════════════════════

def print_summary():
    print("\n" + "=" * 60)
    print("BACKTEST ENGINE — SUMMARY")
    print("=" * 60)
    print(f"✅ Backtests computed : {results['processed']:,}")
    print(f"⏭️  Skipped            : {results['skipped']:,}")
    print(f"❌ Failed             : {len(results['failed'])}")
    if results["failed"]:
        for f in results["failed"][:10]:
            print(f"   {f}")
    print("=" * 60)


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Backtest Engine — computes forward returns for pattern matches"
    )
    parser.add_argument("--full",    action="store_true",
                        help="Reprocess all matches (ignore already-backtested)")
    parser.add_argument("--pattern", type=str, default=None,
                        help="Debug: process a single pattern_id")
    parser.add_argument("--symbol",  type=str, default=None,
                        help="Debug: process a single symbol")
    args = parser.parse_args()

    log.info("=== Backtest Engine Starting ===")
    log.info(f"Mode    : {'FULL' if args.full else 'INCREMENTAL'}")
    log.info(f"Started : {datetime.now()}")

    ch      = get_ch_client()
    run_id  = str(uuid.uuid4())[:8]
    log.info(f"Run ID  : {run_id}")
    log.info("ClickHouse connected ✅")

    # ── Load matches ───────────────────────────────────
    log.info("Fetching pattern matches from pattern_event_map...")
    df_matches = fetch_matches(ch, pattern_id=args.pattern, symbol=args.symbol)

    if df_matches.empty:
        log.warning("No matches found. Run pattern_builder first.")
        return

    log.info(f"Total matches to process: {len(df_matches):,}")

    # ── Incremental dedup ──────────────────────────────
    if not args.full:
        log.info("Fetching already-backtested entries...")
        already_backtested = fetch_already_backtested(ch)
        log.info(f"Already backtested: {len(already_backtested):,}")
    else:
        log.warning("Full mode — reprocessing ALL matches.")
        already_backtested = set()

    # ── Load shared lookup tables (read-only, one fetch) ─
    log.info("Loading event directions...")
    direction_map = fetch_event_directions(ch)
    log.info(f"Direction map: {len(direction_map):,} entries")

    log.info("Loading feature snapshots...")
    snapshot_map = fetch_feature_snapshots(ch)
    log.info(f"Snapshot map: {len(snapshot_map):,} entries")

    # ── Group by symbol for efficient OHLCV fetch ──────
    symbol_groups = {}
    for _, row in df_matches.iterrows():
        key = (row["symbol"], row["market"])
        symbol_groups.setdefault(key, []).append(row)

    total = len(symbol_groups)
    log.info(f"Symbols to process: {total:,}")
    log.info(f"Workers           : {MAX_WORKERS}")

    def _worker(item):
        idx, ((symbol, market), rows) = item
        df_sym = pd.DataFrame(rows)
        process_symbol(
            symbol, market, df_sym,
            already_backtested, direction_map, snapshot_map,
            run_id, args.full, idx, total
        )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(_worker, enumerate(symbol_groups.items(), 1))

    print_summary()

    # ── Aggregate stats per pattern ────────────────────
    log.info("\nAggregating stats per pattern...")
    active_patterns = ch.query(
        "SELECT DISTINCT pattern_id FROM analysis.pattern_event_map FINAL"
    ).result_rows

    for (pid,) in active_patterns:
        result = ch.query(
            "SELECT pattern_id, symbol, signal_date, "
            "       return_1d, return_3d, return_5d, "
            "       hit_2pct_1d, hit_2pct_3d, hit_2pct_5d, "
            "       max_adverse_3d "
            "FROM analysis.backtests FINAL "
            "WHERE pattern_id = {pid:String}",
            parameters={"pid": pid}
        )

        if not result.result_rows:
            continue

        df_bt = pd.DataFrame(result.result_rows, columns=[
            "pattern_id", "symbol", "signal_date",
            "return_1d", "return_3d", "return_5d",
            "hit_2pct_1d", "hit_2pct_3d", "hit_2pct_5d",
            "max_adverse_3d"
        ])

        stats = compute_pattern_stats(df_bt)
        if stats.get("total_matches", 0) >= MIN_SAMPLE_SIZE:
            update_pattern_stats(ch, pid, stats)
            log.info(
                f"  [{pid}] n={stats['total_matches']:,} | "
                f"win_1d={stats['win_rate_1d']}% | "
                f"avg_ret_1d={stats['avg_return_1d']}% | "
                f"sharpe={stats['sharpe_1d']}"
            )
        else:
            log.warning(
                f"  [{pid}] only {stats.get('total_matches',0)} samples "
                f"— below MIN_SAMPLE_SIZE ({MIN_SAMPLE_SIZE}), skipping stat update"
            )

    log.info(f"\nFinished : {datetime.now()}")


if __name__ == "__main__":
    main()
