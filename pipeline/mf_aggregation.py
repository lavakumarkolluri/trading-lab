#!/usr/bin/env python3
"""
mf_aggregation.py
─────────────────
Reads raw NAV data from market.mf_nav, computes enriched aggregations
per scheme, and inserts into market.mf_nav_enriched.

Aggregations computed per row (per scheme_code × date):
  • Weekly rollup  — week_open, week_high, week_low, week_close, week_return_pct
  • Monthly rollup — month_open, month_high, month_low, month_close, month_return_pct
  • SMA            — 20, 50, 200 day Simple Moving Averages on NAV
  • EMA            — 20, 50 day Exponential Moving Averages (Wilder's, adjust=False)
  • RSI-14         — Relative Strength Index (Wilder's smoothing)
  • ATR-14 proxy   — 14-day rolling mean of |Δ NAV| (volatility proxy, no OHLC for MF)
  • 52-week range  — trailing 252-day high / low
  • YTD return     — % change from first NAV of the current calendar year
  • All-time high  — cumulative max NAV from inception
  • Drawdown %     — % below all-time high

Incremental logic:
  • For each scheme, finds max(date) in mf_nav_enriched
  • Re-computes only from (max_date - 290 days) to allow rolling window warm-up
  • On first run, processes full history
  • No explicit deletes — ReplacingMergeTree(version) handles dedup via version
    column. Always use SELECT ... FINAL on mf_nav_enriched for correct reads.

Usage:
  python mf_aggregation.py                  # incremental (default)
  python mf_aggregation.py --full           # recompute all schemes from scratch
  python mf_aggregation.py --scheme 119598  # single scheme (debug)
"""

import os
import sys
import logging
import argparse
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timedelta

import pandas as pd
import numpy as np
import clickhouse_connect

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────
CH_HOST    = os.getenv("CH_HOST", "clickhouse")
CH_PORT    = int(os.getenv("CH_PORT", "8123"))
CH_USER    = os.getenv("CH_USER", "default")
CH_PASS    = os.getenv("CH_PASSWORD", "")

BATCH_SIZE        = 200          # schemes per batch log
INSERT_CHUNK_SIZE = 50_000       # rows per ClickHouse insert
WARMUP_DAYS       = 290          # extra lookback for SMA-200 warm-up
MAX_WORKERS       = 8            # parallel threads

# ── Results tracker (thread-safe) ──────────────────────
results      = {"success": 0, "skipped": 0, "failed": []}
results_lock = threading.Lock()


# ══════════════════════════════════════════════════════
# ClickHouse helpers
# ══════════════════════════════════════════════════════

def get_ch_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


def fetch_all_scheme_codes(ch) -> list[int]:
    """Return list of all scheme_codes present in market.mf_nav."""
    result = ch.query(
        "SELECT DISTINCT scheme_code FROM market.mf_nav ORDER BY scheme_code"
    )
    return [row[0] for row in result.result_rows]


def fetch_last_enriched_date(ch, scheme_code: int):
    """Return max(date) from mf_nav_enriched for the given scheme, or None."""
    result = ch.query(
        "SELECT max(date) FROM market.mf_nav_enriched "
        "WHERE scheme_code = {code:UInt32}",
        parameters={"code": scheme_code}
    )
    val = result.result_rows[0][0]
    return val  # datetime.date or None

def fetch_all_last_enriched_dates(ch) -> dict:
    """Fetch max(date) for ALL schemes in ONE query. Returns {scheme_code: last_date}."""
    result = ch.query(
        "SELECT scheme_code, max(date) "
        "FROM market.mf_nav_enriched "
        "GROUP BY scheme_code"
    )
    return {row[0]: row[1] for row in result.result_rows}

def fetch_nav_for_scheme(ch, scheme_code: int,
                         from_date=None) -> pd.DataFrame:
    """
    Fetch NAV rows for a single scheme from market.mf_nav.
    If from_date provided, fetches from (from_date - WARMUP_DAYS) for
    rolling-window warm-up, but the caller trims before insert.
    """
    if from_date is not None:
        warmup_start = from_date - timedelta(days=WARMUP_DAYS)
        result = ch.query(
            "SELECT date, nav FROM market.mf_nav "
            "WHERE scheme_code = {code:UInt32} AND date >= {d:Date} "
            "ORDER BY date",
            parameters={"code": scheme_code, "d": warmup_start}
        )
    else:
        result = ch.query(
            "SELECT date, nav FROM market.mf_nav "
            "WHERE scheme_code = {code:UInt32} "
            "ORDER BY date",
            parameters={"code": scheme_code}
        )

    if not result.result_rows:
        return pd.DataFrame(columns=["date", "nav"])

    df = pd.DataFrame(result.result_rows, columns=["date", "nav"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["nav"]  = df["nav"].astype(float)
    return df


def insert_enriched(ch, df: pd.DataFrame):
    """Insert enriched DataFrame into ClickHouse in chunks."""
    for start in range(0, len(df), INSERT_CHUNK_SIZE):
        chunk = df.iloc[start:start + INSERT_CHUNK_SIZE]
        ch.insert_df("market.mf_nav_enriched", chunk)


# ══════════════════════════════════════════════════════
# Computation helpers
# ══════════════════════════════════════════════════════

def compute_sma(series: pd.Series, window: int) -> pd.Series:
    """Simple Moving Average.  NaN until `window` rows available."""
    return series.rolling(window=window, min_periods=window).mean()


def compute_ema(series: pd.Series, span: int) -> pd.Series:
    """
    Exponential Moving Average — Wilder's method (adjust=False).
    Equivalent to alpha = 2/(span+1) applied iteratively.
    """
    return series.ewm(span=span, adjust=False, min_periods=span).mean()


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    Wilder's RSI.
    delta    = daily change in NAV
    gain/loss separated, then smoothed with Wilder's EMA (alpha=1/period)
    RSI = 100 - 100/(1 + avg_gain/avg_loss)
    """
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False,
                        min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False,
                        min_periods=period).mean()

    rs  = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0)


def compute_atr_proxy(series: pd.Series, period: int = 14) -> pd.Series:
    """
    ATR proxy for MF (no intraday high/low available).
    Defined as: 14-day rolling mean of |daily NAV change|
    Represents average absolute rupee volatility per day.
    """
    abs_change = series.diff().abs()
    return abs_change.rolling(window=period, min_periods=period).mean().fillna(0)


def compute_weekly_rollup(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_date_ts"]   = pd.to_datetime(df["date"])
    df["week_start"] = df["_date_ts"].dt.to_period("W").apply(
        lambda p: p.start_time.date()
    )

    weekly = df.groupby("week_start")["nav"].agg(
        week_open  = "first",
        week_high  = "max",
        week_low   = "min",
        week_close = "last"
    ).reset_index()

    weekly["week_return_pct"] = (
        (weekly["week_close"] - weekly["week_open"])
        / weekly["week_open"].replace(0, np.nan) * 100
    ).fillna(0)

    df = df.merge(weekly, on="week_start", how="left")
    df.drop(columns=["_date_ts"], inplace=True)
    return df


def compute_monthly_rollup(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_date_ts"]    = pd.to_datetime(df["date"])
    df["month_start"] = df["_date_ts"].dt.to_period("M").apply(
        lambda p: p.start_time.date()
    )

    monthly = df.groupby("month_start")["nav"].agg(
        month_open  = "first",
        month_high  = "max",
        month_low   = "min",
        month_close = "last"
    ).reset_index()

    monthly["month_return_pct"] = (
        (monthly["month_close"] - monthly["month_open"])
        / monthly["month_open"].replace(0, np.nan) * 100
    ).fillna(0)

    df = df.merge(monthly, on="month_start", how="left")
    df.drop(columns=["_date_ts"], inplace=True)
    return df


def compute_52w_range(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    high = series.rolling(window=252, min_periods=1).max()
    low  = series.rolling(window=252, min_periods=1).min()
    return high, low


def compute_ytd_return(df: pd.DataFrame) -> pd.Series:
    df = df.copy()
    df["_year"] = pd.to_datetime(df["date"]).dt.year
    year_first  = df.groupby("_year")["nav"].transform("first")
    ytd         = (df["nav"] - year_first) / year_first.replace(0, np.nan) * 100
    return ytd.fillna(0)


def compute_all_time_high(series: pd.Series) -> pd.Series:
    return series.cummax()


def compute_drawdown(nav: pd.Series, ath: pd.Series) -> pd.Series:
    dd = (ath - nav) / ath.replace(0, np.nan) * 100
    return dd.fillna(0)


# ══════════════════════════════════════════════════════
# Core enrichment function
# ══════════════════════════════════════════════════════

def enrich_scheme(df_raw: pd.DataFrame, scheme_code: int) -> pd.DataFrame:
    df = df_raw.copy().sort_values("date").reset_index(drop=True)
    df["scheme_code"] = scheme_code

    df["sma_20"]  = compute_sma(df["nav"], 20).fillna(0)
    df["sma_50"]  = compute_sma(df["nav"], 50).fillna(0)
    df["sma_200"] = compute_sma(df["nav"], 200).fillna(0)
    df["ema_20"]  = compute_ema(df["nav"], 20).fillna(0)
    df["ema_50"]  = compute_ema(df["nav"], 50).fillna(0)
    df["rsi_14"]  = compute_rsi(df["nav"], 14)
    df["atr_14"]  = compute_atr_proxy(df["nav"], 14)

    df["high_52w"], df["low_52w"] = compute_52w_range(df["nav"])
    df["ytd_return_pct"] = compute_ytd_return(df)
    df["all_time_high"]  = compute_all_time_high(df["nav"])
    df["drawdown_pct"]   = compute_drawdown(df["nav"], df["all_time_high"])

    df = compute_weekly_rollup(df)
    df = compute_monthly_rollup(df)

    df["computed_at"] = datetime.now()
    df["version"]     = int(datetime.now().timestamp())

    cols = [
        "date", "scheme_code", "nav",
        "week_start", "week_open", "week_high",
        "week_low", "week_close", "week_return_pct",
        "month_start", "month_open", "month_high",
        "month_low", "month_close", "month_return_pct",
        "sma_20", "sma_50", "sma_200",
        "ema_20", "ema_50",
        "rsi_14", "atr_14",
        "high_52w", "low_52w",
        "ytd_return_pct",
        "all_time_high", "drawdown_pct",
        "computed_at", "version"
    ]
    return df[cols]


# ══════════════════════════════════════════════════════
# Per-scheme processing
# ══════════════════════════════════════════════════════

def process_scheme(ch, scheme_code: int,
                   last_date=None,
                   idx: int = 0, total: int = 0):
    try:

        df_raw = fetch_nav_for_scheme(ch, scheme_code, from_date=last_date)

        if df_raw.empty:
            with results_lock:
                results["skipped"] += 1
            return

        df_enriched = enrich_scheme(df_raw, scheme_code)

        if last_date is not None:
            insert_df = df_enriched[df_enriched["date"] > last_date]
            if insert_df.empty:
                with results_lock:
                    results["skipped"] += 1
                return
        else:
            insert_df = df_enriched

        insert_enriched(ch, insert_df)

        with results_lock:
            results["success"] += 1
            if results["success"] % BATCH_SIZE == 0:
                log.info(
                    f"  Progress {idx}/{total} | "
                    f"✅ {results['success']} | "
                    f"⏭️  {results['skipped']} | "
                    f"❌ {len(results['failed'])}"
                )

    except Exception as e:
        log.error(f"  FAILED [{scheme_code}]: {e}")
        with results_lock:
            results["failed"].append(f"{scheme_code} → {e}")


# ══════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════

def print_summary():
    print("\n" + "=" * 60)
    print("MF NAV AGGREGATION PIPELINE — SUMMARY")
    print("=" * 60)
    print(f"✅ Enriched  : {results['success']} schemes")
    print(f"⏭️  Skipped   : {results['skipped']} (already up to date)")
    print(f"❌ Failed    : {len(results['failed'])}")
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
        description="MF NAV Aggregation Pipeline"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Recompute all schemes from scratch"
    )
    parser.add_argument(
        "--scheme",
        type=int,
        default=None,
        help="Process a single scheme_code only (for debugging)"
    )
    args = parser.parse_args()

    log.info("=== MF NAV Aggregation Pipeline Starting ===")
    log.info(f"Mode     : {'FULL RECOMPUTE' if args.full else 'INCREMENTAL'}")
    log.info(f"Started  : {datetime.now()}")

    ch = get_ch_client()
    log.info("ClickHouse connected ✅")

    if args.scheme:
        log.info(f"Processing single scheme: {args.scheme}")
        last_date = None if args.full else fetch_last_enriched_date(ch, args.scheme)
        process_scheme(ch, args.scheme,
                       last_date=last_date,
                       idx=1, total=1)
    else:
        scheme_codes   = fetch_all_scheme_codes(ch)
        total          = len(scheme_codes)
        full_recompute = args.full

        log.info(f"Schemes to process: {total}")
        log.info(f"Workers           : {MAX_WORKERS}")

        if args.full:
            log.warning(
                "Full recompute — this will re-enrich ALL schemes."
            )

        # ── Parallel processing — each thread gets its own CH client ──
        # Pre-fetch all last enriched dates in ONE query — eliminates N+1 round-trips
        last_dates = {} if full_recompute else fetch_all_last_enriched_dates(ch)
        log.info(f"Got last enriched dates for {len(last_dates)} schemes")

        def _worker(item):
            idx, code = item
            ch = get_ch_client()
            process_scheme(ch, code,
                           last_date=last_dates.get(code),
                           idx=idx, total=total)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(_worker, enumerate(scheme_codes, 1))
    print_summary()
    log.info(f"Finished : {datetime.now()}")


if __name__ == "__main__":
    main()