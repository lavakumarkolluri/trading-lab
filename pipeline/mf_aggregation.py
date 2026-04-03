#!/usr/bin/env python3
"""
mf_aggregation.py
─────────────────
Orchestrator for MF NAV enrichment pipeline.

Reads raw NAV from market.mf_nav, computes ALL enrichment columns,
and inserts into market.mf_nav_enriched.

Module structure:
  mf_db.py                — ClickHouse queries
  mf_compute_technical.py — SMA, EMA, RSI, ATR, rollups, ATH, drawdown
  mf_compute_returns.py   — return%, CAGR, SIP XIRR
  mf_compute_risk.py      — volatility, Sharpe, Sortino, beta, consistency
  mf_aggregation.py       — this file: orchestration + parallelism

Columns written to market.mf_nav_enriched:
  Technical : week_*, month_*, sma_20/50/200, ema_20/50, rsi_14, atr_14,
              high_52w, low_52w, ytd_return_pct, all_time_high, drawdown_pct
  Returns   : return_1m/3m/6m/1y/3y/5y/10y, cagr_1y/3y/5y/10y,
              sip_return_1y/3y/5y
  Risk      : volatility_1y, sharpe_1y, sortino_1y, max_drawdown_pct,
              max_drawdown_days, beta_vs_nifty, consistency_score

Incremental logic:
  • Fetches ALL last enriched dates in ONE query at startup
  • Warmup window = 3650 days (10y) to support return_10y computation
  • No DELETE mutations — ReplacingMergeTree(version) deduplicates
  • Always SELECT ... FINAL on mf_nav_enriched for correct reads

Usage:
  python mf_aggregation.py                   # incremental (default)
  python mf_aggregation.py --full            # full recompute all schemes
  python mf_aggregation.py --scheme 119598   # single scheme (debug)
"""

import logging
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd

from mf_db import (
    get_ch_client,
    fetch_all_scheme_codes,
    fetch_last_enriched_date,
    fetch_all_last_enriched_dates,
    fetch_nav_for_scheme,
    fetch_nifty_nav,
    insert_enriched,
)
from mf_compute_technical import compute_all_technical
from mf_compute_returns    import compute_all_returns
from mf_compute_risk       import compute_all_risk

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────
BATCH_SIZE   = 200   # log progress every N successes
MAX_WORKERS  = 8     # parallel threads
WARMUP_DAYS  = 3650  # 10y lookback for return_10y + risk metrics

# ── Results tracker (thread-safe) ──────────────────────
results      = {"success": 0, "skipped": 0, "failed": []}
results_lock = threading.Lock()

# ── Column order matching migration schema ─────────────
ENRICHED_COLS = [
    "date", "scheme_code", "nav",
    # weekly rollup
    "week_start", "week_open", "week_high", "week_low",
    "week_close", "week_return_pct",
    # monthly rollup
    "month_start", "month_open", "month_high", "month_low",
    "month_close", "month_return_pct",
    # technical
    "sma_20", "sma_50", "sma_200",
    "ema_20", "ema_50",
    "rsi_14", "atr_14",
    "high_52w", "low_52w",
    "ytd_return_pct",
    "all_time_high", "drawdown_pct",
    # returns (migration 027)
    "return_1m", "return_3m", "return_6m",
    "return_1y", "return_3y", "return_5y", "return_10y",
    "cagr_1y", "cagr_3y", "cagr_5y", "cagr_10y",
    "sip_return_1y", "sip_return_3y", "sip_return_5y",
    # risk (migration 028)
    "volatility_1y", "sharpe_1y", "sortino_1y",
    "max_drawdown_pct", "max_drawdown_days",
    "beta_vs_nifty", "consistency_score",
    # housekeeping
    "computed_at", "version",
]


# ══════════════════════════════════════════════════════
# Core enrichment
# ══════════════════════════════════════════════════════

def enrich_scheme(df_raw: pd.DataFrame,
                  scheme_code: int,
                  nifty_df: pd.DataFrame) -> pd.DataFrame:
    """
    Run all enrichment passes on raw NAV data for one scheme.
    df_raw must be sorted by date ascending.
    """
    df = df_raw.copy().sort_values("date").reset_index(drop=True)
    df["scheme_code"] = scheme_code

    # Pass 1 — technical indicators
    df = compute_all_technical(df)

    # Pass 2 — returns + CAGR + SIP XIRR
    df = compute_all_returns(df)

    # Pass 3 — risk metrics
    df = compute_all_risk(df, nifty_df)

    # Housekeeping
    df["computed_at"] = datetime.now()
    df["version"]     = int(datetime.now().timestamp())

    return df[ENRICHED_COLS]


# ══════════════════════════════════════════════════════
# Per-scheme processing
# ══════════════════════════════════════════════════════

def process_scheme(ch, scheme_code: int,
                   nifty_df: pd.DataFrame,
                   last_date=None,
                   idx: int = 0,
                   total: int = 0):
    try:
        df_raw = fetch_nav_for_scheme(
            ch, scheme_code,
            from_date=last_date,
            warmup_days=WARMUP_DAYS
        )

        if df_raw.empty:
            with results_lock:
                results["skipped"] += 1
            return

        df_enriched = enrich_scheme(df_raw, scheme_code, nifty_df)

        # Trim warm-up rows — only insert rows after last_date
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
    parser = argparse.ArgumentParser(description="MF NAV Aggregation Pipeline")
    parser.add_argument("--full",   action="store_true",
                        help="Recompute all schemes from scratch")
    parser.add_argument("--scheme", type=int, default=None,
                        help="Process a single scheme_code (debug)")
    args = parser.parse_args()

    log.info("=== MF NAV Aggregation Pipeline Starting ===")
    log.info(f"Mode     : {'FULL RECOMPUTE' if args.full else 'INCREMENTAL'}")
    log.info(f"Started  : {datetime.now()}")

    ch = get_ch_client()
    log.info("ClickHouse connected ✅")

    # Fetch NIFTYBEES benchmark once — shared across all threads (read-only)
    log.info("Fetching NIFTYBEES benchmark from ohlcv_daily...")
    nifty_df = fetch_nifty_nav(ch)
    log.info(f"NIFTYBEES rows: {len(nifty_df)} "
             f"({nifty_df['date'].min() if not nifty_df.empty else 'none'} → "
             f"{nifty_df['date'].max() if not nifty_df.empty else 'none'})")

    if args.scheme:
        # ── Single-scheme debug mode ───────────────────
        log.info(f"Processing single scheme: {args.scheme}")
        last_date = None if args.full else fetch_last_enriched_date(ch, args.scheme)
        process_scheme(ch, args.scheme, nifty_df,
                       last_date=last_date, idx=1, total=1)
    else:
        # ── All schemes — parallel ─────────────────────
        scheme_codes   = fetch_all_scheme_codes(ch)
        total          = len(scheme_codes)
        full_recompute = args.full

        log.info(f"Schemes to process : {total}")
        log.info(f"Workers            : {MAX_WORKERS}")

        if full_recompute:
            log.warning("Full recompute — re-enriching ALL schemes.")
            last_dates = {}
        else:
            log.info("Fetching all last enriched dates in ONE query...")
            last_dates = fetch_all_last_enriched_dates(ch)
            log.info(f"Got watermarks for {len(last_dates)} enriched schemes")

        def _worker(item):
            idx, code = item
            # Each thread gets its own CH connection
            # nifty_df is read-only — safe to share across threads
            thread_ch = get_ch_client()
            process_scheme(
                thread_ch, code, nifty_df,
                last_date=last_dates.get(code),
                idx=idx, total=total
            )

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(_worker, enumerate(scheme_codes, 1))

    print_summary()
    log.info(f"Finished : {datetime.now()}")


if __name__ == "__main__":
    main()