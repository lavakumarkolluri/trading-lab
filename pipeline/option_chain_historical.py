#!/usr/bin/env python3
"""
option_chain_historical.py
───────────────────────────
Downloads NSE F&O bhavcopy (daily option OHLC + OI for all contracts)
and populates market.options_chain with historical EOD data.

Source: https://nsearchives.nseindia.com/content/historical/DERIVATIVES/
        {YYYY}/{MMM}/fo{DDMMMYYYY}bhav.csv.zip

Bhavcopy provides: OHLC, OI, volume for all NIFTY/BANKNIFTY option strikes
and all expiries, for every trading day.
Each row is saved with timestamp = date @ 15:30:00 IST (EOD marker).

Usage:
  python option_chain_historical.py                    # last 2 years
  python option_chain_historical.py --from 2024-01-01
  python option_chain_historical.py --from 2024-01-01 --to 2024-03-31
  python option_chain_historical.py --status

Docker:
  docker compose run --rm option_chain_historical
"""

import io
import os
import sys
import time
import logging
import argparse
import zipfile
from datetime import datetime, date, timedelta

import pandas as pd
import clickhouse_connect
from curl_cffi import requests as cffi_requests
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────
CH_HOST  = os.getenv("CH_HOST", "clickhouse")
CH_PORT  = int(os.getenv("CH_PORT", "8123"))
CH_USER  = os.getenv("CH_USER", "default")
CH_PASS  = os.getenv("CH_PASSWORD", "")

LOOKBACK_DAYS = 730    # default 2 years
REQUEST_DELAY = 1.5    # seconds between downloads
HTTP_TIMEOUT  = 30

BHAVCOPY_URL = (
    "https://nsearchives.nseindia.com/content/historical/DERIVATIVES"
    "/{yyyy}/{mmm}/fo{ddmmmyyyy}bhav.csv.zip"
)

# Only index options for NIFTY and BANKNIFTY
TARGET_INSTRUMENT = "OPTIDX"
TARGET_SYMBOLS    = {"NIFTY", "BANKNIFTY"}

# EOD timestamp (market close IST — stored as naive datetime in ClickHouse)
EOD_HOUR   = 15
EOD_MINUTE = 30


# ── Client ────────────────────────────────────────────────

def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ── Already-loaded tracking ───────────────────────────────

def fetch_loaded_dates(ch) -> set[date]:
    """Dates already in market.options_chain as EOD rows (15:30 timestamp)."""
    result = ch.query(
        "SELECT DISTINCT toDate(timestamp) "
        "FROM market.options_chain "
        "WHERE toHour(timestamp) = 15 AND toMinute(timestamp) = 30"
    )
    return {row[0] for row in result.result_rows}


# ── NSE session ──────────────────────────────────────────

def build_session() -> cffi_requests.Session:
    # nsearchives.nseindia.com is a static CDN — no cookie warmup needed
    return cffi_requests.Session(impersonate="chrome110")


# ── Download ─────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=3, max=30),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def download_bhavcopy(session: cffi_requests.Session, d: date) -> bytes | None:
    """
    Download and return the raw ZIP bytes for a given date.
    Returns None if the file does not exist (weekend / holiday).
    """
    url = BHAVCOPY_URL.format(
        yyyy=d.strftime("%Y"),
        mmm=d.strftime("%b").upper(),
        ddmmmyyyy=d.strftime("%d%b%Y").upper(),
    )
    resp = session.get(url, timeout=HTTP_TIMEOUT)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.content


# ── Parse ─────────────────────────────────────────────────

def parse_bhavcopy(zip_bytes: bytes, trade_date: date) -> list[dict]:
    """
    Extract option rows for NIFTY and BANKNIFTY from the bhavcopy ZIP.
    Returns a list of row dicts for market.options_chain.
    """
    rows = []
    eod_ts = datetime(trade_date.year, trade_date.month, trade_date.day,
                      EOD_HOUR, EOD_MINUTE, 0)
    version = int(eod_ts.timestamp())

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
        if csv_name is None:
            return []
        with zf.open(csv_name) as f:
            # header=0: use first row as column names (CSV has trailing comma → 16 cols)
            df = pd.read_csv(f, header=0)

    df = df[
        (df["INSTRUMENT"].str.strip() == TARGET_INSTRUMENT) &
        (df["SYMBOL"].str.strip().isin(TARGET_SYMBOLS))
    ].copy()

    if df.empty:
        return []

    for _, r in df.iterrows():
        symbol = r["SYMBOL"].strip()
        opt_type = r["OPTION_TYP"].strip().upper()
        if opt_type not in ("CE", "PE"):
            continue

        try:
            expiry = datetime.strptime(r["EXPIRY_DT"].strip(), "%d-%b-%Y").date()
        except (ValueError, AttributeError):
            continue

        strike = float(r["STRIKE_PR"])
        ltp    = float(r["CLOSE"])    if pd.notna(r["CLOSE"])    else 0.0
        oi     = int(r["OPEN_INT"])   if pd.notna(r["OPEN_INT"]) else 0
        chg_oi = int(r["CHG_IN_OI"]) if pd.notna(r["CHG_IN_OI"]) else 0
        volume = int(r["CONTRACTS"])  if pd.notna(r["CONTRACTS"]) else 0

        rows.append({
            "symbol":      symbol,
            "timestamp":   eod_ts,
            "expiry":      expiry,
            "strike":      strike,
            "option_type": opt_type,
            "ltp":         ltp,
            "iv":          0.0,
            "oi":          oi,
            "oi_change":   chg_oi,
            "volume":      volume,
            "version":     version,
        })

    return rows


# ── Insert ────────────────────────────────────────────────

def insert_rows(ch, rows: list[dict]):
    df = pd.DataFrame(rows)
    ch.insert_df("market.options_chain", df[[
        "symbol", "timestamp", "expiry", "strike", "option_type",
        "ltp", "iv", "oi", "oi_change", "volume", "version",
    ]])


# ── Status ────────────────────────────────────────────────

def show_status(ch):
    r = ch.query(
        "SELECT symbol, count() as rows, "
        "countDistinct(toDate(timestamp)) as days, "
        "min(toDate(timestamp)) as first_date, "
        "max(toDate(timestamp)) as last_date "
        "FROM market.options_chain "
        "GROUP BY symbol ORDER BY symbol"
    ).result_rows
    print("\n-- market.options_chain --")
    print(f"{'Symbol':<12} {'Rows':>10} {'Days':>6} {'First':>12} {'Last':>12}")
    print("-" * 56)
    for row in r:
        print(f"{row[0]:<12} {row[1]:>10,} {row[2]:>6} {str(row[3]):>12} {str(row[4]):>12}")
    print()


# ── Main ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Option chain historical backfill from NSE bhavcopy"
    )
    parser.add_argument("--from", dest="from_date", type=str, default=None,
                        metavar="YYYY-MM-DD",
                        help=f"Start date (default: today minus {LOOKBACK_DAYS} days)")
    parser.add_argument("--to", dest="to_date", type=str, default=None,
                        metavar="YYYY-MM-DD",
                        help="End date (default: yesterday)")
    parser.add_argument("--status", action="store_true",
                        help="Show current DB stats and exit")
    args = parser.parse_args()

    ch = get_ch_client()

    if args.status:
        show_status(ch)
        return

    today = date.today()
    # Default: don't include today (market may still be open / bhavcopy not yet published)
    to_date   = date.fromisoformat(args.to_date) if args.to_date else today - timedelta(days=1)
    from_date = (date.fromisoformat(args.from_date) if args.from_date
                 else today - timedelta(days=LOOKBACK_DAYS))

    log.info("=== Option Chain Historical Backfill ===")
    log.info(f"Date range : {from_date} → {to_date}")

    loaded_dates = fetch_loaded_dates(ch)
    log.info(f"Already loaded : {len(loaded_dates)} EOD dates in DB")

    # Build list of dates to fetch (skip already loaded)
    dates_to_fetch = [
        from_date + timedelta(days=i)
        for i in range((to_date - from_date).days + 1)
        if (from_date + timedelta(days=i)) not in loaded_dates
    ]
    log.info(f"Dates to fetch : {len(dates_to_fetch)}")

    if not dates_to_fetch:
        log.info("Already up to date.")
        show_status(ch)
        return

    session = build_session()
    stats = {"loaded": 0, "skipped": 0, "failed": []}

    for i, d in enumerate(dates_to_fetch, 1):
        try:
            zip_bytes = download_bhavcopy(session, d)
        except Exception as e:
            log.error(f"[{i}/{len(dates_to_fetch)}] {d}: download failed — {e}")
            stats["failed"].append(str(d))
            time.sleep(REQUEST_DELAY)
            continue

        if zip_bytes is None:
            log.info(f"[{i}/{len(dates_to_fetch)}] {d}: no file (weekend/holiday) — skip")
            stats["skipped"] += 1
            time.sleep(0.3)
            continue

        rows = parse_bhavcopy(zip_bytes, d)
        if not rows:
            log.info(f"[{i}/{len(dates_to_fetch)}] {d}: no NIFTY/BANKNIFTY rows — skip")
            stats["skipped"] += 1
            time.sleep(REQUEST_DELAY)
            continue

        try:
            insert_rows(ch, rows)
            stats["loaded"] += 1
            log.info(f"[{i}/{len(dates_to_fetch)}] {d}: {len(rows)} rows inserted")
        except Exception as e:
            log.error(f"[{i}/{len(dates_to_fetch)}] {d}: insert failed — {e}")
            stats["failed"].append(str(d))

        time.sleep(REQUEST_DELAY)

    print(f"\n{'='*55}")
    print(f"  HISTORICAL BACKFILL COMPLETE")
    print(f"  Loaded  : {stats['loaded']} trading days")
    print(f"  Skipped : {stats['skipped']} (weekends/holidays/already loaded)")
    print(f"  Failed  : {len(stats['failed'])}")
    if stats["failed"]:
        print(f"  Dates   : {', '.join(stats['failed'])}")
    print(f"{'='*55}\n")

    show_status(ch)


if __name__ == "__main__":
    main()
