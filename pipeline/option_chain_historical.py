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
from minio import Minio
from minio.error import S3Error

from fo_utils import build_lot_size_cache

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

MINIO_HOST   = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER   = os.getenv("MINIO_USER", "admin")
MINIO_PASS   = os.getenv("MINIO_PASSWORD", "")
MINIO_BUCKET = "trading-data"
MINIO_PREFIX = "bhavcopy/fo/"

LOOKBACK_DAYS = 730    # default 2 years
REQUEST_DELAY = 1.5    # seconds between downloads
HTTP_TIMEOUT  = 30

# Old format (up to ~Jul 2024): /historical/DERIVATIVES/YYYY/MMM/foDDMMMYYYYbhav.csv.zip
BHAVCOPY_URL_OLD = (
    "https://nsearchives.nseindia.com/content/historical/DERIVATIVES"
    "/{yyyy}/{mmm}/fo{ddmmmyyyy}bhav.csv.zip"
)
# New format (Jul 2024 onwards): /content/fo/BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip
BHAVCOPY_URL_NEW = (
    "https://nsearchives.nseindia.com/content/fo"
    "/BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip"
)

TARGET_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}

# EOD timestamp (market close IST — stored as naive datetime in ClickHouse)
EOD_HOUR   = 15
EOD_MINUTE = 30


# ── Client ────────────────────────────────────────────────

def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


def get_mc() -> Minio:
    return Minio(MINIO_HOST, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)


def save_to_minio(mc: Minio, zip_bytes: bytes, trade_date: date):
    key = f"{MINIO_PREFIX}{trade_date.strftime('%Y%m%d')}.zip"
    try:
        mc.stat_object(MINIO_BUCKET, key)
    except S3Error:
        mc.put_object(MINIO_BUCKET, key, io.BytesIO(zip_bytes), len(zip_bytes),
                      content_type="application/zip")


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

def _get(session: cffi_requests.Session, url: str) -> bytes | None:
    resp = session.get(url, timeout=HTTP_TIMEOUT)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.content


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=3, max=30),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def download_bhavcopy(session: cffi_requests.Session, d: date) -> bytes | None:
    """
    Try old URL format first, then new format (NSE changed schema ~Jul 2024).
    Returns ZIP bytes, or None if the date has no file (weekend/holiday).
    """
    old_url = BHAVCOPY_URL_OLD.format(
        yyyy=d.strftime("%Y"),
        mmm=d.strftime("%b").upper(),
        ddmmmyyyy=d.strftime("%d%b%Y").upper(),
    )
    data = _get(session, old_url)
    if data is not None:
        return data

    new_url = BHAVCOPY_URL_NEW.format(yyyymmdd=d.strftime("%Y%m%d"))
    return _get(session, new_url)


# ── Parse ─────────────────────────────────────────────────

def _parse_old_format(df: pd.DataFrame, eod_ts: datetime, version: int) -> list[dict]:
    """Old bhavcopy: INSTRUMENT/SYMBOL/EXPIRY_DT/STRIKE_PR/OPTION_TYP/CLOSE/OPEN_INT..."""
    df = df[
        (df["INSTRUMENT"].str.strip() == "OPTIDX") &
        (df["SYMBOL"].str.strip().isin(TARGET_SYMBOLS))
    ].copy()
    rows = []
    for _, r in df.iterrows():
        opt_type = str(r["OPTION_TYP"]).strip().upper()
        if opt_type not in ("CE", "PE"):
            continue
        try:
            expiry = datetime.strptime(str(r["EXPIRY_DT"]).strip(), "%d-%b-%Y").date()
        except ValueError:
            continue
        rows.append({
            "symbol":      str(r["SYMBOL"]).strip(),
            "timestamp":   eod_ts,
            "expiry":      expiry,
            "strike":      float(r["STRIKE_PR"]),
            "option_type": opt_type,
            "ltp":         float(r["CLOSE"])    if pd.notna(r["CLOSE"])    else 0.0,
            "iv":          0.0,
            "oi":          int(r["OPEN_INT"])   if pd.notna(r["OPEN_INT"]) else 0,
            "oi_change":   int(r["CHG_IN_OI"])  if pd.notna(r["CHG_IN_OI"]) else 0,
            "volume":      int(r["CONTRACTS"])  if pd.notna(r["CONTRACTS"]) else 0,
            "version":     version,
        })
    return rows


def _parse_new_format(df: pd.DataFrame, eod_ts: datetime, version: int) -> list[dict]:
    """New bhavcopy (Jul 2024+): FinInstrmTp/TckrSymb/XpryDt/StrkPric/OptnTp/ClsPric..."""
    df = df[
        (df["FinInstrmTp"].str.strip() == "IDO") &
        (df["TckrSymb"].str.strip().isin(TARGET_SYMBOLS))
    ].copy()
    rows = []
    for _, r in df.iterrows():
        opt_type = str(r["OptnTp"]).strip().upper()
        if opt_type not in ("CE", "PE"):
            continue
        try:
            expiry = datetime.strptime(str(r["XpryDt"]).strip(), "%Y-%m-%d").date()
        except ValueError:
            continue
        rows.append({
            "symbol":      str(r["TckrSymb"]).strip(),
            "timestamp":   eod_ts,
            "expiry":      expiry,
            "strike":      float(r["StrkPric"]),
            "option_type": opt_type,
            "ltp":         float(r["ClsPric"])         if pd.notna(r["ClsPric"])         else 0.0,
            "iv":          0.0,
            "oi":          int(r["OpnIntrst"])         if pd.notna(r["OpnIntrst"])       else 0,
            "oi_change":   int(r["ChngInOpnIntrst"])   if pd.notna(r["ChngInOpnIntrst"]) else 0,
            "volume":      int(r["TtlTradgVol"])       if pd.notna(r["TtlTradgVol"])     else 0,
            "version":     version,
        })
    return rows


def extract_lot_sizes_from_df(df: pd.DataFrame) -> dict[str, int]:
    """Return {symbol: min_lot_size} from a parsed bhavcopy DataFrame."""
    col = "NewBrdLotQty" if "NewBrdLotQty" in df.columns else None
    sym = "TckrSymb"     if "TckrSymb"     in df.columns else ("SYMBOL" if "SYMBOL" in df.columns else None)
    if not col or not sym:
        return {}
    valid = df[df[col].notna() & (df[col] > 0)].copy()
    valid[sym] = valid[sym].str.strip()
    return {s: int(g[col].min()) for s, g in valid.groupby(sym)}


def upsert_lot_sizes(ch, lot_sizes: dict[str, int], trade_date: date,
                     known: dict[str, list]) -> list[str]:
    """
    Insert a row into market.fo_lot_sizes for each symbol whose lot_size
    differs from the last known value. Updates `known` in-place.
    Returns list of symbols that changed.
    """
    changed = []
    now     = datetime.utcnow()
    version = int(now.timestamp())
    rows    = []

    for sym, ls in lot_sizes.items():
        history = known.get(sym, [])
        last_ls = history[-1][1] if history else None
        if last_ls != ls:
            rows.append({
                "symbol":         sym,
                "effective_from": trade_date,
                "lot_size":       ls,
                "computed_at":    now,
                "version":        version,
            })
            known.setdefault(sym, []).append((trade_date, ls))
            changed.append(sym)

    if rows:
        ch.insert_df("market.fo_lot_sizes", pd.DataFrame(rows))
    return changed


def parse_bhavcopy(zip_bytes: bytes, trade_date: date) -> tuple[list[dict], dict[str, int]]:
    """Returns (option_chain_rows, {symbol: lot_size})."""
    eod_ts  = datetime(trade_date.year, trade_date.month, trade_date.day, EOD_HOUR, EOD_MINUTE)
    version = int(eod_ts.timestamp())

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
        if csv_name is None:
            return [], {}
        with zf.open(csv_name) as f:
            df = pd.read_csv(f, header=0)

    lot_sizes = extract_lot_sizes_from_df(df)

    if "INSTRUMENT" in df.columns:
        return _parse_old_format(df, eod_ts, version), lot_sizes
    elif "FinInstrmTp" in df.columns:
        return _parse_new_format(df, eod_ts, version), lot_sizes
    return [], lot_sizes


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


# ── Reprocess ─────────────────────────────────────────────

def reprocess_from_minio(ch, mc: Minio):
    """
    Re-parse all bhavcopy zips already in MinIO using the current TARGET_SYMBOLS.
    Used to backfill new symbols (e.g. FINNIFTY, MIDCPNIFTY) without re-downloading.
    Insert is idempotent via ReplacingMergeTree.
    """
    objs = sorted(
        mc.list_objects(MINIO_BUCKET, prefix=MINIO_PREFIX, recursive=True),
        key=lambda o: o.object_name
    )
    log.info(f"Reprocessing {len(objs)} bhavcopy files from MinIO "
             f"with symbols: {TARGET_SYMBOLS}")

    lot_size_known = build_lot_size_cache(ch)
    loaded = skipped = 0

    for obj in objs:
        name = obj.object_name.split("/")[-1].replace(".zip", "")
        try:
            trade_date = datetime.strptime(name, "%Y%m%d").date()
        except ValueError:
            continue

        resp      = mc.get_object(MINIO_BUCKET, obj.object_name)
        zip_bytes = resp.read()
        resp.close()

        rows, lot_sizes = parse_bhavcopy(zip_bytes, trade_date)

        if lot_sizes:
            upsert_lot_sizes(ch, lot_sizes, trade_date, lot_size_known)

        if not rows:
            skipped += 1
            continue

        try:
            insert_rows(ch, rows)
            loaded += 1
            if loaded % 50 == 0:
                log.info(f"  {loaded} files inserted so far...")
        except Exception as e:
            log.error(f"{trade_date}: insert failed — {e}")

    log.info(f"Reprocess complete: {loaded} files inserted, {skipped} skipped")
    show_status(ch)


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
    parser.add_argument("--status",    action="store_true",
                        help="Show current DB stats and exit")
    parser.add_argument("--reprocess", action="store_true",
                        help="Re-parse all MinIO bhavcopy zips (backfills new symbols)")
    args = parser.parse_args()

    ch = get_ch_client()
    mc = get_mc()

    if args.status:
        show_status(ch)
        return

    if args.reprocess:
        log.info("=== Reprocess mode — reading from MinIO ===")
        reprocess_from_minio(ch, mc)
        return

    today = date.today()
    # Include today when running after market close (bhavcopy published by ~17:00 IST = 11:30 UTC).
    # If NSE hasn't published yet, download_bhavcopy returns None → "no file" skip, which is safe.
    to_date   = date.fromisoformat(args.to_date) if args.to_date else today
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

    session       = build_session()
    lot_size_known = build_lot_size_cache(ch)   # preload current lot size history
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

        save_to_minio(mc, zip_bytes, d)   # idempotent — skips if already exists
        rows, lot_sizes = parse_bhavcopy(zip_bytes, d)

        # Always upsert lot sizes — captures any changes even on days with no new options rows
        if lot_sizes:
            changed = upsert_lot_sizes(ch, lot_sizes, d, lot_size_known)
            if changed:
                log.info(f"[{i}/{len(dates_to_fetch)}] {d}: lot size changed for {changed}")

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
