#!/usr/bin/env python3
"""
fii_dii_pipeline.py  (v3 — NSE JSON API, simple session)
──────────────────────────────────────────────────────────
Downloads FII/DII cash market data from NSE India's JSON API.

Source: https://www.nseindia.com/api/fiidiiTradeReact
        ?fromDate=DD-Mon-YYYY&toDate=DD-Mon-YYYY

No curl_cffi / TLS fingerprinting needed.
A plain requests.Session() with a one-shot homepage warm-up is sufficient.
The NSE API returns up to ~365 days per call; we chunk into 90-day windows
for safety (smaller chunks = fewer retries needed on flaky days).

Data flow: NSE API → MinIO staging (Parquet) → staging.file_registry → market.fii_dii

MinIO path:
  fii_dii/daily/{YYYY-MM-DD}/fii_dii.parquet  (one file per trading date)

Table: market.fii_dii (ReplacingMergeTree)
  date, entity, buy_value, sell_value, net_value, version
  Values in Rs crores (as reported by NSE — no unit conversion)

Entities: 'FII' and 'DII'  <- canonical names, never change

Usage:
  python fii_dii_pipeline.py                # incremental (default)
  python fii_dii_pipeline.py --full         # re-fetch last LOOKBACK_DAYS
  python fii_dii_pipeline.py --load-only    # reload staged MinIO -> ClickHouse
  python fii_dii_pipeline.py --from 2024-01-01
  python fii_dii_pipeline.py --status       # show DB stats and exit
"""

import os
import io
import sys
import time
import logging
import argparse
import threading
from datetime import datetime, date, timedelta

from curl_cffi import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import clickhouse_connect
from minio import Minio
from minio.error import S3Error
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# Config
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

MINIO_HOST   = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER   = os.getenv("MINIO_USER", "admin")
MINIO_PASS   = os.getenv("MINIO_PASSWORD")
if not MINIO_PASS:
    raise RuntimeError("MINIO_PASSWORD env var not set — refusing to start")
MINIO_BUCKET = "trading-data"

NSE_HOME_URL  = "https://www.nseindia.com"
NSE_FII_URL   = "https://www.nseindia.com/api/fiidiiTradeReact"

LOOKBACK_DAYS   = 730    # 2 years history on first run
CHUNK_DAYS      = 90     # days per API request
REQUEST_DELAY   = 1.5    # seconds between API calls
SESSION_TIMEOUT = 15     # HTTP timeout

ENTITY_MAP = {
    "FII/FPI": "FII",
    "FII":     "FII",
    "DII":     "DII",
}

PARQUET_SCHEMA = pa.schema([
    pa.field("date",       pa.date32()),
    pa.field("entity",     pa.string()),
    pa.field("buy_value",  pa.float64()),
    pa.field("sell_value", pa.float64()),
    pa.field("net_value",  pa.float64()),
])

results = {
    "staged":  0,
    "loaded":  0,
    "skipped": 0,
    "failed":  [],
}
results_lock = threading.Lock()


# Clients

def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


def get_minio_client():
    return Minio(MINIO_HOST, access_key=MINIO_USER,
                 secret_key=MINIO_PASS, secure=False)


# Preflight

def assert_tables_exist(ch):
    for db, tbl in [("market", "fii_dii"), ("staging", "file_registry")]:
        count = ch.query(
            "SELECT count() FROM system.tables "
            "WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": db, "tbl": tbl}
        ).result_rows[0][0]
        if not count:
            raise RuntimeError(
                f"Table {db}.{tbl} missing — run migrations first:\n"
                f"  docker compose run --rm migrate"
            )
    log.info("Tables verified")


def setup_minio_bucket(minio):
    if not minio.bucket_exists(MINIO_BUCKET):
        minio.make_bucket(MINIO_BUCKET)


# Watermarks

def fetch_loaded_dates(ch) -> set:
    result = ch.query("SELECT DISTINCT date FROM market.fii_dii FINAL")
    return {row[0] for row in result.result_rows}


def fetch_staged_dates(ch) -> set:
    result = ch.query(
        "SELECT DISTINCT toDate(file_date) "
        "FROM staging.file_registry "
        "WHERE agent_name = {agent:String} AND status = {s:String}",
        parameters={"agent": "fii_dii_pipeline", "s": "loaded"}
    )
    return {row[0] for row in result.result_rows}


# NSE session

def build_nse_session():
    session = requests.Session(impersonate="chrome110")
    log.info("NSE session: homepage warm-up (curl_cffi chrome110)...")
    resp = session.get(NSE_HOME_URL, timeout=SESSION_TIMEOUT)
    resp.raise_for_status()
    log.info(f"NSE session ready (cookies: {len(session.cookies)})")
    time.sleep(2.0)
    return session


# NSE fetch

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(min=2, max=30),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True
)
def fetch_chunk(session: requests.Session,
                from_date: date,
                to_date: date) -> list:
    """
    Fetch FII/DII JSON for a date range.
    NSE date format: DD-Mon-YYYY  e.g. 01-Jan-2025
    Returns [] for ranges that are entirely weekends/holidays.
    """
    params = {
        "fromDate": from_date.strftime("%d-%b-%Y"),
        "toDate":   to_date.strftime("%d-%b-%Y"),
    }
    resp = session.get(NSE_FII_URL, params=params, timeout=SESSION_TIMEOUT)
    resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, list):
        return []

    rows = []
    for item in data:
        raw_entity = item.get("category", "").strip().upper()
        entity = ENTITY_MAP.get(raw_entity)
        if entity is None:
            continue

        raw_date = item.get("date", "")
        if not raw_date:
            continue

        try:
            trade_date = datetime.strptime(raw_date, "%d-%b-%Y").date()
        except ValueError:
            try:
                trade_date = datetime.strptime(raw_date, "%d-%B-%Y").date()
            except ValueError:
                log.warning(f"Unparseable date: {raw_date!r} — skipping")
                continue

        rows.append({
            "date":       trade_date,
            "entity":     entity,
            "buy_value":  _parse_float(item.get("buyValue", 0)),
            "sell_value": _parse_float(item.get("sellValue", 0)),
            "net_value":  _parse_float(item.get("netValue", 0)),
        })

    return rows


def _parse_float(val) -> float:
    try:
        return float(str(val).replace(",", "").strip() or 0)
    except (ValueError, TypeError):
        return 0.0


# MinIO

def minio_path(trade_date: date) -> str:
    return f"fii_dii/daily/{trade_date.strftime('%Y-%m-%d')}/fii_dii.parquet"


def save_to_minio(minio: Minio, rows: list, trade_date: date) -> str:
    df = pd.DataFrame(rows)[["date", "entity", "buy_value", "sell_value", "net_value"]]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    table  = pa.Table.from_pandas(df, schema=PARQUET_SCHEMA, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    obj_path = minio_path(trade_date)
    minio.put_object(
        MINIO_BUCKET, obj_path, buffer, len(buffer.getvalue()),
        content_type="application/octet-stream"
    )
    return obj_path


def load_from_minio(minio: Minio, trade_date: date) -> pd.DataFrame:
    obj_path = minio_path(trade_date)
    try:
        response = minio.get_object(MINIO_BUCKET, obj_path)
        return pq.read_table(io.BytesIO(response.read())).to_pandas()
    except S3Error as e:
        if e.code == "NoSuchKey":
            return pd.DataFrame()
        raise


# File registry

def register_staged(ch, trade_date: date, obj_path: str, row_count: int):
    ch.insert(
        "staging.file_registry",
        [["fii_dii_pipeline", obj_path, trade_date, "staged", row_count, None, ""]],
        column_names=[
            "agent_name", "file_path", "file_date",
            "status", "row_count", "loaded_at", "error_message"
        ]
    )


def mark_loaded(ch, obj_path: str):
    ch.command(
        "ALTER TABLE staging.file_registry UPDATE "
        "status = 'loaded', loaded_at = now() "
        "WHERE agent_name = {agent:String} AND file_path = {path:String} "
        "SETTINGS mutations_sync = 0",
        parameters={"agent": "fii_dii_pipeline", "path": obj_path}
    )


def mark_failed(ch, obj_path: str, error: str):
    ch.command(
        "ALTER TABLE staging.file_registry UPDATE "
        "status = 'failed', error_message = {err:String} "
        "WHERE agent_name = {agent:String} AND file_path = {path:String} "
        "SETTINGS mutations_sync = 0",
        parameters={"agent": "fii_dii_pipeline", "path": obj_path, "err": error[:500]}
    )


# ClickHouse insert

def insert_to_clickhouse(ch, df: pd.DataFrame):
    if df.empty:
        return
    df = df.copy()
    df["version"] = int(datetime.now().timestamp())
    ch.insert_df(
        "market.fii_dii",
        df[["date", "entity", "buy_value", "sell_value", "net_value", "version"]]
    )


# Per-chunk processor

def process_chunk(session: requests.Session,
                  minio: Minio,
                  chunk_from: date,
                  chunk_to: date,
                  loaded_dates: set,
                  staged_dates: set,
                  idx: int,
                  total: int):
    ch = get_ch_client()

    dates_in_chunk = []
    d = chunk_from
    while d <= chunk_to:
        dates_in_chunk.append(d)
        d += timedelta(days=1)

    already_loaded   = [d for d in dates_in_chunk if d in loaded_dates]
    need_staged_load = [d for d in dates_in_chunk
                        if d not in loaded_dates and d in staged_dates]
    need_nse_fetch   = [d for d in dates_in_chunk
                        if d not in loaded_dates and d not in staged_dates]

    if already_loaded:
        with results_lock:
            results["skipped"] += len(already_loaded)

    # Load staged files from MinIO directly
    for d in need_staged_load:
        try:
            df = load_from_minio(minio, d)
            if df.empty:
                need_nse_fetch.append(d)
                continue
            insert_to_clickhouse(ch, df)
            mark_loaded(ch, minio_path(d))
            with results_lock:
                results["loaded"] += 1
            log.info(f"  [{idx}/{total}] {d}: loaded from MinIO")
        except Exception as e:
            log.error(f"  [{idx}/{total}] {d}: MinIO load failed — {e}")
            need_nse_fetch.append(d)

    if not need_nse_fetch:
        return

    # Fetch from NSE
    fetch_from = min(need_nse_fetch)
    fetch_to   = max(need_nse_fetch)

    try:
        all_rows = fetch_chunk(session, fetch_from, fetch_to)
        time.sleep(REQUEST_DELAY)
    except Exception as e:
        log.error(f"  [{idx}/{total}] NSE fetch {fetch_from}-->{fetch_to} FAILED: {e}")
        with results_lock:
            results["failed"].append(f"{fetch_from}-->{fetch_to}: {e}")
        return

    if not all_rows:
        with results_lock:
            results["skipped"] += len(need_nse_fetch)
        log.debug(f"  [{idx}/{total}] {fetch_from}-->{fetch_to}: no data (holidays/weekends)")
        return

    # Group by date and persist each trading day
    rows_by_date = {}
    for row in all_rows:
        rows_by_date.setdefault(row["date"], []).append(row)

    for trade_date, date_rows in sorted(rows_by_date.items()):
        obj_path = minio_path(trade_date)
        try:
            save_to_minio(minio, date_rows, trade_date)
            register_staged(ch, trade_date, obj_path, len(date_rows))

            df = pd.DataFrame(date_rows)
            insert_to_clickhouse(ch, df)
            mark_loaded(ch, obj_path)

            with results_lock:
                results["staged"] += 1
                results["loaded"] += 1

            fii_net = next((r["net_value"] for r in date_rows if r["entity"] == "FII"), 0)
            dii_net = next((r["net_value"] for r in date_rows if r["entity"] == "DII"), 0)
            log.info(
                f"  [{idx}/{total}] {trade_date}: "
                f"FII={fii_net:+.0f}cr  DII={dii_net:+.0f}cr"
            )
        except Exception as e:
            log.error(f"  [{idx}/{total}] {trade_date}: FAILED — {e}")
            try:
                mark_failed(ch, obj_path, str(e))
            except Exception:
                pass
            with results_lock:
                results["failed"].append(f"{trade_date}: {e}")


# Status

def show_status(ch):
    summary = ch.query(
        "SELECT entity, count() as days, min(date), max(date), "
        "       round(sum(net_value), 2) as total_net_cr "
        "FROM market.fii_dii FINAL "
        "GROUP BY entity ORDER BY entity"
    ).result_rows

    print("\n-- market.fii_dii Summary --")
    print(f"{'Entity':<6} {'Days':>6} {'From':<12} {'To':<12} {'Net Rs Cr':>14}")
    print("-" * 54)
    for r in summary:
        print(f"{r[0]:<6} {r[1]:>6} {str(r[2]):<12} {str(r[3]):<12} {r[4]:>14,.2f}")

    recent = ch.query(
        "SELECT date, entity, buy_value, sell_value, net_value "
        "FROM market.fii_dii FINAL "
        "ORDER BY date DESC, entity LIMIT 6"
    ).result_rows

    print("\n-- Last 3 trading days --")
    print(f"{'Date':<12} {'E':<5} {'Buy Rs Cr':>12} {'Sell Rs Cr':>12} {'Net Rs Cr':>12}")
    print("-" * 57)
    for r in recent:
        print(f"{str(r[0]):<12} {r[1]:<5} {r[2]:>12,.2f} {r[3]:>12,.2f} {r[4]:>12,.2f}")
    print()


# Main

def main():
    parser = argparse.ArgumentParser(
        description="FII/DII Pipeline v3 — NSE JSON API -> MinIO -> ClickHouse"
    )
    parser.add_argument("--full",      action="store_true",
                        help=f"Full backfill ({LOOKBACK_DAYS} days)")
    parser.add_argument("--from",      dest="from_date", type=str, default=None,
                        metavar="YYYY-MM-DD")
    parser.add_argument("--load-only", action="store_true",
                        help="Skip NSE fetch; reload staged MinIO files into ClickHouse")
    parser.add_argument("--status",    action="store_true")
    args = parser.parse_args()

    log.info("=== FII/DII Pipeline v3 (NSE JSON API) Starting ===")
    log.info(f"Started : {datetime.now()}")

    ch    = get_ch_client()
    minio = get_minio_client()
    log.info("ClickHouse connected")

    assert_tables_exist(ch)
    setup_minio_bucket(minio)

    if args.status:
        show_status(ch)
        return

    today = date.today()

    # Load-only mode
    if args.load_only:
        staged  = fetch_staged_dates(ch)
        loaded  = fetch_loaded_dates(ch)
        pending = sorted(staged - loaded)
        log.info(f"Load-only: {len(pending)} dates pending")
        for i, d in enumerate(pending, 1):
            try:
                df = load_from_minio(minio, d)
                if df.empty:
                    continue
                insert_to_clickhouse(ch, df)
                mark_loaded(ch, minio_path(d))
                log.info(f"  [{i}/{len(pending)}] {d}: loaded")
                with results_lock:
                    results["loaded"] += 1
            except Exception as e:
                log.error(f"  {d}: FAILED — {e}")
                with results_lock:
                    results["failed"].append(f"{d}: {e}")
        _print_summary()
        return

    # Determine fetch window
    staged_dates = fetch_staged_dates(ch)
    loaded_dates = fetch_loaded_dates(ch)

    if args.full:
        fetch_from   = today - timedelta(days=LOOKBACK_DAYS)
        staged_dates = set()
        loaded_dates = set()
        log.info(f"Mode: FULL (from {fetch_from})")
    elif args.from_date:
        fetch_from = date.fromisoformat(args.from_date)
        log.info(f"Mode: FROM {fetch_from}")
    else:
        covered    = staged_dates | loaded_dates
        fetch_from = (max(covered) + timedelta(days=1)) if covered \
                     else (today - timedelta(days=LOOKBACK_DAYS))
        log.info(f"Mode: {'INCREMENTAL' if covered else 'FIRST RUN'} (from {fetch_from})")

    log.info(f"Staged: {len(staged_dates)} | Loaded: {len(loaded_dates)}")

    if fetch_from > today:
        log.info("Already up to date.")
        return

    # Build NSE session
    try:
        session = build_nse_session()
    except Exception as e:
        log.error(f"Could not establish NSE session: {e}")
        sys.exit(1)

    # Chunk date range and process sequentially
    # (sequential because NSE rate limits — no parallel chunking)
    chunks = []
    current = fetch_from
    while current <= today:
        end = min(current + timedelta(days=CHUNK_DAYS - 1), today)
        chunks.append((current, end))
        current = end + timedelta(days=1)

    total = len(chunks)
    log.info(f"Date range : {fetch_from} --> {today}")
    log.info(f"Chunks     : {total} x {CHUNK_DAYS}-day windows")

    for idx, (chunk_from, chunk_to) in enumerate(chunks, 1):
        log.info(f"Chunk [{idx}/{total}]: {chunk_from} --> {chunk_to}")
        process_chunk(
            session, minio, chunk_from, chunk_to,
            loaded_dates, staged_dates, idx, total
        )

    _print_summary()
    if results["loaded"] > 0:
        show_status(ch)
    log.info(f"Finished : {datetime.now()}")


def _print_summary():
    print("\n" + "=" * 60)
    print("FII/DII PIPELINE v3 -- SUMMARY")
    print("=" * 60)
    print(f"Staged to MinIO  : {results['staged']} dates")
    print(f"Loaded to CH     : {results['loaded']} dates")
    print(f"Skipped          : {results['skipped']} dates (weekends/holidays/current)")
    print(f"Failed           : {len(results['failed'])}")
    if results["failed"]:
        for f in results["failed"][:15]:
            print(f"   {f}")
    print("=" * 60)


if __name__ == "__main__":
    main()