#!/usr/bin/env python3
"""
participant_oi_pipeline.py
──────────────────────────
Downloads NSE F&O Participant wise Open Interest data into market.participant_oi.

Source: NSE archives (no session/cookie needed — direct CSV download)
  https://archives.nseindia.com/content/nsccl/fao_participant_oi_DDMMYYYY.csv

Data: Number of contracts per entity per contract type as of end of day.
Entities: Client, DII, FII, Pro, TOTAL

Data flow: NSE archives → MinIO staging (Parquet) → staging.file_registry → market.participant_oi

MinIO path: participant_oi/daily/{YYYY-MM-DD}/participant_oi.parquet

Backfill: 2 years of daily CSVs available at NSE archives — no auth needed.
Incremental: checks max(date) in market.participant_oi, fetches only missing dates.

Skips weekends and NSE holidays automatically (404 = no trading = skip).

Usage:
  python participant_oi_pipeline.py              # incremental (default)
  python participant_oi_pipeline.py --full       # backfill LOOKBACK_DAYS
  python participant_oi_pipeline.py --load-only  # reload MinIO → ClickHouse
  python participant_oi_pipeline.py --status     # show DB stats
  python participant_oi_pipeline.py --date 2026-04-09  # single date debug
"""

import os
import io
import sys
import time
import logging
import argparse
import threading
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import clickhouse_connect
from minio import Minio
from minio.error import S3Error
from curl_cffi import requests
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

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

MINIO_HOST   = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER   = os.getenv("MINIO_USER", "admin")
MINIO_PASS = os.getenv("MINIO_PASSWORD")
if not MINIO_PASS:
    raise RuntimeError("MINIO_PASSWORD env var not set — refusing to start")
MINIO_BUCKET = "trading-data"

NSE_ARCHIVE_URL = (
    "https://archives.nseindia.com/content/nsccl/fao_participant_oi_{date}.csv"
)

LOOKBACK_DAYS  = 730   # 2 years
MAX_WORKERS    = 6     # NSE archives are more permissive than the main site
REQUEST_DELAY  = 0.5   # seconds between requests per worker

# Entities expected in every CSV
ENTITIES = ["Client", "DII", "FII", "Pro", "TOTAL"]

PARQUET_SCHEMA = pa.schema([
    pa.field("date",                 pa.date32()),
    pa.field("entity",               pa.string()),
    pa.field("fut_index_long",       pa.int64()),
    pa.field("fut_index_short",      pa.int64()),
    pa.field("fut_index_net",        pa.int64()),
    pa.field("fut_stock_long",       pa.int64()),
    pa.field("fut_stock_short",      pa.int64()),
    pa.field("fut_stock_net",        pa.int64()),
    pa.field("opt_index_call_long",  pa.int64()),
    pa.field("opt_index_call_short", pa.int64()),
    pa.field("opt_index_call_net",   pa.int64()),
    pa.field("opt_index_put_long",   pa.int64()),
    pa.field("opt_index_put_short",  pa.int64()),
    pa.field("opt_index_put_net",    pa.int64()),
    pa.field("opt_stock_call_long",  pa.int64()),
    pa.field("opt_stock_call_short", pa.int64()),
    pa.field("opt_stock_call_net",   pa.int64()),
    pa.field("opt_stock_put_long",   pa.int64()),
    pa.field("opt_stock_put_short",  pa.int64()),
    pa.field("opt_stock_put_net",    pa.int64()),
    pa.field("total_long",           pa.int64()),
    pa.field("total_short",          pa.int64()),
    pa.field("total_net",            pa.int64()),
])

# ── Results tracker ────────────────────────────────────
results = {
    "staged":   0,
    "loaded":   0,
    "skipped":  0,   # weekends/holidays (404)
    "failed":   [],
}
results_lock = threading.Lock()


# ══════════════════════════════════════════════════════
# Clients
# ══════════════════════════════════════════════════════

def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


def get_minio_client():
    return Minio(MINIO_HOST, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)


# ══════════════════════════════════════════════════════
# Preflight
# ══════════════════════════════════════════════════════

def assert_tables_exist(ch):
    for db, tbl in [("market", "participant_oi"), ("staging", "file_registry")]:
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
    log.info("Tables verified ✅")


def setup_minio_bucket(minio):
    if not minio.bucket_exists(MINIO_BUCKET):
        minio.make_bucket(MINIO_BUCKET)


# ══════════════════════════════════════════════════════
# Watermarks — ONE query each
# ══════════════════════════════════════════════════════

def fetch_loaded_dates(ch) -> set[date]:
    result = ch.query("SELECT DISTINCT date FROM market.participant_oi FINAL")
    return {row[0] for row in result.result_rows}


def fetch_staged_dates(ch) -> set[date]:
    result = ch.query(
        "SELECT DISTINCT toDate(file_date) FROM staging.file_registry "
        "WHERE agent_name = {agent:String} AND status = {s:String}",
        parameters={"agent": "participant_oi_pipeline", "s": "loaded"}
    )
    return {row[0] for row in result.result_rows}


# ══════════════════════════════════════════════════════
# NSE fetch — one date
# ══════════════════════════════════════════════════════

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=1, max=10),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True
)
def fetch_csv_for_date(trade_date: date) -> str | None:
    """
    Download participant OI CSV for one trading date.
    Returns raw CSV text, or None if 404 (holiday/weekend).
    Raises on other HTTP errors (triggers tenacity retry).
    """
    date_str = trade_date.strftime("%d%m%Y")
    url      = NSE_ARCHIVE_URL.format(date=date_str)
    resp     = requests.get(url, timeout=15)

    if resp.status_code == 404:
        return None   # holiday or weekend — not an error
    resp.raise_for_status()
    return resp.text


# ══════════════════════════════════════════════════════
# CSV parser
# ══════════════════════════════════════════════════════

def parse_csv(csv_text: str, trade_date: date) -> pd.DataFrame:
    """
    Parse NSE participant OI CSV into a clean DataFrame.

    CSV format:
      Row 0: title line (skip)
      Row 1: header
      Rows 2-6: Client, DII, FII, Pro, TOTAL

    Columns (after header row):
      Client Type,
      Future Index Long, Future Index Short,
      Future Stock Long, Future Stock Short,
      Option Index Call Long, Option Index Put Long,
      Option Index Call Short, Option Index Put Short,
      Option Stock Call Long, Option Stock Put Long,
      Option Stock Call Short, Option Stock Put Short,
      Total Long Contracts, Total Short Contracts
    """
    lines = [l for l in csv_text.strip().splitlines() if l.strip()]
    if len(lines) < 3:
        raise ValueError(f"CSV too short: {len(lines)} lines")

    # Skip title row, parse from header row
    from io import StringIO
    df = pd.read_csv(StringIO("\n".join(lines[1:])), skipinitialspace=True)

    # Normalise column names
    df.columns = [c.strip() for c in df.columns]

    # Filter to known entities only
    df = df[df["Client Type"].isin(ENTITIES)].copy()
    df = df.reset_index(drop=True)

    def col(name):
        """Find column by partial match — NSE adds trailing spaces."""
        matches = [c for c in df.columns if name.lower() in c.lower()]
        return matches[0] if matches else None

    def int_col(name):
        c = col(name)
        if c is None:
            return pd.Series([0] * len(df))
        return pd.to_numeric(df[c].astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int)

    result = pd.DataFrame({
        "date":                 trade_date,
        "entity":               df["Client Type"].str.strip(),

        "fut_index_long":       int_col("Future Index Long"),
        "fut_index_short":      int_col("Future Index Short"),

        "fut_stock_long":       int_col("Future Stock Long"),
        "fut_stock_short":      int_col("Future Stock Short"),

        "opt_index_call_long":  int_col("Option Index Call Long"),
        "opt_index_call_short": int_col("Option Index Call Short"),

        "opt_index_put_long":   int_col("Option Index Put Long"),
        "opt_index_put_short":  int_col("Option Index Put Short"),

        "opt_stock_call_long":  int_col("Option Stock Call Long"),
        "opt_stock_call_short": int_col("Option Stock Call Short"),

        "opt_stock_put_long":   int_col("Option Stock Put Long"),
        "opt_stock_put_short":  int_col("Option Stock Put Short"),

        "total_long":           int_col("Total Long Contracts"),
        "total_short":          int_col("Total Short Contracts"),
    })

    # Pre-compute net columns
    result["fut_index_net"]       = result["fut_index_long"]       - result["fut_index_short"]
    result["fut_stock_net"]       = result["fut_stock_long"]       - result["fut_stock_short"]
    result["opt_index_call_net"]  = result["opt_index_call_long"]  - result["opt_index_call_short"]
    result["opt_index_put_net"]   = result["opt_index_put_long"]   - result["opt_index_put_short"]
    result["opt_stock_call_net"]  = result["opt_stock_call_long"]  - result["opt_stock_call_short"]
    result["opt_stock_put_net"]   = result["opt_stock_put_long"]   - result["opt_stock_put_short"]
    result["total_net"]           = result["total_long"]           - result["total_short"]

    return result


# ══════════════════════════════════════════════════════
# MinIO staging
# ══════════════════════════════════════════════════════

def minio_path(trade_date: date) -> str:
    return f"participant_oi/daily/{trade_date.strftime('%Y-%m-%d')}/participant_oi.parquet"


def save_to_minio(minio: Minio, df: pd.DataFrame, trade_date: date) -> str:
    cols = [f.name for f in PARQUET_SCHEMA]
    table  = pa.Table.from_pandas(df[cols], schema=PARQUET_SCHEMA, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    size     = len(buffer.getvalue())
    obj_path = minio_path(trade_date)
    minio.put_object(MINIO_BUCKET, obj_path, buffer, size,
                     content_type="application/octet-stream")
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


# ══════════════════════════════════════════════════════
# staging.file_registry
# ══════════════════════════════════════════════════════

def register_staged(ch, trade_date: date, obj_path: str, row_count: int):
    ch.insert(
        "staging.file_registry",
        [["participant_oi_pipeline", obj_path, trade_date, "staged", row_count, None, ""]],
        column_names=["agent_name", "file_path", "file_date",
                      "status", "row_count", "loaded_at", "error_message"]
    )


def mark_loaded(ch, obj_path: str):
    ch.command(
        "ALTER TABLE staging.file_registry UPDATE "
        "status = 'loaded', loaded_at = now() "
        "WHERE agent_name = {agent:String} AND file_path = {path:String} "
        "SETTINGS mutations_sync = 0",
        parameters={"agent": "participant_oi_pipeline", "path": obj_path}
    )


def mark_failed(ch, obj_path: str, error: str):
    ch.command(
        "ALTER TABLE staging.file_registry UPDATE "
        "status = 'failed', error_message = {err:String} "
        "WHERE agent_name = {agent:String} AND file_path = {path:String} "
        "SETTINGS mutations_sync = 0",
        parameters={"agent": "participant_oi_pipeline", "path": obj_path, "err": error[:500]}
    )


# ══════════════════════════════════════════════════════
# ClickHouse insert
# ══════════════════════════════════════════════════════

def insert_to_clickhouse(ch, df: pd.DataFrame):
    if df.empty:
        return
    df = df.copy()
    df["version"] = int(datetime.now().timestamp())
    cols = [f.name for f in PARQUET_SCHEMA] + ["version"]
    ch.insert_df("market.participant_oi", df[cols])


# ══════════════════════════════════════════════════════
# Per-date worker
# ══════════════════════════════════════════════════════

def process_date(trade_date: date,
                 minio: Minio,
                 staged_dates: set[date],
                 loaded_dates: set[date],
                 idx: int,
                 total: int):
    ch = get_ch_client()

    # Already in ClickHouse
    if trade_date in loaded_dates:
        with results_lock:
            results["skipped"] += 1
        return

    # Already staged in MinIO — load directly
    if trade_date in staged_dates:
        try:
            df = load_from_minio(minio, trade_date)
            if df.empty:
                log.warning(f"  [{idx}/{total}] {trade_date}: staged file missing in MinIO")
            else:
                insert_to_clickhouse(ch, df)
                mark_loaded(ch, minio_path(trade_date))
                with results_lock:
                    results["loaded"] += 1
                log.info(f"  [{idx}/{total}] {trade_date}: loaded from MinIO ({len(df)} rows)")
                return
        except Exception as e:
            log.error(f"  [{idx}/{total}] {trade_date}: MinIO load failed — {e}")

    # Fetch from NSE archives
    obj_path = minio_path(trade_date)
    try:
        csv_text = fetch_csv_for_date(trade_date)
        time.sleep(REQUEST_DELAY)

        if csv_text is None:
            # 404 = weekend or holiday — expected, not an error
            with results_lock:
                results["skipped"] += 1
            log.debug(f"  [{idx}/{total}] {trade_date}: no data (weekend/holiday)")
            return

        df = parse_csv(csv_text, trade_date)

        # Stage to MinIO
        save_to_minio(minio, df, trade_date)
        register_staged(ch, trade_date, obj_path, len(df))

        # Load to ClickHouse
        insert_to_clickhouse(ch, df)
        mark_loaded(ch, obj_path)

        with results_lock:
            results["staged"] += 1
            results["loaded"] += 1

        log.info(
            f"  [{idx}/{total}] {trade_date}: "
            f"MinIO ✅ + ClickHouse ✅ ({len(df)} rows)"
        )

    except Exception as e:
        log.error(f"  [{idx}/{total}] {trade_date}: FAILED — {e}")
        try:
            mark_failed(ch, obj_path, str(e))
        except Exception:
            pass
        with results_lock:
            results["failed"].append(f"{trade_date}: {e}")


# ══════════════════════════════════════════════════════
# Status display
# ══════════════════════════════════════════════════════

def show_status(ch):
    summary = ch.query(
        "SELECT entity, count() as days, min(date), max(date), "
        "       sum(fut_index_net) as net_fut_idx "
        "FROM market.participant_oi FINAL "
        "GROUP BY entity ORDER BY entity"
    ).result_rows

    print("\n── market.participant_oi — Summary ─────────────────────")
    print(f"{'Entity':<8} {'Days':>6} {'From':<12} {'To':<12} {'Net FutIdx':>12}")
    print("-" * 54)
    for r in summary:
        print(f"{r[0]:<8} {r[1]:>6} {str(r[2]):<12} {str(r[3]):<12} {r[4]:>12,}")

    recent = ch.query(
        "SELECT date, entity, fut_index_net, opt_index_call_net, "
        "       opt_index_put_net, total_net "
        "FROM market.participant_oi FINAL "
        "WHERE entity IN ('FII','DII') "
        "ORDER BY date DESC, entity LIMIT 6"
    ).result_rows

    print("\n── Last 3 trading days (FII + DII) ─────────────────────")
    print(f"{'Date':<12} {'E':<6} {'FutIdxNet':>10} {'CallNet':>10} {'PutNet':>10} {'TotalNet':>10}")
    print("-" * 62)
    for r in recent:
        print(f"{str(r[0]):<12} {r[1]:<6} {r[2]:>10,} {r[3]:>10,} {r[4]:>10,} {r[5]:>10,}")
    print()


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Participant OI Pipeline — NSE archives → MinIO → ClickHouse"
    )
    parser.add_argument("--full",      action="store_true",
                        help=f"Full backfill ({LOOKBACK_DAYS} days)")
    parser.add_argument("--from",      dest="from_date", type=str, default=None,
                        metavar="YYYY-MM-DD")
    parser.add_argument("--date",      dest="single_date", type=str, default=None,
                        metavar="YYYY-MM-DD", help="Process single date (debug)")
    parser.add_argument("--load-only", action="store_true",
                        help="Skip NSE fetch; reload staged MinIO files → ClickHouse")
    parser.add_argument("--status",    action="store_true")
    args = parser.parse_args()

    log.info("=== Participant OI Pipeline Starting ===")
    log.info(f"Started : {datetime.now()}")

    ch    = get_ch_client()
    minio = get_minio_client()
    log.info("ClickHouse connected ✅")

    assert_tables_exist(ch)
    setup_minio_bucket(minio)

    if args.status:
        show_status(ch)
        return

    today = date.today()

    # ── Single date debug ──────────────────────────────
    if args.single_date:
        d = date.fromisoformat(args.single_date)
        log.info(f"Single date mode: {d}")
        process_date(d, minio, set(), set(), 1, 1)
        _print_summary()
        return

    # ── Load-only mode ─────────────────────────────────
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
                log.info(f"  [{i}/{len(pending)}] {d}: loaded ✅")
                with results_lock:
                    results["loaded"] += 1
            except Exception as e:
                log.error(f"  {d}: FAILED — {e}")
                with results_lock:
                    results["failed"].append(f"{d}: {e}")
        _print_summary()
        return

    # ── Determine date range ───────────────────────────
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

    if fetch_from > today:
        log.info("Already up to date.")
        return

    # Build list of all calendar dates in range
    all_dates = []
    d = fetch_from
    while d <= today:
        all_dates.append(d)
        d += timedelta(days=1)

    total = len(all_dates)
    log.info(f"Date range : {fetch_from} → {today} ({total} calendar days)")
    log.info(f"Staged     : {len(staged_dates)} | Loaded: {len(loaded_dates)}")
    log.info(f"Workers    : {MAX_WORKERS}")

    def _worker(item):
        idx, trade_date = item
        process_date(trade_date, minio, staged_dates, loaded_dates, idx, total)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(_worker, enumerate(all_dates, 1))

    _print_summary()
    if results["loaded"] > 0:
        show_status(ch)
    log.info(f"Finished : {datetime.now()}")


def _print_summary():
    print("\n" + "=" * 60)
    print("PARTICIPANT OI PIPELINE — SUMMARY")
    print("=" * 60)
    print(f"📦 Staged to MinIO  : {results['staged']} dates")
    print(f"✅ Loaded to CH     : {results['loaded']} dates")
    print(f"⏭️  Skipped           : {results['skipped']} dates (weekends/holidays/up-to-date)")
    print(f"❌ Failed            : {len(results['failed'])}")
    if results["failed"]:
        for f in results["failed"][:15]:
            print(f"   {f}")
    print("=" * 60)


if __name__ == "__main__":
    main()