#!/usr/bin/env python3
"""
fii_dii_pipeline.py
───────────────────
Downloads FII/DII daily trade data from NSE India.

Data flow (mirrors ohlcv_daily pipeline pattern):
  NSE API → MinIO staging (Parquet) → staging.file_registry → market.fii_dii

MinIO path:
  fii_dii/daily/{YYYY-MM-DD}/fii_dii.parquet
  One file per calendar date (contains both FII and DII rows).

Why MinIO first:
  • Raw data preserved even if ClickHouse insert fails
  • Re-runnable: skips dates already staged (file_registry check)
  • Consistent with ohlcv_daily architecture
  • Immutable audit trail of exactly what NSE returned

Table: market.fii_dii (ReplacingMergeTree)
  date, entity, buy_value, sell_value, net_value, version
  Values in ₹ crores (as reported by NSE — no unit conversion)

Entities: 'FII' (maps from NSE's 'FII/FPI') and 'DII'

Watermark logic:
  • fetch_staged_dates() — ONE query to staging.file_registry
  • fetch_loaded_dates() — ONE query to market.fii_dii
  • Per-date round-trips: zero

Usage:
  python fii_dii_pipeline.py               # incremental (default)
  python fii_dii_pipeline.py --full        # re-fetch last LOOKBACK_DAYS
  python fii_dii_pipeline.py --load-only   # reload staged MinIO files → ClickHouse
  python fii_dii_pipeline.py --from 2024-01-01
  python fii_dii_pipeline.py --status      # show DB stats and exit
"""

import os
import sys
import io
import time
import logging
import argparse
import threading
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import clickhouse_connect
from minio import Minio
from minio.error import S3Error
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
MINIO_PASS   = os.getenv("MINIO_PASSWORD", "password123")
MINIO_BUCKET = "trading-data"

NSE_HOME_URL = "https://www.nseindia.com"
NSE_FII_URL  = "https://www.nseindia.com/api/fiidiiTradeReact"

LOOKBACK_DAYS   = 730   # history on first run (~2 years)
CHUNK_DAYS      = 30    # days per NSE API request
MAX_WORKERS     = 4     # parallel chunks — keep low for NSE rate limits
REQUEST_DELAY   = 1.5   # seconds between requests per worker
SESSION_TIMEOUT = 20    # HTTP timeout — NSE can be slow

# Canonical entity names — stable contract for all downstream consumers
# Never change these without updating pattern_feature_extractor and confidence scorer
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

# ── Results tracker ────────────────────────────────────
results = {
    "staged":       0,
    "loaded":       0,
    "skipped":      0,
    "empty":        0,
    "failed":       [],
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
    log.info("Tables verified ✅")


def setup_minio_bucket(minio):
    if not minio.bucket_exists(MINIO_BUCKET):
        minio.make_bucket(MINIO_BUCKET)
        log.info(f"Created bucket: {MINIO_BUCKET}")


# ══════════════════════════════════════════════════════
# Watermarks — ONE query each, no per-date round-trips
# ══════════════════════════════════════════════════════

def fetch_staged_dates(ch) -> set[date]:
    """Dates already written to MinIO (status='loaded' in file_registry)."""
    result = ch.query(
        "SELECT DISTINCT toDate(file_date) "
        "FROM staging.file_registry "
        "WHERE agent_name = {agent:String} AND status = {s:String}",
        parameters={"agent": "fii_dii_pipeline", "s": "loaded"}
    )
    return {row[0] for row in result.result_rows}


def fetch_loaded_dates(ch) -> set[date]:
    """Dates already in market.fii_dii."""
    result = ch.query("SELECT DISTINCT date FROM market.fii_dii FINAL")
    return {row[0] for row in result.result_rows}


# ══════════════════════════════════════════════════════
# NSE session
# ══════════════════════════════════════════════════════

def build_nse_session() -> requests.Session:
    """
    Bootstrap NSE session to pass WAF checks.

    NSE's WAF validates:
      1. User-Agent looks like a real Chrome browser
      2. sec-ch-ua / sec-fetch-* headers are present (browsers always send these)
      3. Cookies are set by visiting the homepage BEFORE any API call
      4. A second warm-up page visit before the data API call

    Two-step warm-up:
      Step 1 — GET https://www.nseindia.com          (sets _ga, nsit, nseappid cookies)
      Step 2 — GET https://www.nseindia.com/market-data/equity-stock-indices-futures
                                                       (sets additional session cookies)
      Step 3 — API calls now accepted

    Raises RuntimeError if either warm-up step returns non-2xx.
    """
    session = requests.Session()

    # Full Chrome 122 header set — NSE WAF checks for sec-ch-ua and sec-fetch-*
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "sec-ch-ua":                 '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "sec-ch-ua-mobile":          "?0",
        "sec-ch-ua-platform":        '"Linux"',
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
        "Sec-Fetch-User":            "?1",
        "DNT":                       "1",
        "Cache-Control":             "max-age=0",
    })

    # Step 1 — Homepage (sets base cookies)
    log.info("NSE session: step 1 — homepage...")
    try:
        resp = session.get(NSE_HOME_URL, timeout=SESSION_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"NSE homepage blocked (step 1): {e}") from e
    log.info(f"  Homepage OK — cookies: {len(session.cookies)}")
    time.sleep(2.0)

    # Step 2 — FII/DII market data page (sets additional session cookies)
    # Switch headers to look like a same-site navigation (not a fresh tab)
    session.headers.update({
        "Referer":        "https://www.nseindia.com/",
        "Sec-Fetch-Site": "same-origin",
    })
    warmup_url = "https://www.nseindia.com/market-data/fii-dii-activity"
    log.info("NSE session: step 2 — FII/DII market page warm-up...")
    try:
        resp = session.get(warmup_url, timeout=SESSION_TIMEOUT, allow_redirects=True)
        # 200 or 403 both OK here — we just want the cookies from the response
        log.info(f"  Warm-up page status: {resp.status_code} — cookies: {len(session.cookies)}")
    except Exception as e:
        log.warning(f"  Warm-up page failed (non-fatal): {e}")
    time.sleep(2.0)

    # Switch to JSON API headers for all subsequent calls
    session.headers.update({
        "Accept":         "application/json, text/plain, */*",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Referer":        "https://www.nseindia.com/market-data/fii-dii-activity",
        "X-Requested-With": "XMLHttpRequest",
    })

    log.info(f"NSE session ready ✅ (total cookies: {len(session.cookies)})")
    return session


# ══════════════════════════════════════════════════════
# NSE fetch — one date chunk
# ══════════════════════════════════════════════════════

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=2, max=20),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True
)
def fetch_from_nse(session: requests.Session,
                   from_date: date,
                   to_date: date) -> list[dict]:
    """
    Fetch FII/DII for one date window from NSE.
    Returns [] for windows that fall entirely on holidays/weekends.
    """
    url = (
        f"{NSE_FII_URL}"
        f"?&fromDate={from_date.strftime('%d-%b-%Y')}"
        f"&toDate={to_date.strftime('%d-%b-%Y')}"
    )
    resp = session.get(url, timeout=SESSION_TIMEOUT)
    resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, list):
        return []

    rows = []
    for item in data:
        try:
            raw_date = item.get("date", "")
            if not raw_date:
                continue
            try:
                trade_date = datetime.strptime(raw_date, "%d-%b-%Y").date()
            except ValueError:
                trade_date = datetime.strptime(raw_date, "%d-%B-%Y").date()

            raw_entity = item.get("category", item.get("entity", "")).strip().upper()
            entity     = ENTITY_MAP.get(raw_entity)
            if entity is None:
                log.debug(f"Unknown entity {raw_entity!r} — skipping row")
                continue

            rows.append({
                "date":       trade_date,
                "entity":     entity,
                "buy_value":  _parse_float(item.get("buyValue",  item.get("buySell", 0))),
                "sell_value": _parse_float(item.get("sellValue", 0)),
                "net_value":  _parse_float(item.get("netValue",  item.get("net", 0))),
            })
        except Exception as e:
            log.warning(f"Malformed NSE row skipped: {item!r} — {e}")

    return rows


def _parse_float(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(str(val).replace(",", "").strip() or 0)
    except (ValueError, TypeError):
        return 0.0


# ══════════════════════════════════════════════════════
# MinIO — save / load
# ══════════════════════════════════════════════════════

def minio_path(trade_date: date) -> str:
    return f"fii_dii/daily/{trade_date.strftime('%Y-%m-%d')}/fii_dii.parquet"


def save_to_minio(minio: Minio, rows: list[dict], trade_date: date) -> str:
    """Serialise rows to Parquet and upload. Returns object path."""
    df = pd.DataFrame(rows)[["date", "entity", "buy_value", "sell_value", "net_value"]]
    df["date"] = pd.to_datetime(df["date"]).dt.date

    table  = pa.Table.from_pandas(df, schema=PARQUET_SCHEMA, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    size = len(buffer.getvalue())

    obj_path = minio_path(trade_date)
    minio.put_object(
        MINIO_BUCKET, obj_path, buffer, size,
        content_type="application/octet-stream"
    )
    return obj_path


def load_from_minio(minio: Minio, trade_date: date) -> pd.DataFrame:
    """Read a staged Parquet file. Returns empty DataFrame if not found."""
    obj_path = minio_path(trade_date)
    try:
        response = minio.get_object(MINIO_BUCKET, obj_path)
        return pq.read_table(io.BytesIO(response.read())).to_pandas()
    except S3Error as e:
        if e.code == "NoSuchKey":
            return pd.DataFrame()
        raise


# ══════════════════════════════════════════════════════
# staging.file_registry helpers
# ══════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════
# ClickHouse insert
# ══════════════════════════════════════════════════════

def insert_to_clickhouse(ch, df: pd.DataFrame):
    if df.empty:
        return
    df = df.copy()
    df["version"] = int(datetime.now().timestamp())
    ch.insert_df(
        "market.fii_dii",
        df[["date", "entity", "buy_value", "sell_value", "net_value", "version"]]
    )


# ══════════════════════════════════════════════════════
# Per-chunk worker
# ══════════════════════════════════════════════════════

def process_chunk(session: requests.Session,
                  minio: Minio,
                  from_date: date,
                  to_date: date,
                  staged_dates: set[date],
                  loaded_dates: set[date],
                  idx: int,
                  total: int):
    ch = get_ch_client()

    # Classify each date in this window
    dates = []
    d = from_date
    while d <= to_date:
        dates.append(d)
        d += timedelta(days=1)

    already_loaded  = [d for d in dates if d in loaded_dates]
    already_staged  = [d for d in dates if d not in loaded_dates and d in staged_dates]
    needs_nse_fetch = [d for d in dates if d not in loaded_dates and d not in staged_dates]

    if already_loaded:
        with results_lock:
            results["skipped"] += len(already_loaded)

    # Load staged-but-not-yet-loaded dates directly from MinIO
    for d in already_staged:
        try:
            df = load_from_minio(minio, d)
            if df.empty:
                log.warning(f"  {d}: MinIO file missing — adding to NSE fetch queue")
                needs_nse_fetch.append(d)
                continue
            insert_to_clickhouse(ch, df)
            mark_loaded(ch, minio_path(d))
            with results_lock:
                results["loaded"] += 1
            log.info(f"  [{idx}/{total}] {d}: loaded from MinIO ({len(df)} rows)")
        except Exception as e:
            log.error(f"  [{idx}/{total}] {d}: MinIO load failed — {e}")
            with results_lock:
                results["failed"].append(f"{d} (MinIO load): {e}")

    if not needs_nse_fetch:
        return

    # Fetch from NSE for dates not yet staged
    fetch_from = min(needs_nse_fetch)
    fetch_to   = max(needs_nse_fetch)
    try:
        all_rows = fetch_from_nse(session, fetch_from, fetch_to)
        time.sleep(REQUEST_DELAY)
    except Exception as e:
        log.error(f"  [{idx}/{total}] NSE fetch {fetch_from}→{fetch_to} failed: {e}")
        with results_lock:
            results["failed"].append(f"{fetch_from}→{fetch_to} (NSE): {e}")
        return

    if not all_rows:
        with results_lock:
            results["empty"] += 1
        log.debug(f"  [{idx}/{total}] {fetch_from}→{fetch_to}: no data (weekends/holidays)")
        return

    # Group by date and process each
    rows_by_date: dict[date, list[dict]] = {}
    for row in all_rows:
        rows_by_date.setdefault(row["date"], []).append(row)

    for trade_date, date_rows in sorted(rows_by_date.items()):
        if trade_date not in needs_nse_fetch:
            continue
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
            log.info(
                f"  [{idx}/{total}] {trade_date}: "
                f"MinIO ✅ + ClickHouse ✅ ({len(date_rows)} rows)"
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
        "       round(sum(net_value), 2) as total_net_cr "
        "FROM market.fii_dii FINAL "
        "GROUP BY entity ORDER BY entity"
    ).result_rows

    print("\n── market.fii_dii — Summary ────────────────────────────")
    print(f"{'Entity':<6} {'Days':>6} {'From':<12} {'To':<12} {'Net ₹Cr':>14}")
    print("-" * 54)
    for r in summary:
        print(f"{r[0]:<6} {r[1]:>6} {str(r[2]):<12} {str(r[3]):<12} {r[4]:>14,.2f}")

    recent = ch.query(
        "SELECT date, entity, buy_value, sell_value, net_value "
        "FROM market.fii_dii FINAL "
        "ORDER BY date DESC, entity LIMIT 6"
    ).result_rows

    print("\n── Last 3 trading days ─────────────────────────────────")
    print(f"{'Date':<12} {'E':<5} {'Buy ₹Cr':>12} {'Sell ₹Cr':>12} {'Net ₹Cr':>12}")
    print("-" * 57)
    for r in recent:
        print(f"{str(r[0]):<12} {r[1]:<5} {r[2]:>12,.2f} {r[3]:>12,.2f} {r[4]:>12,.2f}")
    print()


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="FII/DII Pipeline — NSE → MinIO staging → ClickHouse"
    )
    parser.add_argument("--full",      action="store_true",
                        help=f"Full re-fetch ({LOOKBACK_DAYS} days)")
    parser.add_argument("--from",      dest="from_date", type=str, default=None,
                        metavar="YYYY-MM-DD")
    parser.add_argument("--load-only", action="store_true",
                        help="Skip NSE; reload staged MinIO files into ClickHouse only")
    parser.add_argument("--status",    action="store_true")
    args = parser.parse_args()

    log.info("=== FII/DII Pipeline Starting ===")
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

    # ── Load-only mode ─────────────────────────────────
    if args.load_only:
        log.info("Load-only mode — reloading staged MinIO files not yet in ClickHouse...")
        staged  = fetch_staged_dates(ch)
        loaded  = fetch_loaded_dates(ch)
        pending = sorted(staged - loaded)
        if not pending:
            log.info("Nothing to load — all staged dates already in ClickHouse.")
            return
        log.info(f"Dates pending load: {len(pending)}")
        for trade_date in pending:
            try:
                df = load_from_minio(minio, trade_date)
                if df.empty:
                    log.warning(f"  {trade_date}: file missing in MinIO")
                    continue
                insert_to_clickhouse(ch, df)
                mark_loaded(ch, minio_path(trade_date))
                log.info(f"  {trade_date}: loaded ({len(df)} rows) ✅")
                with results_lock:
                    results["loaded"] += 1
            except Exception as e:
                log.error(f"  {trade_date}: FAILED — {e}")
                with results_lock:
                    results["failed"].append(f"{trade_date}: {e}")
        _print_summary()
        return

    # ── Determine fetch window ─────────────────────────
    if args.full:
        fetch_from   = today - timedelta(days=LOOKBACK_DAYS)
        staged_dates = set()
        loaded_dates = set()
        log.info(f"Mode: FULL (from {fetch_from})")
    elif args.from_date:
        fetch_from   = date.fromisoformat(args.from_date)
        staged_dates = fetch_staged_dates(ch)
        loaded_dates = fetch_loaded_dates(ch)
        log.info(f"Mode: FROM {fetch_from}")
    else:
        staged_dates = fetch_staged_dates(ch)
        loaded_dates = fetch_loaded_dates(ch)
        covered      = staged_dates | loaded_dates
        if covered:
            fetch_from = max(covered) + timedelta(days=1)
            log.info(f"Mode: INCREMENTAL (from {fetch_from})")
        else:
            fetch_from = today - timedelta(days=LOOKBACK_DAYS)
            log.info(f"Mode: FIRST RUN (from {fetch_from})")

    log.info(f"Staged dates : {len(staged_dates)}")
    log.info(f"Loaded dates : {len(loaded_dates)}")

    if fetch_from > today:
        log.info("Already up to date.")
        return

    # ── Chunk and launch ───────────────────────────────
    chunks = []
    current = fetch_from
    while current <= today:
        end = min(current + timedelta(days=CHUNK_DAYS - 1), today)
        chunks.append((current, end))
        current = end + timedelta(days=1)

    total = len(chunks)
    log.info(f"Date range : {fetch_from} → {today}")
    log.info(f"Chunks     : {total} × {CHUNK_DAYS} days")
    log.info(f"Workers    : {MAX_WORKERS}")

    try:
        session = build_nse_session()
    except Exception as e:
        log.error(f"NSE session failed: {e}")
        sys.exit(1)

    def _worker(item):
        idx, (chunk_from, chunk_to) = item
        process_chunk(
            session, minio, chunk_from, chunk_to,
            staged_dates, loaded_dates, idx, total
        )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(_worker, enumerate(chunks, 1))

    _print_summary()
    if results["loaded"] > 0:
        show_status(ch)
    log.info(f"Finished : {datetime.now()}")


def _print_summary():
    print("\n" + "=" * 60)
    print("FII/DII PIPELINE — SUMMARY")
    print("=" * 60)
    print(f"📦 Staged to MinIO  : {results['staged']} dates")
    print(f"✅ Loaded to CH     : {results['loaded']} dates")
    print(f"⏭️  Skipped           : {results['skipped']} dates (already loaded)")
    print(f"🈳 Empty             : {results['empty']} chunks (weekends/holidays)")
    print(f"❌ Failed            : {len(results['failed'])}")
    if results["failed"]:
        for f in results["failed"][:15]:
            print(f"   {f}")
    print("=" * 60)


if __name__ == "__main__":
    main()