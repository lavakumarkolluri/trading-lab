import yfinance as yf
import pandas as pd
from minio import Minio
import clickhouse_connect
import io
import os
import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from symbols import MARKETS

# ── Logging Setup ──────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────
MINIO_HOST     = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER     = os.getenv("MINIO_USER", "admin")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "password123")
MINIO_BUCKET   = "trading-data"

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

MAX_WORKERS             = 8    # parallel download threads
DELAY_BETWEEN_DOWNLOADS = 0.3  # per-worker delay

# ── Results Tracker ────────────────────────────────────
results      = {"success": [], "skipped": [], "failed": []}
results_lock = threading.Lock()


# ── Clients ────────────────────────────────────────────
def get_minio_client():
    return Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )


def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASS
    )


def setup_minio(minio):
    if not minio.bucket_exists(MINIO_BUCKET):
        minio.make_bucket(MINIO_BUCKET)
        log.info(f"Created bucket: {MINIO_BUCKET}")
    else:
        log.info(f"Bucket exists: {MINIO_BUCKET}")


# ── Bulk fetch all watermarks in ONE query ─────────────
# Instead of querying ClickHouse once per symbol (724 round-trips),
# we fetch max(date) for every symbol in a single query at startup.
def fetch_all_watermarks(ch) -> dict:
    result = ch.query(
        "SELECT symbol, market, max(date) "
        "FROM market.ohlcv_daily "
        "GROUP BY symbol, market"
    )
    return {
        (row[0], row[1]): row[2]
        for row in result.result_rows
    }


# ── Download Only New Data ─────────────────────────────
def download_symbol(last_date, symbol, market):
    ticker = yf.Ticker(symbol)

    if last_date is None:
        log.info(f"  First load, downloading full history...")
        df = ticker.history(period="max", interval="1d")
    else:
        today = datetime.now().date()
        if last_date >= today:
            log.info(f"  Already up to date (last: {last_date}), skipping")
            return None
        from_date = last_date.strftime("%Y-%m-%d")
        log.info(f"  Last date in DB: {from_date}, downloading new rows only...")
        df = ticker.history(start=from_date, interval="1d")

    if df.empty:
        raise ValueError(f"No data returned for {symbol}")

    df = df.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
    df["Date"] = df["Date"].dt.date

    # Filter pre-1970 dates (ClickHouse Date type limitation)
    df = df[df["Date"] >= pd.Timestamp("1970-01-01").date()]

    # Filter rows already in DB (avoid duplicates on overlap)
    if last_date is not None:
        df = df[df["Date"] > last_date]

    if df.empty:
        log.info(f"  No new rows found, skipping")
        return None

    df["symbol"] = symbol
    df["market"] = market
    df.columns   = ["date", "open", "high", "low",
                    "close", "volume", "symbol", "market"]
    df["volume"]  = df["volume"].fillna(0).astype("int64")
    log.info(f"  Got {len(df)} new rows")
    return df


# ── Save to MinIO ──────────────────────────────────────
def save_to_minio(minio, df, symbol, market):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    size        = len(buffer.getvalue())
    today       = datetime.now().strftime("%Y-%m-%d")
    safe_symbol = symbol.replace("=", "_").replace("&", "_")
    object_path = f"{market}/daily/{today}/{safe_symbol}.parquet"
    minio.put_object(
        MINIO_BUCKET, object_path,
        buffer, size,
        content_type="application/octet-stream"
    )
    log.info(f"  Saved to MinIO: {object_path}")


# ── Save to ClickHouse ─────────────────────────────────
def save_to_clickhouse(ch, df):
    ch.insert_df("market.ohlcv_daily", df[[
        "date", "symbol", "market",
        "open", "high", "low", "close", "volume"
    ]])
    log.info(f"  Inserted {len(df)} rows into ClickHouse")


# ── Process One Symbol ─────────────────────────────────
def process_symbol(symbol, market, minio, ch, last_date):
    try:
        log.info(f"  Checking {symbol}...")
        df = download_symbol(last_date, symbol, market)

        if df is None:
            with results_lock:
                results["skipped"].append(f"{market}/{symbol}")
            return

        save_to_minio(minio, df, symbol, market)
        save_to_clickhouse(ch, df)
        with results_lock:
            results["success"].append(f"{market}/{symbol} (+{len(df)} rows)")

    except Exception as e:
        log.error(f"  FAILED {symbol}: {e}")
        with results_lock:
            results["failed"].append(f"{market}/{symbol} → {e}")


# ── Print Summary ──────────────────────────────────────
def print_summary():
    print("\n" + "=" * 55)
    print("PIPELINE SUMMARY")
    print("=" * 55)

    print(f"\n✅ Loaded:   {len(results['success'])}")
    for s in results["success"]:
        print(f"   {s}")

    print(f"\n⏭️  Skipped:  {len(results['skipped'])} (already up to date)")

    print(f"\n❌ Failed:   {len(results['failed'])}")
    for f in results["failed"]:
        print(f"   {f}")

    print("=" * 55)


# ── Worker (one per thread) ────────────────────────────
def _worker(args):
    symbol, market, total, idx, last_date = args
    # Each thread gets its own clients — not thread-safe to share
    ch    = get_ch_client()
    minio = get_minio_client()
    log.info(f"[{idx}/{total}] {market}/{symbol}")
    process_symbol(symbol, market, minio, ch, last_date)
    time.sleep(DELAY_BETWEEN_DOWNLOADS)


# ── Main ───────────────────────────────────────────────
def main():
    log.info("=== Trading Pipeline Starting ===")
    log.info(f"Start time: {datetime.now()}")

    # Setup MinIO bucket (serial, runs once)
    setup_minio(get_minio_client())

    # Fetch all existing watermarks in ONE query — eliminates N+1 round-trips
    ch = get_ch_client()
    log.info("Fetching watermarks from ClickHouse...")
    watermarks = fetch_all_watermarks(ch)
    log.info(f"Got watermarks for {len(watermarks)} existing symbols")

    # Build flat list of all symbols with their watermark date
    all_symbols = [
        (symbol, market)
        for market, symbols in MARKETS.items()
        for symbol in symbols
    ]
    total = len(all_symbols)
    log.info(f"Total symbols to check: {total}")
    log.info(f"Running with {MAX_WORKERS} parallel workers")

    tasks = [
        (symbol, market, total, idx, watermarks.get((symbol, market)))
        for idx, (symbol, market) in enumerate(all_symbols, 1)
    ]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(_worker, tasks)

    print_summary()
    log.info(f"End time: {datetime.now()}")


if __name__ == "__main__":
    main()