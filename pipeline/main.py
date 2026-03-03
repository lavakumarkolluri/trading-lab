import yfinance as yf
import pandas as pd
from minio import Minio
import clickhouse_connect
import io
import os
import logging
import time
from datetime import datetime

# ── Logging Setup ──────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Watchlist ──────────────────────────────────────────
MARKETS = {
    "indian": [
        "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
        "HINDUNILVR.NS", "ITC.NS", "SBIN.NS", "BHARTIARTL.NS", "KOTAKBANK.NS",
        "LT.NS", "AXISBANK.NS", "ASIANPAINT.NS", "MARUTI.NS", "TITAN.NS",
        "SUNPHARMA.NS", "ULTRACEMCO.NS", "WIPRO.NS", "HCLTECH.NS", "BAJFINANCE.NS",
        "NESTLEIND.NS", "TECHM.NS", "POWERGRID.NS", "NTPC.NS", "ONGC.NS",
        "TATASTEEL.NS", "JSWSTEEL.NS", "ADANIENT.NS", "ADANIPORTS.NS",
        "COALINDIA.NS", "DIVISLAB.NS", "DRREDDY.NS", "EICHERMOT.NS", "GRASIM.NS",
        "HEROMOTOCO.NS", "HINDALCO.NS", "INDUSINDBK.NS", "M&M.NS", "BAJAJFINSV.NS",
        "BAJAJ-AUTO.NS", "BRITANNIA.NS", "CIPLA.NS", "SBILIFE.NS", "HDFCLIFE.NS",
        "APOLLOHOSP.NS", "BPCL.NS", "TATACONSUM.NS", "UPL.NS", "NIFTYBEES.NS"
    ],
    "us": [
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
        "META", "TSLA", "BRK-B", "JPM", "JNJ",
        "V", "UNH", "XOM", "PG", "MA",
        "HD", "CVX", "MRK", "ABBV", "PFE",
        "BAC", "KO", "AVGO", "PEP", "TMO",
        "COST", "MCD", "ACN", "ABT", "CSCO",
        "CRM", "DHR", "NEE", "LIN", "TXN",
        "WMT", "PM", "ORCL", "RTX", "QCOM",
        "HON", "AMGN", "IBM", "GS", "BLK",
        "CAT", "GE", "INTU", "AXP", "SPGI",
        "SPY", "QQQ", "DIA", "IWM", "VTI",
        "XLF", "XLK", "XLE", "XLV", "XLI"
    ],
    "crypto": [
        "BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD",
        "ADA-USD", "AVAX-USD", "DOGE-USD", "DOT-USD", "MATIC-USD",
        "LINK-USD", "UNI-USD", "ATOM-USD", "LTC-USD", "BCH-USD",
        "XLM-USD", "ALGO-USD", "VET-USD", "FIL-USD", "AAVE-USD"
    ],
    "forex": [
        "USDINR=X", "EURUSD=X", "GBPUSD=X", "JPYUSD=X",
        "AUDUSD=X", "CADUSD=X", "CHFUSD=X", "CNYUSD=X"
    ]
}

# ── Config ─────────────────────────────────────────────
MINIO_HOST     = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER     = os.getenv("MINIO_USER", "admin")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "password123")
MINIO_BUCKET   = "trading-data"

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

DELAY_BETWEEN_DOWNLOADS = 2

# ── Results Tracker ────────────────────────────────────
results = {"success": [], "skipped": [], "failed": []}


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


# ── Get Last Loaded Date ───────────────────────────────
def get_last_date(ch, symbol, market):
    result = ch.query(f"""
        SELECT max(date) FROM market.ohlcv_daily
        WHERE symbol = '{symbol}' AND market = '{market}'
    """)
    last_date = result.result_rows[0][0]
    return last_date  # None if symbol not in DB yet


# ── Download Only New Data ─────────────────────────────
def download_symbol(ch, symbol, market):
    last_date = get_last_date(ch, symbol, market)
    ticker    = yf.Ticker(symbol)

    if last_date is None:
        # First time — download full history
        log.info(f"  First load, downloading full history...")
        df = ticker.history(period="max", interval="1d")
    else:
        today = datetime.now().date()
        if last_date >= today:
            log.info(f"  Already up to date (last: {last_date}), skipping")
            return None
        # Incremental — download from last date
        from_date = last_date.strftime("%Y-%m-%d")
        log.info(f"  Last date in DB: {from_date}, downloading new rows only...")
        df = ticker.history(start=from_date, interval="1d")

    if df.empty:
        raise ValueError(f"No data returned for {symbol}")

    df = df.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
    df["Date"] = df["Date"].dt.date

    # Filter pre-1970 (ClickHouse Date type limitation)
    df = df[df["Date"] >= pd.Timestamp("1970-01-01").date()]

    # Filter rows already in DB
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
    object_path = f"{market}/daily/{today}/{symbol}.parquet"
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
def process_symbol(symbol, market, minio, ch):
    try:
        log.info(f"  Checking {symbol}...")
        df = download_symbol(ch, symbol, market)

        if df is None:
            results["skipped"].append(f"{market}/{symbol}")
            return

        save_to_minio(minio, df, symbol, market)
        save_to_clickhouse(ch, df)
        results["success"].append(f"{market}/{symbol} (+{len(df)} rows)")

    except Exception as e:
        log.error(f"  FAILED {symbol}: {e}")
        results["failed"].append(f"{market}/{symbol} → {e}")


# ── Print Summary ──────────────────────────────────────
def print_summary():
    print("\n" + "=" * 55)
    print("PIPELINE SUMMARY")
    print("=" * 55)
    print(f"✅ Loaded:   {len(results['success'])}")
    for s in results["success"]:
        print(f"   {s}")
    print(f"\n⏭️  Skipped:  {len(results['skipped'])} (already up to date)")
    print(f"\n❌ Failed:   {len(results['failed'])}")
    for f in results["failed"]:
        print(f"   {f}")
    print("=" * 55)


# ── Main ───────────────────────────────────────────────
def main():
    log.info("=== Trading Pipeline Starting ===")
    log.info(f"Start time: {datetime.now()}")

    minio = get_minio_client()
    ch    = get_ch_client()

    setup_minio(minio)

    total = sum(len(v) for v in MARKETS.values())
    log.info(f"Total symbols to check: {total}")

    for market, symbols in MARKETS.items():
        log.info(f"\n── {market.upper()} ──────────────────────")
        for i, symbol in enumerate(symbols, 1):
            log.info(f"[{i}/{len(symbols)}] {symbol}")
            process_symbol(symbol, market, minio, ch)
            time.sleep(DELAY_BETWEEN_DOWNLOADS)

    print_summary()
    log.info(f"End time: {datetime.now()}")


if __name__ == "__main__":
    main()