import yfinance as yf
import pandas as pd
from minio import Minio
import clickhouse_connect
import io

print("=== Trading Pipeline Starting ===")

# ── Step 1: Download Data ──────────────────────────────
print("Downloading AAPL data from Yahoo Finance...")
ticker = yf.Ticker("AAPL")
df = ticker.history(period="1y", interval="1d")
df = df.reset_index()[["Date","Open","High","Low","Close","Volume"]]
df["Symbol"] = "AAPL"
df["Date"] = df["Date"].dt.date
print(f"Got {len(df)} rows")

# ── Step 2: Save to MinIO ──────────────────────────────
print("Uploading to MinIO...")
minio_client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)

bucket = "trading-data"
if not minio_client.bucket_exists(bucket):
    minio_client.make_bucket(bucket)

parquet_buffer = io.BytesIO()
df.to_parquet(parquet_buffer, index=False)
parquet_buffer.seek(0)

minio_client.put_object(
    bucket, "stocks/AAPL/daily.parquet",
    parquet_buffer, len(parquet_buffer.getvalue()),
    content_type="application/octet-stream"
)
print("Uploaded to MinIO successfully")

# ── Step 3: Load into ClickHouse ───────────────────────
print("Loading into ClickHouse...")
ch = clickhouse_connect.get_client(host="clickhouse", port=8123)

ch.command("""
    CREATE DATABASE IF NOT EXISTS trades
""")

ch.command("""
    CREATE TABLE IF NOT EXISTS trades.ohlcv (
        date       Date,
        open       Float64,
        high       Float64,
        low        Float64,
        close      Float64,
        volume     UInt64,
        symbol     String
    ) ENGINE = MergeTree()
    ORDER BY (symbol, date)
""")

ch.insert_df("trades.ohlcv", df.rename(columns={
    "Date":"date","Open":"open","High":"high",
    "Low":"low","Close":"close","Volume":"volume","Symbol":"symbol"
}))

print("Loaded into ClickHouse successfully")
print("=== Pipeline Complete! ===")
