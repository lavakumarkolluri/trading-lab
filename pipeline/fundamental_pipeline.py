#!/usr/bin/env python3
"""
fundamental_pipeline.py
────────────────────────
Downloads yfinance fundamental data for all NSE-listed tickers.

Data flow: yfinance → MinIO (Parquet) → ClickHouse

MinIO paths:
  fundamentals/snapshots/{YYYY-MM-DD}.parquet  — daily valuation snapshot (all symbols)
  fundamentals/quarterly/{YYYY-MM-DD}.parquet  — quarterly financials (all symbols)

ClickHouse tables:
  market.fundamental_snapshot   — valuation ratios per symbol per date
  market.fundamental_quarterly  — quarterly income/balance/cashflow per symbol

Usage:
  python fundamental_pipeline.py           # incremental (skip if already loaded)
  python fundamental_pipeline.py --full    # re-fetch regardless
  python fundamental_pipeline.py --status  # show row counts

Docker:
  docker compose run --rm fundamental_pipeline
"""

import io
import os
import time
import logging
import argparse
from datetime import datetime, date

import pandas as pd
import yfinance as yf
import clickhouse_connect
from minio import Minio
from minio.error import S3Error

from symbols import MARKETS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

MINIO_HOST   = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER   = os.getenv("MINIO_USER", "admin")
MINIO_PASS   = os.getenv("MINIO_PASSWORD", "")
MINIO_BUCKET = "trading-data"

RATE_LIMIT_S = 0.3  # seconds between yfinance calls to avoid rate limiting

NSE_SYMBOLS = MARKETS["indian"]


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


def get_mc():
    return Minio(MINIO_HOST, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)


def setup_bucket(mc: Minio):
    if not mc.bucket_exists(MINIO_BUCKET):
        mc.make_bucket(MINIO_BUCKET)


def _get(d: dict, *keys, default: float = 0.0) -> float:
    for k in keys:
        v = d.get(k)
        if v is not None and v != "N/A":
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return default


def _safe_float(val) -> float:
    try:
        return float(val) if val is not None and pd.notna(val) else 0.0
    except Exception:
        return 0.0


def fetch_snapshot(symbol: str) -> dict | None:
    try:
        info = yf.Ticker(symbol).info
        if not info or not info.get("regularMarketPrice") and not info.get("currentPrice"):
            return None
        return {
            "symbol":             symbol.replace(".NS", ""),
            "date":               date.today(),
            "market_cap":         _get(info, "marketCap"),
            "pe_ratio":           _get(info, "trailingPE"),
            "forward_pe":         _get(info, "forwardPE"),
            "pb_ratio":           _get(info, "priceToBook"),
            "ev_ebitda":          _get(info, "enterpriseToEbitda"),
            "ev_revenue":         _get(info, "enterpriseToRevenue"),
            "eps_ttm":            _get(info, "trailingEps"),
            "book_value":         _get(info, "bookValue"),
            "revenue_ttm":        _get(info, "totalRevenue"),
            "gross_margins":      _get(info, "grossMargins"),
            "operating_margins":  _get(info, "operatingMargins"),
            "profit_margins":     _get(info, "profitMargins"),
            "roe":                _get(info, "returnOnEquity"),
            "roa":                _get(info, "returnOnAssets"),
            "free_cashflow":      _get(info, "freeCashflow"),
            "operating_cashflow": _get(info, "operatingCashflow"),
            "total_debt":         _get(info, "totalDebt"),
            "cash":               _get(info, "totalCash"),
            "dividend_yield":     _get(info, "dividendYield"),
        }
    except Exception as e:
        log.debug(f"{symbol}: snapshot fetch failed: {e}")
        return None


def fetch_quarterly(symbol: str, now: datetime, version: int) -> list[dict]:
    rows = []
    try:
        t   = yf.Ticker(symbol)
        sym = symbol.replace(".NS", "")

        fin = t.quarterly_financials
        bs  = t.quarterly_balance_sheet
        cf  = t.quarterly_cashflow

        if fin is None or fin.empty:
            return rows

        for period_ts in fin.columns:
            period_date = pd.Timestamp(period_ts).date()

            def _f(df, *keys) -> float:
                for k in keys:
                    if df is not None and not df.empty and k in df.index:
                        try:
                            return _safe_float(df.loc[k, period_ts])
                        except Exception:
                            pass
                return 0.0

            rows.append({
                "symbol":             sym,
                "period":             period_date,
                "period_type":        "quarterly",
                "revenue":            _f(fin, "Total Revenue"),
                "gross_profit":       _f(fin, "Gross Profit"),
                "operating_income":   _f(fin, "Operating Income", "EBIT"),
                "net_income":         _f(fin, "Net Income"),
                "ebitda":             _f(fin, "EBITDA"),
                "eps":                _f(fin, "Diluted EPS", "Basic EPS"),
                "total_assets":       _f(bs,  "Total Assets"),
                "total_debt":         _f(bs,  "Total Debt", "Long Term Debt"),
                "cash":               _f(bs,  "Cash And Cash Equivalents", "Cash"),
                "equity":             _f(bs,  "Stockholders Equity", "Total Stockholder Equity"),
                "operating_cashflow": _f(cf,  "Operating Cash Flow"),
                "capex":              _f(cf,  "Capital Expenditure"),
                "free_cashflow":      _f(cf,  "Free Cash Flow"),
                "computed_at":        now,
                "version":            version,
            })
    except Exception as e:
        log.debug(f"{symbol}: quarterly fetch failed: {e}")
    return rows


def save_to_minio(mc: Minio, df: pd.DataFrame, key: str):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    mc.put_object(MINIO_BUCKET, key, buf, buf.getbuffer().nbytes,
                  content_type="application/octet-stream")
    log.info(f"Saved {len(df)} rows → MinIO {key}")


def is_snapshot_loaded_today(ch) -> bool:
    try:
        r = ch.query("SELECT count() FROM market.fundamental_snapshot WHERE date = today()")
        return r.result_rows[0][0] > 0
    except Exception:
        return False


def is_quarterly_fresh(ch) -> bool:
    """True if quarterly data was computed within the last 30 days."""
    try:
        r = ch.query("SELECT max(toDate(computed_at)) FROM market.fundamental_quarterly")
        val = r.result_rows[0][0]
        return bool(val and val > date(2000, 1, 1) and (date.today() - val).days < 30)
    except Exception:
        return False


def show_status(ch):
    snap_r = ch.query("SELECT count(), max(date) FROM market.fundamental_snapshot FINAL")
    cnt, mx = snap_r.result_rows[0]
    print(f"  market.fundamental_snapshot  : {cnt:,} rows, latest date={mx}")

    qtr_r = ch.query("SELECT count(), max(period) FROM market.fundamental_quarterly FINAL")
    cnt, mx = qtr_r.result_rows[0]
    print(f"  market.fundamental_quarterly : {cnt:,} rows, latest period={mx}")


def main():
    parser = argparse.ArgumentParser(description="Fundamental data pipeline — yfinance → MinIO → ClickHouse")
    parser.add_argument("--full",    action="store_true", help="Re-fetch even if already loaded")
    parser.add_argument("--status",  action="store_true", help="Show ClickHouse row counts")
    parser.add_argument("--no-save", action="store_true", help="Dry run — skip ClickHouse insert")
    args = parser.parse_args()

    log.info("=== Fundamental Pipeline ===")
    ch = get_ch()
    mc = get_mc()
    setup_bucket(mc)

    if args.status:
        show_status(ch)
        return

    today_str = date.today().strftime("%Y-%m-%d")
    now       = datetime.utcnow()
    version   = int(now.timestamp())

    # ── Valuation snapshot (ticker.info) ──────────────────────────────────────
    snap_key = f"fundamentals/snapshots/{today_str}.parquet"

    if not args.full and is_snapshot_loaded_today(ch):
        log.info("Snapshot already loaded for today — use --full to re-fetch")
    else:
        log.info(f"Fetching snapshot for {len(NSE_SYMBOLS)} symbols...")
        snap_rows = []
        for i, sym in enumerate(NSE_SYMBOLS, 1):
            row = fetch_snapshot(sym)
            if row:
                snap_rows.append(row)
            if i % 25 == 0:
                log.info(f"  {i}/{len(NSE_SYMBOLS)} done  ({len(snap_rows)} fetched)")
            time.sleep(RATE_LIMIT_S)

        log.info(f"Snapshot: {len(snap_rows)}/{len(NSE_SYMBOLS)} symbols fetched")
        if snap_rows:
            snap_df = pd.DataFrame(snap_rows)
            snap_df["computed_at"] = now
            snap_df["version"]     = version
            save_to_minio(mc, snap_df, snap_key)
            if not args.no_save:
                ch.insert_df("market.fundamental_snapshot", snap_df)
                log.info(f"Inserted {len(snap_df)} rows → market.fundamental_snapshot")

    # ── Quarterly financials (income / balance / cashflow) ────────────────────
    qtr_key = f"fundamentals/quarterly/{today_str}.parquet"

    if not args.full and is_quarterly_fresh(ch):
        log.info("Quarterly data is fresh (< 30 days) — use --full to re-fetch")
    else:
        log.info(f"Fetching quarterly financials for {len(NSE_SYMBOLS)} symbols...")
        qtr_rows = []
        for i, sym in enumerate(NSE_SYMBOLS, 1):
            rows = fetch_quarterly(sym, now, version)
            qtr_rows.extend(rows)
            if i % 25 == 0:
                log.info(f"  {i}/{len(NSE_SYMBOLS)} done  ({len(qtr_rows)} rows so far)")
            time.sleep(RATE_LIMIT_S)

        log.info(f"Quarterly: {len(qtr_rows)} rows from {len(NSE_SYMBOLS)} symbols")
        if qtr_rows:
            qtr_df = pd.DataFrame(qtr_rows)
            save_to_minio(mc, qtr_df, qtr_key)
            if not args.no_save:
                ch.insert_df("market.fundamental_quarterly", qtr_df)
                log.info(f"Inserted {len(qtr_df)} rows → market.fundamental_quarterly")

    log.info("Fundamental pipeline complete ✅")


if __name__ == "__main__":
    main()
