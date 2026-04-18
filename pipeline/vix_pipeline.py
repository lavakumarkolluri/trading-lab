#!/usr/bin/env python3
"""
vix_pipeline.py
───────────────
Fetches India VIX and Nifty 50 spot daily from yfinance.

Data flow: yfinance → MinIO (Parquet) → ClickHouse
  MinIO path: trading-data/vix/vix_nifty.parquet  (full history, overwritten each run)

On rerun: existing parquet is loaded from MinIO; only dates missing from it
are fetched from yfinance, then the merged result is saved back.

Writes to:
  market.nifty_live       — one EOD row per trading date
  analysis.market_regime  — VIX-based regime classification per date

India VIX regime bands (NSE standard):
  < 13       → 'low'       complacency; trend-following, full position size
  13–18      → 'normal'    balanced; standard position sizing
  18–25      → 'elevated'  caution; reduce size, favour mean-reversion
  > 25       → 'extreme'   fear; buy-the-dip only with confirmation

Usage:
  python vix_pipeline.py             # incremental (since last loaded date)
  python vix_pipeline.py --full      # re-fetch last 2 years
  python vix_pipeline.py --days 90   # specific lookback window
  python vix_pipeline.py --load-only # reload MinIO parquet → ClickHouse (no yfinance)

Docker:
  docker compose run --rm vix_pipeline
"""

import io
import os
import logging
import argparse
from datetime import datetime, date, timedelta

import pandas as pd
import yfinance as yf
import clickhouse_connect
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

MINIO_HOST   = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER   = os.getenv("MINIO_USER", "admin")
MINIO_PASS   = os.getenv("MINIO_PASSWORD", "")
MINIO_BUCKET = "trading-data"
MINIO_KEY    = "vix/vix_nifty.parquet"

LOOKBACK_DAYS = 730
VIX_SYMBOL    = "^INDIAVIX"
NIFTY_SYMBOL  = "^NSEI"

_VIX_BANDS = [(13.0, "low"), (18.0, "normal"), (25.0, "elevated")]


# ── Clients ─────────────────────────────────────────────

def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


def get_minio_client() -> Minio:
    return Minio(MINIO_HOST, access_key=MINIO_USER,
                 secret_key=MINIO_PASS, secure=False)


def setup_minio_bucket(mc: Minio):
    if not mc.bucket_exists(MINIO_BUCKET):
        mc.make_bucket(MINIO_BUCKET)


# ── MinIO helpers ────────────────────────────────────────

def load_from_minio(mc: Minio) -> pd.DataFrame:
    """Load full VIX history parquet from MinIO. Returns empty DF on miss."""
    try:
        resp = mc.get_object(MINIO_BUCKET, MINIO_KEY)
        df = pd.read_parquet(io.BytesIO(resp.read()))
        resp.close()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        log.info(f"MinIO cache hit: {len(df)} rows "
                 f"({df['date'].min()} → {df['date'].max()})")
        return df
    except S3Error:
        log.info("MinIO cache miss — will fetch from yfinance")
        return pd.DataFrame(columns=["date", "vix", "nifty_spot"])


def save_to_minio(mc: Minio, df: pd.DataFrame):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    mc.put_object(MINIO_BUCKET, MINIO_KEY, buf, buf.getbuffer().nbytes,
                  content_type="application/octet-stream")
    log.info(f"Saved {len(df)} rows → MinIO {MINIO_KEY}")


# ── Regime helpers ───────────────────────────────────────

def classify_regime(vix: float) -> str:
    for threshold, label in _VIX_BANDS:
        if vix < threshold:
            return label
    return "extreme"


def compute_opportunity(regime: str, nifty_trend: str) -> tuple[int, int]:
    base = {"low": 75, "normal": 60, "elevated": 40, "extreme": 25}[regime]
    if nifty_trend == "up":
        base = min(100, base + 10)
    elif nifty_trend == "down":
        base = max(0, base - 10)
    return base, int(base >= 50)


# ── Watermark ────────────────────────────────────────────

def fetch_last_loaded_date(ch) -> date | None:
    try:
        result = ch.query(
            "SELECT max(toDate(timestamp)) FROM market.nifty_live"
        )
        val = result.result_rows[0][0]
        return val if val and val > date(2000, 1, 1) else None
    except Exception:
        return None


# ── yfinance fetch ───────────────────────────────────────

def fetch_yfinance(from_date: date, to_date: date) -> pd.DataFrame:
    start = from_date.strftime("%Y-%m-%d")
    end   = (to_date + timedelta(days=1)).strftime("%Y-%m-%d")
    log.info(f"Downloading {VIX_SYMBOL} + {NIFTY_SYMBOL}  {start} → {end}")

    vix_raw   = yf.download(VIX_SYMBOL,   start=start, end=end,
                             progress=False, auto_adjust=True)
    nifty_raw = yf.download(NIFTY_SYMBOL, start=start, end=end,
                             progress=False, auto_adjust=True)

    if vix_raw.empty:
        log.warning("yfinance returned no VIX data")
        return pd.DataFrame()

    def _flatten(df):
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df

    vix_raw   = _flatten(vix_raw)
    nifty_raw = _flatten(nifty_raw)

    df = pd.DataFrame({"vix": vix_raw["Close"]})
    df["nifty_spot"] = nifty_raw["Close"] if not nifty_raw.empty else 0.0
    df.index = pd.to_datetime(df.index).date
    df.index.name = "date"
    df = df.dropna(subset=["vix"])
    df = df[df["vix"] > 0]
    return df.reset_index()


# ── ClickHouse insert ────────────────────────────────────

def insert_nifty_live(ch, df: pd.DataFrame):
    if df.empty:
        return
    now_ts = int(datetime.now().timestamp())
    rows = []
    for _, row in df.iterrows():
        dt = datetime(row["date"].year, row["date"].month, row["date"].day, 10, 0, 0)
        rows.append({
            "timestamp":       dt,
            "nifty_spot":      float(row.get("nifty_spot") or 0.0),
            "vix":             float(row["vix"]),
            "pcr":             0.0,
            "advance_decline": 0.0,
            "version":         now_ts,
        })
    ch.insert_df("market.nifty_live", pd.DataFrame(rows))
    log.info(f"Inserted {len(rows)} rows → market.nifty_live")


def insert_market_regime(ch, df: pd.DataFrame):
    if df.empty:
        return
    now_ts     = int(datetime.now().timestamp())
    rows       = []
    prev_nifty = None

    for _, row in df.sort_values("date").iterrows():
        vix    = float(row["vix"])
        regime = classify_regime(vix)
        nifty  = float(row.get("nifty_spot") or 0.0)

        if prev_nifty and prev_nifty > 0 and nifty > 0:
            trend = "up" if nifty > prev_nifty else "down"
        else:
            trend = "flat"
        if nifty > 0:
            prev_nifty = nifty

        opp_score, trade_rec = compute_opportunity(regime, trend)
        rows.append({
            "date":              row["date"],
            "vix_regime":        regime,
            "trend":             trend,
            "momentum":          "",
            "fii_stance":        "",
            "opportunity_score": opp_score,
            "trade_recommended": trade_rec,
            "reason":            f"VIX={vix:.1f} ({regime}), trend={trend}",
            "version":           now_ts,
        })

    ch.insert_df("analysis.market_regime", pd.DataFrame(rows))
    log.info(f"Inserted {len(rows)} rows → analysis.market_regime")


# ── Main ─────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="VIX Pipeline — India VIX + Nifty spot + market regime"
    )
    parser.add_argument("--full",      action="store_true",
                        help="Re-fetch last 2 years (ignores watermark)")
    parser.add_argument("--days",      type=int, default=None,
                        help="Custom lookback in days")
    parser.add_argument("--load-only", action="store_true",
                        help="Reload MinIO parquet → ClickHouse, skip yfinance")
    args = parser.parse_args()

    log.info("=== VIX Pipeline ===")
    ch = get_ch_client()
    mc = get_minio_client()
    setup_minio_bucket(mc)

    cached_df = load_from_minio(mc)

    if args.load_only:
        if cached_df.empty:
            log.error("No MinIO cache found. Run without --load-only first.")
            return
        df = cached_df
    else:
        # Determine date range to fetch
        if args.full or args.days:
            lookback  = args.days or LOOKBACK_DAYS
            from_date = date.today() - timedelta(days=lookback)
            log.info(f"Mode: explicit {lookback}d lookback from {from_date}")
        else:
            # Use MinIO cache max date if available, else DB watermark
            if not cached_df.empty:
                from_date = cached_df["date"].max()
                log.info(f"Mode: incremental from MinIO watermark {from_date}")
            else:
                last = fetch_last_loaded_date(ch)
                from_date = last if last else date.today() - timedelta(days=LOOKBACK_DAYS)
                log.info(f"Mode: incremental from DB watermark {from_date}")

        # Only fetch dates not already in cache
        if not cached_df.empty:
            cached_dates = set(cached_df["date"])
            need_from = from_date
        else:
            cached_dates = set()
            need_from = from_date

        new_df = fetch_yfinance(need_from, date.today())

        if new_df.empty:
            log.warning("No new data from yfinance")
            df = cached_df
        else:
            log.info(f"Fetched {len(new_df)} rows from yfinance")
            # Merge: cached + new, deduplicate keeping latest
            df = pd.concat([cached_df, new_df], ignore_index=True)
            df = df.drop_duplicates(subset=["date"], keep="last")
            df = df.sort_values("date").reset_index(drop=True)
            save_to_minio(mc, df)

    if df.empty:
        log.warning("No data — nothing to insert")
        return

    log.info(f"Inserting {len(df)} rows  "
             f"({df['date'].min()} → {df['date'].max()})")
    insert_nifty_live(ch, df)
    insert_market_regime(ch, df)

    print(f"\n{'='*50}")
    print(f"  VIX rows loaded : {len(df)}")
    if not df.empty:
        latest = df.iloc[-1]
        print(f"  Latest VIX      : {latest['vix']:.2f}  "
              f"({classify_regime(float(latest['vix']))})")
        print(f"  Nifty spot      : {latest.get('nifty_spot', 0):.0f}")
    print(f"{'='*50}\n")

    log.info("VIX pipeline complete ✅")


if __name__ == "__main__":
    main()
