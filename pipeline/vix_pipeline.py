#!/usr/bin/env python3
"""
vix_pipeline.py
───────────────
Fetches India VIX and Nifty 50 spot daily from yfinance.
Writes to:
  market.nifty_live       — one EOD row per trading date
  analysis.market_regime  — VIX-based regime classification per date

India VIX regime bands (NSE standard):
  < 13       → 'low'       complacency; trend-following, full position size
  13–18      → 'normal'    balanced; standard position sizing
  18–25      → 'elevated'  caution; reduce size, favour mean-reversion
  > 25       → 'extreme'   fear; buy-the-dip only with confirmation

Sources (yfinance):
  INDIAVIX.NS → India VIX published by NSE
  ^NSEI       → Nifty 50 index closing value

Usage:
  python vix_pipeline.py             # incremental (since last loaded date)
  python vix_pipeline.py --full      # re-fetch last 2 years
  python vix_pipeline.py --days 90   # specific lookback window

Docker:
  docker compose run --rm vix_pipeline
"""

import os
import logging
import argparse
from datetime import datetime, date, timedelta

import pandas as pd
import yfinance as yf
import clickhouse_connect

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

LOOKBACK_DAYS = 730
VIX_SYMBOL    = "^INDIAVIX"
NIFTY_SYMBOL  = "^NSEI"

# VIX → regime label thresholds (upper-exclusive bounds)
_VIX_BANDS = [(13.0, "low"), (18.0, "normal"), (25.0, "elevated")]


def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ══════════════════════════════════════════════════════
# Regime helpers
# ══════════════════════════════════════════════════════

def classify_regime(vix: float) -> str:
    for threshold, label in _VIX_BANDS:
        if vix < threshold:
            return label
    return "extreme"


def compute_opportunity(regime: str, nifty_trend: str) -> tuple[int, int]:
    """
    Returns (opportunity_score 0–100, trade_recommended 0/1).
    High score = favourable conditions to deploy capital.
    """
    base = {"low": 75, "normal": 60, "elevated": 40, "extreme": 25}[regime]
    if nifty_trend == "up":
        base = min(100, base + 10)
    elif nifty_trend == "down":
        base = max(0, base - 10)
    return base, int(base >= 50)


# ══════════════════════════════════════════════════════
# Watermark
# ══════════════════════════════════════════════════════

def fetch_last_loaded_date(ch) -> date | None:
    try:
        result = ch.query(
            "SELECT max(toDate(timestamp)) FROM market.nifty_live"
        )
        val = result.result_rows[0][0]
        return val if val and val > date(2000, 1, 1) else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════
# Data fetch
# ══════════════════════════════════════════════════════

def fetch_yfinance(from_date: date, to_date: date) -> pd.DataFrame:
    """
    Download VIX and Nifty from yfinance.
    Returns DataFrame: date (index), vix, nifty_spot.
    """
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

    # Flatten MultiIndex columns if present (yfinance ≥0.2)
    def _flatten(df):
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df

    vix_raw   = _flatten(vix_raw)
    nifty_raw = _flatten(nifty_raw)

    df = pd.DataFrame({"vix": vix_raw["Close"]})
    if not nifty_raw.empty:
        df["nifty_spot"] = nifty_raw["Close"]
    else:
        df["nifty_spot"] = 0.0

    df.index = pd.to_datetime(df.index).date
    df.index.name = "date"
    df = df.dropna(subset=["vix"])
    df = df[df["vix"] > 0]
    return df.reset_index()


# ══════════════════════════════════════════════════════
# Insert: market.nifty_live
# ══════════════════════════════════════════════════════

def insert_nifty_live(ch, df: pd.DataFrame):
    """One row per date, timestamp = 15:30 IST (10:00 UTC)."""
    if df.empty:
        return
    now_ts = int(datetime.now().timestamp())
    rows = []
    for _, row in df.iterrows():
        dt = datetime(row["date"].year, row["date"].month, row["date"].day,
                      10, 0, 0)   # 10:00 UTC = 15:30 IST
        rows.append({
            "timestamp":       dt,
            "nifty_spot":      float(row.get("nifty_spot") or 0.0),
            "vix":             float(row["vix"]),
            "pcr":             0.0,    # updated daily by option_chain_pipeline
            "advance_decline": 0.0,
            "version":         now_ts,
        })
    ch.insert_df("market.nifty_live", pd.DataFrame(rows))
    log.info(f"Inserted {len(rows)} rows → market.nifty_live")


# ══════════════════════════════════════════════════════
# Insert: analysis.market_regime
# ══════════════════════════════════════════════════════

def insert_market_regime(ch, df: pd.DataFrame):
    """Derive and upsert VIX-based regime for each date."""
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


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="VIX Pipeline — India VIX + Nifty spot + market regime"
    )
    parser.add_argument("--full", action="store_true",
                        help="Re-fetch last 2 years (ignores watermark)")
    parser.add_argument("--days", type=int, default=None,
                        help="Custom lookback in days")
    args = parser.parse_args()

    log.info("=== VIX Pipeline ===")
    ch = get_ch_client()

    if args.full or args.days:
        lookback  = args.days or LOOKBACK_DAYS
        from_date = date.today() - timedelta(days=lookback)
        log.info(f"Mode: explicit {lookback}d lookback from {from_date}")
    else:
        last = fetch_last_loaded_date(ch)
        if last:
            from_date = last   # re-fetch last loaded date (catches corrections)
            log.info(f"Mode: incremental from {from_date}")
        else:
            from_date = date.today() - timedelta(days=LOOKBACK_DAYS)
            log.info(f"Mode: initial backfill from {from_date}")

    df = fetch_yfinance(from_date, date.today())

    if df.empty:
        log.warning("No data — nothing to insert")
        return

    log.info(f"Fetched {len(df)} rows  "
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
