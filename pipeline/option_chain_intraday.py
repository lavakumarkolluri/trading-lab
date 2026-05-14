#!/usr/bin/env python3
"""
option_chain_intraday.py
─────────────────────────
Fetches NSE option chain every 3 minutes during market hours and saves
ALL strikes and ALL expiries for NIFTY and BANKNIFTY.

Runs as a long-lived process: started at 9:10 IST, exits at 15:35 IST.
Session is refreshed every 25 minutes to avoid NSE cookie expiry.

Data flow: NSE API → MinIO (raw JSON) → ClickHouse (market.options_chain)
MinIO path: option_chain/intraday/{YYYY-MM-DD}/{HHMM}/{symbol}.json

Usage:
  python option_chain_intraday.py
  python option_chain_intraday.py --dry-run

Docker:
  docker compose run --rm option_chain_intraday
"""

import io
import json
import os
import signal
import sys
import time
import argparse
import zoneinfo
from datetime import datetime, time as dtime, date

import pandas as pd
from jugaad_data.nse import NSELive
from minio import Minio

from ch_utils import ch_client as get_ch_client, minio_client as get_minio_client
from logging_utils import get_logger
log = get_logger(__name__)

# ── Config ───────────────────────────────────────────────
IST = zoneinfo.ZoneInfo("Asia/Kolkata")
MARKET_OPEN      = dtime(9, 15)
MARKET_CLOSE     = dtime(15, 30)
EXIT_AFTER       = dtime(15, 35)    # hard exit
FETCH_INTERVAL_S = 180              # 3 minutes

SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY"]

MINIO_BUCKET = "trading-data"

HTTP_TIMEOUT = 20

_TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

_SHUTDOWN = False


def _sigterm_handler(signum, frame):
    global _SHUTDOWN
    log.info("SIGTERM received — will exit after current fetch")
    _SHUTDOWN = True


signal.signal(signal.SIGTERM, _sigterm_handler)


def _send_telegram(message: str):
    if not (_TG_TOKEN and _TG_CHAT_ID):
        return
    try:
        import urllib.request, json as _json
        payload = _json.dumps({"chat_id": _TG_CHAT_ID, "text": message}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{_TG_TOKEN}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        log.warning("Telegram alert failed: %s", e)


# ── NSE session ──────────────────────────────────────────

def build_nse_session() -> NSELive:
    """Return a jugaad-data NSELive client (handles NSE auth internally)."""
    log.info("Building NSE session via jugaad-data...")
    client = NSELive()
    log.info("NSE session ready (jugaad-data)")
    return client


def fetch_chain(client: NSELive, symbol: str) -> dict | None:
    try:
        data = client.equities_option_chain(symbol)
        if not data or "records" not in data:
            raise ValueError(f"Empty/invalid response: {str(data)[:60]}")
        return data
    except Exception as e:
        log.warning(f"Fetch failed for {symbol}: {e}")
        return None


# ── Parsing ──────────────────────────────────────────────

def parse_all_strikes(data: dict, symbol: str, now: datetime) -> list[dict]:
    """
    Parse ALL strikes across ALL expiries from NSE option chain JSON.
    Returns a list of row dicts for market.options_chain.
    """
    records  = data.get("records", {})
    spot     = float(records.get("underlyingValue", 0))
    all_data = records.get("data", [])  # full chain (all expiries)

    if not all_data or spot == 0:
        return []

    version = int(now.timestamp())
    rows = []

    for item in all_data:
        strike = float(item.get("strikePrice", 0))
        ce = item.get("CE", {})
        pe = item.get("PE", {})
        # jugaad-data puts expiryDate inside CE/PE; original NSE puts it at item level
        expiry_str = (item.get("expiryDate")
                      or (ce or {}).get("expiryDate")
                      or (pe or {}).get("expiryDate")
                      or "")
        expiry = None
        for fmt in ("%d-%b-%Y", "%d-%m-%Y"):
            try:
                expiry = datetime.strptime(expiry_str, fmt).date()
                break
            except ValueError:
                continue
        if expiry is None:
            continue

        for opt_type, side in (("CE", ce), ("PE", pe)):
            if not side:
                continue
            ltp = float(side.get("lastPrice", 0))
            oi  = int(side.get("openInterest", 0))
            if ltp == 0 and oi == 0:
                continue
            rows.append({
                "symbol":      symbol,
                "timestamp":   now,
                "expiry":      expiry,
                "strike":      strike,
                "option_type": opt_type,
                "ltp":         ltp,
                "iv":          float(side.get("impliedVolatility", 0)),
                "delta":       float(side.get("delta", 0)),
                "theta":       float(side.get("theta", 0)),
                "oi":          oi,
                "oi_change":   int(side.get("changeinOpenInterest", 0)),
                "volume":      int(side.get("totalTradedVolume", 0)),
                "version":     version,
            })

    log.info(f"  {symbol}: spot={spot:.0f}  strikes={len(rows)//2}  "
             f"expiries={len({r['expiry'] for r in rows})}")
    return rows


# ── MinIO ─────────────────────────────────────────────────

def save_to_minio(mc: Minio, data: dict, symbol: str, now: datetime):
    key = (f"option_chain/intraday/{now.strftime('%Y-%m-%d')}/"
           f"{now.strftime('%H%M')}/{symbol}.json")
    raw = json.dumps(data).encode()
    mc.put_object(MINIO_BUCKET, key, io.BytesIO(raw), len(raw),
                  content_type="application/json")


# ── ClickHouse insert ─────────────────────────────────────

def insert_rows(ch, rows: list[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    ch.insert_df("market.options_chain", df[[
        "symbol", "timestamp", "expiry", "strike", "option_type",
        "ltp", "iv", "delta", "theta", "oi", "oi_change", "volume", "version",
    ]])


# ── Market hours helpers ──────────────────────────────────

def ist_now() -> datetime:
    return datetime.now(IST)


def ist_time() -> dtime:
    return ist_now().time().replace(tzinfo=None)


def seconds_until_open() -> float:
    now = ist_now()
    today_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if now >= today_open:
        return 0
    return (today_open - now).total_seconds()


# ── Main loop ─────────────────────────────────────────────

def _assert_env(*names: str):
    missing = [n for n in names if not os.getenv(n)]
    if missing:
        raise SystemExit(f"Missing required env vars: {', '.join(missing)}")


def main():
    parser = argparse.ArgumentParser(description="Option chain intraday scraper")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and log but do not write to DB or MinIO")
    args = parser.parse_args()

    if not args.dry_run:
        _assert_env("CH_PASSWORD", "MINIO_PASSWORD")

    log.info("=== Option Chain Intraday Scraper ===")
    log.info(f"Symbols   : {SYMBOLS}")
    log.info(f"Interval  : {FETCH_INTERVAL_S}s ({FETCH_INTERVAL_S//60} min)")
    log.info(f"Hours     : {MARKET_OPEN}–{MARKET_CLOSE} IST")

    # Wait until market open if started early
    wait_s = seconds_until_open()
    if wait_s > 0:
        log.info(f"Market opens in {wait_s/60:.1f} min — sleeping...")
        time.sleep(wait_s)

    if not args.dry_run:
        mc = get_minio_client()
        if not mc.bucket_exists(MINIO_BUCKET):
            mc.make_bucket(MINIO_BUCKET)
        ch = get_ch_client()
        log.info("ClickHouse + MinIO connected")

    session = build_nse_session()
    fetch_count = 0
    session_fail_streak = 0
    _MAX_SESSION_FAILS = 3
    consecutive_empty = 0     # force refresh when all symbols return None repeatedly

    while ist_time() <= EXIT_AFTER and not _SHUTDOWN:
        now_ist = ist_now()
        t = ist_time()

        # Outside market hours — exit
        if t < MARKET_OPEN:
            log.info("Before market open — waiting")
            time.sleep(30)
            continue
        if t > MARKET_CLOSE:
            log.info(f"Market closed ({t}) — exiting")
            break

        fetch_count += 1
        log.info(f"--- Snapshot {fetch_count} at {now_ist.strftime('%H:%M:%S')} IST ---")

        all_rows = []
        fetched = 0
        for symbol in SYMBOLS:
            data = fetch_chain(session, symbol)
            if data is None:
                continue
            fetched += 1
            rows = parse_all_strikes(data, symbol, now_ist.replace(tzinfo=None))
            all_rows.extend(rows)
            if not args.dry_run:
                save_to_minio(mc, data, symbol, now_ist)
            time.sleep(2.5)  # brief pause between symbols

        # Force session refresh if all symbols failed (likely Akamai block)
        if fetched == 0:
            consecutive_empty += 1
            log.warning("All fetches returned empty (%d consecutive) — forcing session refresh",
                        consecutive_empty)
            try:
                session = build_nse_session()
                last_session_refresh = time.monotonic()
                consecutive_empty = 0
            except Exception as e:
                log.warning("Forced session refresh failed: %s", e)
        else:
            consecutive_empty = 0

        if not args.dry_run and all_rows:
            insert_rows(ch, all_rows)
            log.info(f"Inserted {len(all_rows)} rows → market.options_chain")
        elif args.dry_run:
            log.info(f"[DRY RUN] Would insert {len(all_rows)} rows")

        # Heartbeat for Docker healthcheck
        try:
            with open("/tmp/intraday_heartbeat", "w") as _f:
                _f.write(str(int(time.time())))
        except Exception:
            pass

        # Sleep until the next 3-min mark
        elapsed = (ist_now() - now_ist).total_seconds()
        sleep_s = max(0, FETCH_INTERVAL_S - elapsed)
        log.info(f"Next fetch in {sleep_s:.0f}s")
        time.sleep(sleep_s)

    log.info(f"Intraday scraper done — {fetch_count} snapshots captured")


if __name__ == "__main__":
    main()
