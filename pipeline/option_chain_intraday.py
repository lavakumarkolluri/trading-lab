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
import logging
import argparse
import zoneinfo
from datetime import datetime, time as dtime, date

import pandas as pd
import clickhouse_connect
from curl_cffi import requests as cffi_requests
from minio import Minio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────
IST = zoneinfo.ZoneInfo("Asia/Kolkata")
MARKET_OPEN      = dtime(9, 15)
MARKET_CLOSE     = dtime(15, 30)
EXIT_AFTER       = dtime(15, 35)    # hard exit
FETCH_INTERVAL_S = 180              # 3 minutes
SESSION_REFRESH_S = 25 * 60         # refresh NSE session every 25 min

SYMBOLS = ["NIFTY", "BANKNIFTY"]

CH_HOST  = os.getenv("CH_HOST", "clickhouse")
CH_PORT  = int(os.getenv("CH_PORT", "8123"))
CH_USER  = os.getenv("CH_USER", "default")
CH_PASS  = os.getenv("CH_PASSWORD", "")

MINIO_HOST   = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER   = os.getenv("MINIO_USER", "admin")
MINIO_PASS   = os.getenv("MINIO_PASSWORD", "")
MINIO_BUCKET = "trading-data"

NSE_HOME   = "https://www.nseindia.com"
NSE_OC_URL = "https://www.nseindia.com/api/option-chain-indices"
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


# ── Clients ──────────────────────────────────────────────

def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


def get_minio_client() -> Minio:
    return Minio(MINIO_HOST, access_key=MINIO_USER,
                 secret_key=MINIO_PASS, secure=False)


# ── NSE session ──────────────────────────────────────────

def build_nse_session() -> cffi_requests.Session:
    session = cffi_requests.Session(impersonate="chrome110")
    log.info("Building NSE session (homepage warm-up)...")
    resp = session.get(
        f"{NSE_HOME}/option-chain",
        timeout=HTTP_TIMEOUT,
        headers={
            "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 Chrome/110.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    resp.raise_for_status()
    log.info(f"NSE session ready (cookies: {len(session.cookies)})")
    time.sleep(2.0)
    return session


def fetch_chain(session: cffi_requests.Session, symbol: str) -> dict | None:
    try:
        resp = session.get(
            NSE_OC_URL,
            params={"symbol": symbol},
            timeout=HTTP_TIMEOUT,
            headers={
                "Referer":          "https://www.nseindia.com/option-chain",
                "X-Requested-With": "XMLHttpRequest",
                "Accept":           "application/json",
            },
        )
        resp.raise_for_status()
        return resp.json()
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
        expiry_str = item.get("expiryDate", "")
        try:
            expiry = datetime.strptime(expiry_str, "%d-%b-%Y").date()
        except ValueError:
            continue

        ce = item.get("CE", {})
        pe = item.get("PE", {})

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
    last_session_refresh = time.monotonic()
    fetch_count = 0
    session_fail_streak = 0
    _MAX_SESSION_FAILS = 3

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

        # Refresh session every 25 min
        if time.monotonic() - last_session_refresh > SESSION_REFRESH_S:
            log.info("Refreshing NSE session...")
            try:
                session = build_nse_session()
                last_session_refresh = time.monotonic()
                session_fail_streak = 0
            except Exception as e:
                session_fail_streak += 1
                log.warning(
                    f"Session refresh failed ({session_fail_streak}/{_MAX_SESSION_FAILS}): "
                    f"{e} — continuing with old session"
                )
                if session_fail_streak >= _MAX_SESSION_FAILS:
                    _send_telegram(
                        f"⚠️ option_chain_intraday: NSE session refresh failed "
                        f"{session_fail_streak} times in a row. Fetching with stale cookies."
                    )
                    session_fail_streak = 0  # reset after alert to avoid spam

        fetch_count += 1
        log.info(f"--- Snapshot {fetch_count} at {now_ist.strftime('%H:%M:%S')} IST ---")

        all_rows = []
        for symbol in SYMBOLS:
            data = fetch_chain(session, symbol)
            if data is None:
                continue
            rows = parse_all_strikes(data, symbol, now_ist.replace(tzinfo=None))
            all_rows.extend(rows)
            if not args.dry_run:
                save_to_minio(mc, data, symbol, now_ist)
            time.sleep(2.5)  # brief pause between symbols

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
