#!/usr/bin/env python3
"""
holidays_pipeline.py
────────────────────
Seeds NSE/BSE trading holidays into market.trading_holidays.

Strategy:
  1. Try NSE API (requires active session — may fail from Docker)
  2. Fall back to hardcoded known holidays for 2024–2026
  3. Idempotent — safe to re-run anytime

Also provides utility functions used by other pipeline components:
  is_trading_day(d)     → bool
  next_trading_day(d)   → date (next weekday that is not a holiday)
  prev_trading_day(d)   → date (previous weekday that is not a holiday)

Usage:
  python holidays_pipeline.py                    # seed holidays into DB
  python holidays_pipeline.py --check 2026-04-14 # is this a trading day?
  python holidays_pipeline.py --next 2026-04-03  # next trading day after date
  python holidays_pipeline.py --list             # show all holidays in DB
"""

import os
import sys
import logging
import argparse
from datetime import datetime, date, timedelta

import requests
import pandas as pd
import clickhouse_connect

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

NSE_HOLIDAY_API = "https://www.nseindia.com/api/holiday-master?type=trading"


# ── ClickHouse client ──────────────────────────────────
def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ══════════════════════════════════════════════════════
# Hardcoded holiday data (2024–2026)
# Source: NSE circulars + official exchange communications
# ══════════════════════════════════════════════════════

NSE_HOLIDAYS = [

    # ── 2024 ──────────────────────────────────────────
    {"holiday_date": "2024-01-22", "description": "Ram Lalla Pran Pratishtha",          "holiday_type": "full_closure"},
    {"holiday_date": "2024-01-26", "description": "Republic Day",                        "holiday_type": "full_closure"},
    {"holiday_date": "2024-03-08", "description": "Mahashivratri",                       "holiday_type": "full_closure"},
    {"holiday_date": "2024-03-25", "description": "Holi",                                "holiday_type": "full_closure"},
    {"holiday_date": "2024-03-29", "description": "Good Friday",                         "holiday_type": "full_closure"},
    {"holiday_date": "2024-04-11", "description": "Id-Ul-Fitr (Ramzan Id)",              "holiday_type": "full_closure"},
    {"holiday_date": "2024-04-14", "description": "Dr. Ambedkar Jayanti",                "holiday_type": "full_closure"},
    {"holiday_date": "2024-04-17", "description": "Ram Navami",                          "holiday_type": "full_closure"},
    {"holiday_date": "2024-04-21", "description": "Mahavir Jayanti",                     "holiday_type": "full_closure"},
    {"holiday_date": "2024-05-23", "description": "Buddha Purnima",                      "holiday_type": "full_closure"},
    {"holiday_date": "2024-06-17", "description": "Bakri Id (Id-Ul-Adha)",               "holiday_type": "full_closure"},
    {"holiday_date": "2024-07-17", "description": "Muharram",                            "holiday_type": "full_closure"},
    {"holiday_date": "2024-08-15", "description": "Independence Day",                    "holiday_type": "full_closure"},
    {"holiday_date": "2024-10-02", "description": "Mahatma Gandhi Jayanti",              "holiday_type": "full_closure"},
    {"holiday_date": "2024-10-12", "description": "Dussehra",                            "holiday_type": "full_closure"},
    {"holiday_date": "2024-11-01", "description": "Diwali Laxmi Pujan",                  "holiday_type": "full_closure"},
    {"holiday_date": "2024-11-15", "description": "Gurunanak Jayanti",                   "holiday_type": "full_closure"},
    {"holiday_date": "2024-12-25", "description": "Christmas",                           "holiday_type": "full_closure"},

    # ── 2025 ──────────────────────────────────────────
    {"holiday_date": "2025-01-26", "description": "Republic Day",                        "holiday_type": "full_closure"},
    {"holiday_date": "2025-02-26", "description": "Mahashivratri",                       "holiday_type": "full_closure"},
    {"holiday_date": "2025-03-14", "description": "Holi",                                "holiday_type": "full_closure"},
    {"holiday_date": "2025-03-31", "description": "Id-Ul-Fitr (Ramzan Id)",              "holiday_type": "full_closure"},
    {"holiday_date": "2025-04-10", "description": "Shri Ram Navami",                     "holiday_type": "full_closure"},
    {"holiday_date": "2025-04-14", "description": "Dr. Ambedkar Jayanti",                "holiday_type": "full_closure"},
    {"holiday_date": "2025-04-18", "description": "Good Friday",                         "holiday_type": "full_closure"},
    {"holiday_date": "2025-05-01", "description": "Maharashtra Day",                     "holiday_type": "full_closure"},
    {"holiday_date": "2025-08-15", "description": "Independence Day",                    "holiday_type": "full_closure"},
    {"holiday_date": "2025-08-27", "description": "Ganesh Chaturthi",                    "holiday_type": "full_closure"},
    {"holiday_date": "2025-10-02", "description": "Mahatma Gandhi Jayanti / Dussehra",   "holiday_type": "full_closure"},
    {"holiday_date": "2025-10-20", "description": "Diwali Laxmi Pujan",                  "holiday_type": "full_closure"},
    {"holiday_date": "2025-10-21", "description": "Diwali Balipratipada",                "holiday_type": "full_closure"},
    {"holiday_date": "2025-11-05", "description": "Gurunanak Jayanti",                   "holiday_type": "full_closure"},
    {"holiday_date": "2025-12-25", "description": "Christmas",                           "holiday_type": "full_closure"},

    # ── 2026 ──────────────────────────────────────────
    {"holiday_date": "2026-01-26", "description": "Republic Day",                        "holiday_type": "full_closure"},
    {"holiday_date": "2026-02-19", "description": "Chhatrapati Shivaji Maharaj Jayanti", "holiday_type": "full_closure"},
    {"holiday_date": "2026-03-20", "description": "Holi",                                "holiday_type": "full_closure"},
    {"holiday_date": "2026-03-26", "description": "Shri Ram Navami",                     "holiday_type": "full_closure"},
    {"holiday_date": "2026-03-31", "description": "Mahavir Jayanti",                     "holiday_type": "full_closure"},
    {"holiday_date": "2026-04-03", "description": "Good Friday",                         "holiday_type": "full_closure"},
    {"holiday_date": "2026-04-14", "description": "Dr. Ambedkar Jayanti",                "holiday_type": "full_closure"},
    {"holiday_date": "2026-05-01", "description": "Maharashtra Day",                     "holiday_type": "full_closure"},
    {"holiday_date": "2026-06-19", "description": "Eid al-Adha (Bakri Id)",              "holiday_type": "full_closure"},
    {"holiday_date": "2026-08-15", "description": "Independence Day",                    "holiday_type": "full_closure"},
    {"holiday_date": "2026-10-02", "description": "Mahatma Gandhi Jayanti",              "holiday_type": "full_closure"},
    {"holiday_date": "2026-10-19", "description": "Dussehra",                            "holiday_type": "full_closure"},
    {"holiday_date": "2026-11-06", "description": "Diwali Laxmi Pujan",                  "holiday_type": "full_closure"},
    {"holiday_date": "2026-11-08", "description": "Diwali Muhurat Trading",              "holiday_type": "muhurat"},
    {"holiday_date": "2026-11-27", "description": "Gurunanak Jayanti",                   "holiday_type": "full_closure"},
    {"holiday_date": "2026-12-25", "description": "Christmas",                           "holiday_type": "full_closure"},
]


# ══════════════════════════════════════════════════════
# NSE API attempt (may fail without browser session)
# ══════════════════════════════════════════════════════

def try_fetch_from_nse_api() -> list[dict]:
    """
    Attempt to fetch holidays from NSE API.
    Requires a browser-like session — will likely fail from Docker.
    Returns empty list on failure (falls back to hardcoded).
    """
    headers = {
        "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept":          "application/json",
        "Referer":         "https://www.nseindia.com/",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        resp = session.get(NSE_HOLIDAY_API, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        rows = []
        for item in data.get("CM", []):  # CM = Capital Market segment
            d    = item.get("tradingDate", "")
            desc = item.get("description", "")
            if d:
                try:
                    parsed = datetime.strptime(d, "%d-%b-%Y").date()
                    rows.append({
                        "holiday_date": str(parsed),
                        "description":  desc,
                        "holiday_type": "full_closure",
                    })
                except Exception:
                    pass

        log.info(f"NSE API: fetched {len(rows)} holidays")
        return rows

    except Exception as e:
        log.warning(f"NSE API fetch failed (expected from Docker): {e}")
        log.info("Falling back to hardcoded holiday data.")
        return []


# ══════════════════════════════════════════════════════
# Insert into ClickHouse
# ══════════════════════════════════════════════════════

def seed_holidays(ch, holidays: list[dict], source: str = "hardcoded"):
    """
    Insert holidays into market.trading_holidays.
    ReplacingMergeTree deduplicates on (exchange, holiday_date).
    Safe to re-run — new version timestamp wins after merge.
    """
    if not holidays:
        log.warning("No holidays to insert.")
        return 0

    now_ts = int(datetime.now().timestamp())
    rows   = []

    for h in holidays:
        d = date.fromisoformat(h["holiday_date"])
        rows.append({
            "holiday_date": d,
            "exchange":     "NSE",
            "description":  h["description"],
            "holiday_type": h.get("holiday_type", "full_closure"),
            "source":       source,
            "added_on":     datetime.now(),
            "version":      now_ts,
        })

    df = pd.DataFrame(rows)
    ch.insert_df("market.trading_holidays", df)
    log.info(f"✅ Inserted {len(rows)} holiday records into market.trading_holidays")
    return len(rows)


# ══════════════════════════════════════════════════════
# Trading day utilities
# Imported by: prediction_engine.py, validation_engine.py
# ══════════════════════════════════════════════════════

# Module-level cache — populated on first call
_holiday_cache: set[date] = set()
_cache_loaded:  bool      = False


def _load_holiday_cache(ch=None):
    """Load all full_closure holidays into module-level cache."""
    global _holiday_cache, _cache_loaded
    if _cache_loaded:
        return

    try:
        if ch is None:
            ch = get_ch_client()

        result = ch.query(
            "SELECT holiday_date FROM market.trading_holidays FINAL "
            "WHERE exchange = 'NSE' AND holiday_type = 'full_closure'"
        )
        _holiday_cache = {row[0] for row in result.result_rows}
        _cache_loaded  = True
        log.debug(f"Holiday cache loaded: {len(_holiday_cache)} dates")

    except Exception as e:
        log.warning(f"Could not load holiday cache: {e}. Using empty cache.")
        _holiday_cache = set()
        _cache_loaded  = True


def is_trading_day(d: date, ch=None) -> bool:
    """
    Returns True if d is a weekday and NOT a NSE full_closure holiday.
    """
    _load_holiday_cache(ch)
    if d.weekday() >= 5:           # Saturday=5, Sunday=6
        return False
    return d not in _holiday_cache


def next_trading_day(d: date, ch=None) -> date:
    """
    Returns the next trading day AFTER d (not including d).
    Skips weekends and NSE holidays.
    """
    _load_holiday_cache(ch)
    candidate = d + timedelta(days=1)
    while not is_trading_day(candidate, ch):
        candidate += timedelta(days=1)
    return candidate


def prev_trading_day(d: date, ch=None) -> date:
    """
    Returns the most recent trading day BEFORE d (not including d).
    Skips weekends and NSE holidays.
    """
    _load_holiday_cache(ch)
    candidate = d - timedelta(days=1)
    while not is_trading_day(candidate, ch):
        candidate -= timedelta(days=1)
    return candidate


def trading_days_between(start: date, end: date, ch=None) -> int:
    """
    Count trading days between start (inclusive) and end (inclusive).
    """
    _load_holiday_cache(ch)
    count = 0
    current = start
    while current <= end:
        if is_trading_day(current, ch):
            count += 1
        current += timedelta(days=1)
    return count


# ══════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════

def show_holidays(ch):
    """Print all holidays in DB sorted by date."""
    result = ch.query(
        "SELECT holiday_date, description, holiday_type, source "
        "FROM market.trading_holidays FINAL "
        "WHERE exchange = 'NSE' "
        "ORDER BY holiday_date"
    )
    print(f"\n{'Date':<14} {'Type':<15} {'Description':<45} {'Source'}")
    print("-" * 85)
    for row in result.result_rows:
        print(f"{str(row[0]):<14} {row[2]:<15} {row[1]:<45} {row[3]}")
    print(f"\nTotal: {len(result.result_rows)} holidays\n")


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Holiday Pipeline — seeds NSE trading holidays into ClickHouse"
    )
    parser.add_argument("--check", type=str, default=None,
                        help="Check if a date is a trading day (YYYY-MM-DD)")
    parser.add_argument("--next",  type=str, default=None,
                        help="Get next trading day after date (YYYY-MM-DD)")
    parser.add_argument("--prev",  type=str, default=None,
                        help="Get previous trading day before date (YYYY-MM-DD)")
    parser.add_argument("--list",  action="store_true",
                        help="List all holidays in DB")
    args = parser.parse_args()

    log.info("=== Holiday Pipeline Starting ===")

    ch = get_ch_client()
    log.info("ClickHouse connected ✅")

    # ── Query modes ────────────────────────────────────
    if args.list:
        show_holidays(ch)
        return

    if args.check:
        d       = date.fromisoformat(args.check)
        trading = is_trading_day(d, ch)
        status  = "✅ TRADING DAY" if trading else "❌ NOT a trading day"
        print(f"\n{d} → {status}")
        if not trading:
            nxt = next_trading_day(d, ch)
            print(f"Next trading day: {nxt}")
        return

    if args.next:
        d   = date.fromisoformat(args.next)
        nxt = next_trading_day(d, ch)
        print(f"\nNext trading day after {d}: {nxt}")
        return

    if args.prev:
        d   = date.fromisoformat(args.prev)
        prv = prev_trading_day(d, ch)
        print(f"\nPrevious trading day before {d}: {prv}")
        return

    # ── Seed mode (default) ────────────────────────────
    log.info("Attempting NSE API fetch...")
    api_holidays = try_fetch_from_nse_api()

    if api_holidays:
        holidays = api_holidays
        source   = "nse_api"
        log.info(f"Using {len(holidays)} holidays from NSE API")
    else:
        holidays = NSE_HOLIDAYS
        source   = "hardcoded"
        log.info(f"Using {len(holidays)} hardcoded holidays (2024–2026)")

    count = seed_holidays(ch, holidays, source)

    # Verify
    result = ch.query(
        "SELECT count() FROM market.trading_holidays FINAL "
        "WHERE exchange = 'NSE'"
    )
    total = result.result_rows[0][0]
    log.info(f"Total holidays in DB: {total}")

    # Show upcoming holidays
    result = ch.query(
        "SELECT holiday_date, description "
        "FROM market.trading_holidays FINAL "
        "WHERE exchange = 'NSE' "
        "  AND holiday_date >= today() "
        "  AND holiday_type = 'full_closure' "
        "ORDER BY holiday_date "
        "LIMIT 5"
    )
    print("\nNext 5 upcoming NSE holidays:")
    for row in result.result_rows:
        print(f"  {row[0]}  {row[1]}")

    log.info(f"Finished ✅")


if __name__ == "__main__":
    main()
