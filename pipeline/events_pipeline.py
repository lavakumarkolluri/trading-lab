#!/usr/bin/env python3
"""
events_pipeline.py — Download high-impact market events → MinIO → ClickHouse

Sources:
  1. US Federal Reserve FOMC meeting dates (federalreserve.gov — reliable HTML)
  2. RBI MPC meeting dates (rbi.org.in — HTML + hardcoded fallback)
  3. Static Indian calendar events (Budget day, known recurring events)

Output: market.events (event_date, event_name, impact, description)

Run modes:
  python events_pipeline.py           # fetch + save all
  python events_pipeline.py --dry-run # print fetched events, no write
"""

import io
import json
import os
import re
import argparse
from datetime import date, datetime
from typing import Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup
from minio import Minio

from ch_utils import ch_client as get_ch, minio_client as get_mc
from logging_utils import get_logger
log = get_logger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

MINIO_BUCKET = "trading-data"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}

REQUEST_TIMEOUT = 20   # seconds



# ── Source 1: FOMC dates ──────────────────────────────────────────────────────

FOMC_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"

# Hardcoded fallback — updated through 2026; scraper fills beyond this
FOMC_FALLBACK = [
    # 2024
    ("2024-01-31", "FOMC Meeting Decision"),
    ("2024-03-20", "FOMC Meeting Decision"),
    ("2024-05-01", "FOMC Meeting Decision"),
    ("2024-06-12", "FOMC Meeting Decision"),
    ("2024-07-31", "FOMC Meeting Decision"),
    ("2024-09-18", "FOMC Meeting Decision"),
    ("2024-11-07", "FOMC Meeting Decision"),
    ("2024-12-18", "FOMC Meeting Decision"),
    # 2025
    ("2025-01-29", "FOMC Meeting Decision"),
    ("2025-03-19", "FOMC Meeting Decision"),
    ("2025-05-07", "FOMC Meeting Decision"),
    ("2025-06-18", "FOMC Meeting Decision"),
    ("2025-07-30", "FOMC Meeting Decision"),
    ("2025-09-17", "FOMC Meeting Decision"),
    ("2025-10-29", "FOMC Meeting Decision"),
    ("2025-12-10", "FOMC Meeting Decision"),
    # 2026
    ("2026-01-28", "FOMC Meeting Decision"),
    ("2026-03-18", "FOMC Meeting Decision"),
    ("2026-05-06", "FOMC Meeting Decision"),
    ("2026-06-17", "FOMC Meeting Decision"),
    ("2026-07-29", "FOMC Meeting Decision"),
    ("2026-09-16", "FOMC Meeting Decision"),
    ("2026-10-28", "FOMC Meeting Decision"),
    ("2026-12-09", "FOMC Meeting Decision"),
]


def fetch_fomc_dates() -> list[dict]:
    """Scrape FOMC decision dates from Federal Reserve website."""
    events = []
    try:
        resp = requests.get(FOMC_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Fed page uses <div class="panel panel-default"> blocks per year
        # Each block has a <h4> with the year and <p> tags with month/date ranges
        # Decision date is the last date in each range (e.g. "Jan 28-29" → Jan 29)
        current_year = None
        months = {
            "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
            "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12
        }

        for panel in soup.select(".panel-default"):
            h4 = panel.find("h4")
            if h4:
                yr_match = re.search(r"(\d{4})", h4.get_text())
                if yr_match:
                    current_year = int(yr_match.group(1))

            if not current_year:
                continue

            for p in panel.select("p.fomc-meeting--date, .fomc-meeting p"):
                text = p.get_text(strip=True)
                # Matches: "January 28-29*" or "January 29" or "Jan 28-29"
                m = re.search(
                    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+"
                    r"(\d+)(?:[–\-](\d+))?",
                    text, re.IGNORECASE
                )
                if m:
                    mon  = months[m.group(1)[:3].lower()]
                    # decision date = last day of the range
                    day  = int(m.group(3)) if m.group(3) else int(m.group(2))
                    try:
                        decision_date = date(current_year, mon, day)
                        events.append({
                            "event_date":  decision_date,
                            "event_name":  "FOMC Meeting Decision",
                            "impact":      "HIGH",
                            "description": "US Federal Reserve interest rate decision",
                        })
                    except ValueError:
                        pass

        if events:
            log.info(f"FOMC: scraped {len(events)} dates from Fed website")
            return events

    except Exception as e:
        log.warning(f"FOMC scrape failed ({e}), using hardcoded fallback")

    # Fallback
    for ds, name in FOMC_FALLBACK:
        events.append({
            "event_date":  date.fromisoformat(ds),
            "event_name":  name,
            "impact":      "HIGH",
            "description": "US Federal Reserve interest rate decision (hardcoded)",
        })
    log.info(f"FOMC: loaded {len(events)} dates from fallback")
    return events


# ── Source 2: RBI MPC dates ───────────────────────────────────────────────────

RBI_MPC_URL = "https://www.rbi.org.in/Scripts/MPC.aspx"

# Hardcoded fallback for RBI MPC decision dates (last day of each meeting)
RBI_MPC_FALLBACK = [
    # FY2024-25
    ("2024-04-05", "RBI MPC Decision"),
    ("2024-06-07", "RBI MPC Decision"),
    ("2024-08-08", "RBI MPC Decision"),
    ("2024-10-09", "RBI MPC Decision"),
    ("2024-12-06", "RBI MPC Decision"),
    ("2025-02-07", "RBI MPC Decision"),
    # FY2025-26
    ("2025-04-09", "RBI MPC Decision"),
    ("2025-06-06", "RBI MPC Decision"),
    ("2025-08-06", "RBI MPC Decision"),
    ("2025-10-01", "RBI MPC Decision"),
    ("2025-12-05", "RBI MPC Decision"),
    ("2026-02-06", "RBI MPC Decision"),
    # FY2026-27 (partial — first meeting announced)
    ("2026-04-09", "RBI MPC Decision"),
]


def fetch_rbi_mpc_dates() -> list[dict]:
    """Scrape RBI MPC decision dates; fallback to hardcoded list."""
    events = []
    try:
        resp = requests.get(RBI_MPC_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        months = {
            "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
            "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
            "jan":1,"feb":2,"mar":3,"apr":4,"jun":6,"jul":7,"aug":8,
            "sep":9,"oct":10,"nov":11,"dec":12
        }

        # RBI page has date ranges like "June 5-7, 2024"
        text = soup.get_text(" ")
        pattern = re.compile(
            r"(January|February|March|April|May|June|July|August|September|"
            r"October|November|December)\s+(\d+)[–\-\s]+(\d+),?\s+(\d{4})",
            re.IGNORECASE
        )
        for m in pattern.finditer(text):
            mon_name = m.group(1).lower()
            if mon_name not in months:
                continue
            mon  = months[mon_name]
            day  = int(m.group(3))   # decision = last day
            yr   = int(m.group(4))
            try:
                decision_date = date(yr, mon, day)
                if decision_date >= date(2024, 1, 1):
                    events.append({
                        "event_date":  decision_date,
                        "event_name":  "RBI MPC Decision",
                        "impact":      "HIGH",
                        "description": "Reserve Bank of India Monetary Policy Committee rate decision",
                    })
            except ValueError:
                pass

        # Deduplicate
        seen = set()
        unique = []
        for e in events:
            key = e["event_date"]
            if key not in seen:
                seen.add(key)
                unique.append(e)
        events = unique

        if events:
            log.info(f"RBI MPC: scraped {len(events)} dates from RBI website")
            return events

    except Exception as e:
        log.warning(f"RBI MPC scrape failed ({e}), using hardcoded fallback")

    # Fallback
    for ds, name in RBI_MPC_FALLBACK:
        events.append({
            "event_date":  date.fromisoformat(ds),
            "event_name":  name,
            "impact":      "HIGH",
            "description": "Reserve Bank of India MPC rate decision (hardcoded)",
        })
    log.info(f"RBI MPC: loaded {len(events)} dates from fallback")
    return events


# ── Source 3: Static Indian calendar events ───────────────────────────────────

def static_indian_events() -> list[dict]:
    """
    Known recurring high-impact Indian market events.
    Budget day is typically Feb 1; add each year as known.
    """
    events = [
        # Union Budget — Feb 1 each year
        {
            "event_date":  date(2024, 2, 1),
            "event_name":  "Union Budget",
            "impact":      "HIGH",
            "description": "India Union Budget presentation",
        },
        {
            "event_date":  date(2025, 2, 1),
            "event_name":  "Union Budget",
            "impact":      "HIGH",
            "description": "India Union Budget presentation",
        },
        {
            "event_date":  date(2026, 2, 1),
            "event_name":  "Union Budget",
            "impact":      "HIGH",
            "description": "India Union Budget presentation",
        },
        # General Election Results (known past events)
        {
            "event_date":  date(2024, 6, 4),
            "event_name":  "India General Election Results",
            "impact":      "HIGH",
            "description": "18th Lok Sabha election result declaration",
        },
        # RBI interim / emergency actions (add as they happen manually)
    ]
    log.info(f"Static events: {len(events)} dates loaded")
    return events


# ── Aggregate + Deduplicate ───────────────────────────────────────────────────

def collect_all_events() -> list[dict]:
    """Fetch all sources and merge, deduplicating on (event_date, event_name)."""
    all_events: list[dict] = []
    all_events.extend(fetch_fomc_dates())
    all_events.extend(fetch_rbi_mpc_dates())
    all_events.extend(static_indian_events())

    # Deduplicate
    seen: set[tuple] = set()
    unique: list[dict] = []
    for e in all_events:
        key = (e["event_date"], e["event_name"])
        if key not in seen:
            seen.add(key)
            unique.append(e)

    unique.sort(key=lambda x: x["event_date"])
    log.info(f"Total unique events: {len(unique)}")
    return unique


# ── MinIO ─────────────────────────────────────────────────────────────────────

def save_to_minio(mc: Minio, events: list[dict]) -> str:
    """Save events as JSON to MinIO; return object path."""
    if not mc.bucket_exists(MINIO_BUCKET):
        mc.make_bucket(MINIO_BUCKET)

    payload = [
        {**e, "event_date": e["event_date"].isoformat()}
        for e in events
    ]
    data    = json.dumps(payload, indent=2).encode()
    buf     = io.BytesIO(data)
    today   = datetime.utcnow().strftime("%Y-%m-%d")
    path    = f"events/calendar_{today}.json"

    mc.put_object(
        MINIO_BUCKET, path, buf, length=len(data),
        content_type="application/json",
    )
    log.info(f"Saved {len(events)} events to MinIO: {path}")
    return path


# ── ClickHouse ────────────────────────────────────────────────────────────────

def save_to_clickhouse(ch, events: list[dict]) -> None:
    """Upsert events into market.events (ReplacingMergeTree deduplicates)."""
    now_ts = int(datetime.utcnow().timestamp())
    rows = [
        [
            e["event_date"],
            "",                      # event_time — not used currently
            e["event_name"],
            e["impact"],
            e.get("description", ""),
            now_ts,                  # version — triggers replacement on re-run
        ]
        for e in events
    ]
    ch.insert(
        "market.events",
        rows,
        column_names=["event_date", "event_time", "event_name",
                      "impact", "description", "version"],
    )
    log.info(f"Inserted/updated {len(rows)} events in market.events")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Print events without writing to MinIO/ClickHouse")
    args = parser.parse_args()

    events = collect_all_events()

    if args.dry_run:
        print(f"\n{'Date':<12} {'Impact':<8} {'Event'}")
        print("-" * 60)
        for e in events:
            print(f"{e['event_date'].isoformat():<12} {e['impact']:<8} {e['event_name']}")
        print(f"\nTotal: {len(events)} events")
        return

    mc = get_mc()
    ch = get_ch()

    save_to_minio(mc, events)
    save_to_clickhouse(ch, events)
    log.info("events_pipeline done")


if __name__ == "__main__":
    main()
