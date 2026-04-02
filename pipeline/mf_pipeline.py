#!/usr/bin/env python3
"""
mf_pipeline.py
──────────────
Downloads MF scheme master and NAV history from mfapi.in into ClickHouse.

SCHEME FILTER — only downloads schemes matching:
  1. Name contains "DIRECT" AND "GROWTH"
  2. Name contains "ETF"
  3. Name contains "FUND OF FUND" or " FOF"
  4. Name contains "OVERSEAS" or "INTERNATIONAL" or "GLOBAL"

Inactive schemes (no NAV in last 10 days) are skipped automatically
via the incremental watermark check.
"""

import requests
import clickhouse_connect
import pandas as pd
import os
import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential


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

MFAPI_BASE        = "https://api.mfapi.in/mf"
DELAY_PER_REQUEST = 0.1   # per-worker delay
BATCH_SIZE        = 500   # log progress every N schemes
MAX_WORKERS       = 8     # parallel download threads
INACTIVITY_DAYS   = 10    # skip schemes with no NAV in this many days

# ── Results Tracker ────────────────────────────────────
results      = {"success": [], "skipped": [], "failed": []}
results_lock = threading.Lock()


# ── ClickHouse Client ──────────────────────────────────
def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ── Scheme Filter ──────────────────────────────────────
def is_relevant_scheme(name: str) -> bool:
    n = name.upper()
    is_etf           = "ETF" in n
    is_direct_growth = "DIRECT" in n and "GROWTH" in n
    is_fof_growth    = ("FUND OF FUND" in n or " FOF" in n or n.startswith("FOF ")) and "GROWTH" in n
    is_overseas_growth = any(x in n for x in ["OVERSEAS", "INTERNATIONAL", "GLOBAL"]) and "GROWTH" in n
    return is_etf or is_direct_growth or is_fof_growth or is_overseas_growth


# ── Preflight check — tables must exist ───────────────
def assert_tables_exist(ch):
    """Verify migration has been run before pipeline executes."""
    for table in ["market.mf_schemes", "market.mf_nav"]:
        result = ch.query(
            "SELECT count() FROM system.tables "
            "WHERE database = {db:String} AND name = {tbl:String}",
            parameters={
                "db":  table.split(".")[0],
                "tbl": table.split(".")[1]
            }
        ).result_rows[0][0]
        if not result:
            raise RuntimeError(
                f"Table {table} missing — run migrations first: "
                f"docker compose run --rm migrate"
            )
    log.info("Tables verified ✅")


# ── Fetch All Schemes from MFAPI ───────────────────────
def fetch_all_schemes():
    log.info("Fetching list of all mutual fund schemes from mfapi.in...")
    for attempt in range(1, 6):  # 5 attempts
        try:
            session = requests.Session()
            session.mount("https://", requests.adapters.HTTPAdapter())
            resp = session.get(
                MFAPI_BASE,
                timeout=120,  # longer timeout for 5.6MB response
                headers={"Accept-Encoding": "identity"}  # disable compression
            )
            resp.raise_for_status()
            schemes = resp.json()
            log.info(f"Total schemes from API  : {len(schemes)}")
            filtered = [s for s in schemes if is_relevant_scheme(s["schemeName"])]
            log.info(f"After Direct/Growth/ETF/FoF filter: {len(filtered)}")
            return filtered
        except Exception as e:
            log.warning(f"Attempt {attempt}/5 failed: {e}")
            if attempt < 5:
                wait = attempt * 10  # 10s, 20s, 30s, 40s
                log.info(f"Waiting {wait}s before retry...")
                time.sleep(wait)
    raise RuntimeError("Failed to fetch scheme list after 5 attempts")


# ── Parse Fund House from Scheme Name ─────────────────
def parse_fund_house(name):
    known = [
        "SBI", "HDFC", "ICICI Prudential", "Axis", "Kotak",
        "Nippon India", "Mirae Asset", "Aditya Birla Sun Life",
        "UTI", "DSP", "Franklin Templeton", "Tata", "IDFC",
        "Sundaram", "Canara Robeco", "Invesco", "Motilal Oswal",
        "Edelweiss", "Parag Parikh", "Quant", "PGIM India",
        "Bandhan", "Navi", "WhiteOak", "Baroda BNP Paribas",
        "LIC", "JM Financial", "ITI", "Quantum", "Mahindra Manulife",
        "NJ", "Trust", "Bajaj Finserv", "Old Bridge", "Samco",
        "360 ONE", "HSBC", "Union", "BOI AXA"
    ]
    name_upper = name.upper()
    for fh in known:
        if fh.upper() in name_upper:
            return fh
    parts = name.split(" - ")[0].split("(")[0].strip()
    words = parts.split()
    return " ".join(words[:2]) if len(words) >= 2 else words[0]


# ── Parse Scheme Type from Name ────────────────────────
def parse_scheme_type(name):
    name_upper = name.upper()
    if any(x in name_upper for x in ["EQUITY", "FLEXI CAP", "LARGE CAP",
            "MID CAP", "SMALL CAP", "MULTI CAP", "ELSS", "FOCUSED",
            "THEMATIC", "SECTORAL", "CONTRA", "VALUE", "DIVIDEND YIELD"]):
        return "Equity"
    if any(x in name_upper for x in ["DEBT", "BOND", "GILT", "LIQUID",
            "MONEY MARKET", "OVERNIGHT", "ULTRA SHORT", "LOW DURATION",
            "SHORT DURATION", "MEDIUM DURATION", "LONG DURATION",
            "CREDIT RISK", "BANKING AND PSU", "CORPORATE BOND",
            "DYNAMIC BOND", "FLOATING RATE", "FIXED MATURITY"]):
        return "Debt"
    if any(x in name_upper for x in ["HYBRID", "BALANCED", "AGGRESSIVE",
            "CONSERVATIVE", "MULTI ASSET", "EQUITY SAVINGS", "ARBITRAGE"]):
        return "Hybrid"
    if any(x in name_upper for x in ["GOLD", "SILVER", "COMMODITY"]):
        return "Commodity"
    if any(x in name_upper for x in ["FUND OF FUND", "FOF", "OVERSEAS",
            "INTERNATIONAL", "GLOBAL"]):
        return "FoF"
    if "INDEX" in name_upper or "ETF" in name_upper or "NIFTY" in name_upper \
            or "SENSEX" in name_upper or "NEXT 50" in name_upper:
        return "Index/ETF"
    return "Other"


# ── Parse Scheme Category ──────────────────────────────
def parse_scheme_category(name):
    name_upper = name.upper()
    cats = {
        "ELSS":               "ELSS",
        "LIQUID":             "Liquid",
        "OVERNIGHT":          "Overnight",
        "ULTRA SHORT":        "Ultra Short Duration",
        "LOW DURATION":       "Low Duration",
        "SHORT DURATION":     "Short Duration",
        "MEDIUM DURATION":    "Medium Duration",
        "LONG DURATION":      "Long Duration",
        "GILT":               "Gilt",
        "CREDIT RISK":        "Credit Risk",
        "CORPORATE BOND":     "Corporate Bond",
        "BANKING AND PSU":    "Banking & PSU",
        "DYNAMIC BOND":       "Dynamic Bond",
        "FLOATING RATE":      "Floating Rate",
        "ARBITRAGE":          "Arbitrage",
        "MULTI ASSET":        "Multi Asset",
        "BALANCED ADVANTAGE": "Balanced Advantage",
        "AGGRESSIVE HYBRID":  "Aggressive Hybrid",
        "CONSERVATIVE HYBRID":"Conservative Hybrid",
        "EQUITY SAVINGS":     "Equity Savings",
        "LARGE AND MID":      "Large & Mid Cap",
        "LARGE CAP":          "Large Cap",
        "MID CAP":            "Mid Cap",
        "SMALL CAP":          "Small Cap",
        "MULTI CAP":          "Multi Cap",
        "FLEXI CAP":          "Flexi Cap",
        "FOCUSED":            "Focused",
        "CONTRA":             "Contra",
        "VALUE":              "Value",
        "DIVIDEND YIELD":     "Dividend Yield",
        "THEMATIC":           "Thematic",
        "SECTORAL":           "Sectoral",
        "INDEX":              "Index",
        "ETF":                "ETF",
        "GOLD":               "Gold",
        "SILVER":             "Silver",
        "FUND OF FUND":       "Fund of Funds",
        "FOF":                "Fund of Funds",
        "INTERNATIONAL":      "International",
        "OVERSEAS":           "International",
    }
    for key, cat in cats.items():
        if key in name_upper:
            return cat
    return "Other"


# ── Load Schemes Master into ClickHouse ────────────────
def load_schemes_master(ch, schemes):
    """Insert scheme master — skips if already loaded."""
    count = ch.query(
        "SELECT count() FROM market.mf_schemes"
    ).result_rows[0][0]

    if count > 0:
        log.info(f"Schemes master already loaded ({count} rows), skipping")
        return

    log.info(f"Loading {len(schemes)} schemes into market.mf_schemes...")
    today = date.today()
    rows  = []
    for s in schemes:
        code = int(s["schemeCode"])
        name = s["schemeName"]
        rows.append({
            "scheme_code":     code,
            "scheme_name":     name,
            "fund_house":      parse_fund_house(name),
            "scheme_type":     parse_scheme_type(name),
            "scheme_category": parse_scheme_category(name),
            "is_active":       1,
            "added_on":        today
        })
    df = pd.DataFrame(rows)
    ch.insert_df("market.mf_schemes", df)
    log.info(f"✅ Loaded {len(df)} schemes into market.mf_schemes")


# ── Fetch all NAV watermarks in ONE query ─────────────
def fetch_all_nav_watermarks(ch) -> dict:
    """
    Fetch max(date) for ALL schemes in ONE query.
    Returns dict: {scheme_code: last_date}
    """
    result = ch.query(
        "SELECT scheme_code, max(date) "
        "FROM market.mf_nav "
        "GROUP BY scheme_code"
    )
    return {row[0]: row[1] for row in result.result_rows}


# ── Fetch NAV History for One Scheme ──────────────────
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def fetch_nav_history(scheme_code):
    url  = f"{MFAPI_BASE}/{scheme_code}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])


# ── Parse NAV Data ─────────────────────────────────────
def parse_nav_data(scheme_code, nav_list, last_date=None):
    rows = []
    for item in nav_list:
        try:
            d   = datetime.strptime(item["date"], "%d-%m-%Y").date()
            nav = float(item["nav"])
            if nav <= 0:
                continue
            if last_date and d <= last_date:
                continue
            rows.append({
                "date":        d,
                "scheme_code": int(scheme_code),
                "nav":         nav,
                "version":     int(datetime.now().timestamp())
            })
        except Exception:
            continue
    return rows


# ── Insert NAV Rows to ClickHouse ──────────────────────
def insert_nav_rows(ch, rows):
    if not rows:
        return
    df = pd.DataFrame(rows)
    ch.insert_df("market.mf_nav", df)


# ── Process One Scheme ─────────────────────────────────
def process_scheme(ch, scheme_code, scheme_name, last_date, idx, total):
    try:
        today      = date.today()
        cutoff     = today - timedelta(days=INACTIVITY_DAYS)

        # Skip if last NAV is recent enough and already up to date
        if last_date and last_date >= today:
            with results_lock:
                results["skipped"].append(scheme_code)
            return

        nav_list = fetch_nav_history(scheme_code)
        if not nav_list:
            raise ValueError("No NAV data returned")

        # Check if fund is inactive — most recent NAV older than cutoff
        try:
            most_recent = datetime.strptime(nav_list[0]["date"], "%d-%m-%Y").date()
            if most_recent < cutoff:
                log.info(f"  Skipping inactive fund [{scheme_code}] {scheme_name[:40]} "
                         f"(last NAV: {most_recent})")
                with results_lock:
                    results["skipped"].append(scheme_code)
                return
        except Exception:
            pass

        rows = parse_nav_data(scheme_code, nav_list, last_date)
        if not rows:
            with results_lock:
                results["skipped"].append(scheme_code)
            return

        insert_nav_rows(ch, rows)
        with results_lock:
            results["success"].append(scheme_code)

        if idx % BATCH_SIZE == 0:
            with results_lock:
                log.info(f"  Progress: {idx}/{total} | "
                         f"✅ {len(results['success'])} | "
                         f"⏭️  {len(results['skipped'])} | "
                         f"❌ {len(results['failed'])}")

    except Exception as e:
        log.error(f"  FAILED [{scheme_code}] {scheme_name[:40]}: {e}")
        with results_lock:
            results["failed"].append(f"{scheme_code} → {e}")


# ── Print Summary ──────────────────────────────────────
def print_summary():
    print("\n" + "=" * 55)
    print("MUTUAL FUND PIPELINE SUMMARY")
    print("=" * 55)
    print(f"✅ Loaded:   {len(results['success'])} schemes")
    print(f"⏭️  Skipped:  {len(results['skipped'])} (up to date or inactive)")
    print(f"❌ Failed:   {len(results['failed'])}")
    if results["failed"]:
        for f in results["failed"][:20]:
            print(f"   {f}")
        if len(results["failed"]) > 20:
            print(f"   ... and {len(results['failed']) - 20} more")
    print("=" * 55)


# ── Worker (one per thread) ────────────────────────────
def _worker(args):
    idx, scheme, watermarks, total = args
    code = int(scheme["schemeCode"])
    name = scheme["schemeName"]
    ch   = get_ch_client()  # each thread gets its own connection
    last_date = watermarks.get(code)
    process_scheme(ch, code, name, last_date, idx, total)
    time.sleep(DELAY_PER_REQUEST)


# ── Main ───────────────────────────────────────────────
def main():
    log.info("=== Mutual Fund Pipeline Starting ===")
    log.info(f"Start time: {datetime.now()}")

    ch = get_ch_client()

    # Verify tables exist — fail fast if migrations not run
    assert_tables_exist(ch)

    # Fetch and filter schemes
    schemes = fetch_all_schemes()
    total   = len(schemes)

    # Load schemes master (skips if already loaded)
    load_schemes_master(ch, schemes)

    # Fetch all watermarks in ONE query
    log.info("Fetching NAV watermarks from ClickHouse...")
    watermarks = fetch_all_nav_watermarks(ch)
    log.info(f"Got watermarks for {len(watermarks)} existing schemes")

    # Parallel NAV download
    log.info(f"\nDownloading NAV history for {total} schemes...")
    log.info(f"Running with {MAX_WORKERS} parallel workers")

    tasks = [
        (idx, scheme, watermarks, total)
        for idx, scheme in enumerate(schemes, 1)
    ]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(_worker, tasks)

    print_summary()
    log.info(f"End time: {datetime.now()}")


if __name__ == "__main__":
    main()