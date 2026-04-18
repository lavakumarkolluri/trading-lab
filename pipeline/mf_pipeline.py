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

import io
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
from minio import Minio
from minio.error import S3Error


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

MINIO_HOST   = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER   = os.getenv("MINIO_USER", "admin")
MINIO_PASS   = os.getenv("MINIO_PASSWORD", "")
MINIO_BUCKET = "trading-data"

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


def get_minio_client() -> Minio:
    return Minio(MINIO_HOST, access_key=MINIO_USER,
                 secret_key=MINIO_PASS, secure=False)


def setup_minio_bucket(mc: Minio):
    if not mc.bucket_exists(MINIO_BUCKET):
        mc.make_bucket(MINIO_BUCKET)


def _nav_key(scheme_code: int) -> str:
    return f"mf/nav/{scheme_code}.parquet"


def load_nav_from_minio(mc: Minio, scheme_code: int) -> pd.DataFrame:
    try:
        resp = mc.get_object(MINIO_BUCKET, _nav_key(scheme_code))
        df = pd.read_parquet(io.BytesIO(resp.read()))
        resp.close()
        return df
    except S3Error:
        return pd.DataFrame()


def save_nav_to_minio(mc: Minio, scheme_code: int, df: pd.DataFrame):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    mc.put_object(MINIO_BUCKET, _nav_key(scheme_code), buf,
                  buf.getbuffer().nbytes,
                  content_type="application/octet-stream")


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
def fetch_schemes_from_db(ch) -> list[dict]:
    """
    Read scheme list from market.mf_schemes instead of hitting mfapi.in.
    Falls back to API only if DB is empty.
    """
    result = ch.query(
        "SELECT scheme_code, scheme_name FROM market.mf_schemes ORDER BY scheme_code"
    )
    if result.result_rows:
        log.info(f"Loaded {len(result.result_rows)} schemes from DB (skipping API)")
        return [{"schemeCode": str(row[0]), "schemeName": row[1]}
                for row in result.result_rows]

    log.info("DB empty — falling back to mfapi.in...")
    return fetch_all_schemes_from_api()  # rename old function


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
def process_scheme(ch, mc, scheme_code, scheme_name, last_date, idx, total):
    try:
        today  = date.today()
        cutoff = today - timedelta(days=INACTIVITY_DAYS)

        # Load existing NAV from MinIO to get accurate watermark
        cached = load_nav_from_minio(mc, scheme_code)
        minio_last = None
        if not cached.empty and "date" in cached.columns:
            minio_last = pd.to_datetime(cached["date"]).dt.date.max()

        # Use the later of DB watermark and MinIO watermark
        effective_last = max(
            filter(None, [last_date, minio_last])
        ) if (last_date or minio_last) else None

        if effective_last and effective_last >= today:
            with results_lock:
                results["skipped"].append(scheme_code)
            return

        nav_list = fetch_nav_history(scheme_code)
        if not nav_list:
            raise ValueError("No NAV data returned")

        # Check if fund is inactive
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

        rows = parse_nav_data(scheme_code, nav_list, effective_last)
        if not rows:
            with results_lock:
                results["skipped"].append(scheme_code)
            return

        # Insert new rows to ClickHouse
        insert_nav_rows(ch, rows)

        # Merge with cached and save back to MinIO
        new_df = pd.DataFrame(rows)
        merged = pd.concat([cached, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["date", "scheme_code"], keep="last")
        save_nav_to_minio(mc, scheme_code, merged)

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
    ch   = get_ch_client()
    mc   = get_minio_client()
    last_date = watermarks.get(code)
    process_scheme(ch, mc, code, name, last_date, idx, total)
    time.sleep(DELAY_PER_REQUEST)


# ── Main ───────────────────────────────────────────────
def main():
    log.info("=== Mutual Fund Pipeline Starting ===")
    log.info(f"Start time: {datetime.now()}")

    ch = get_ch_client()
    mc = get_minio_client()
    setup_minio_bucket(mc)

    # Verify tables exist — fail fast if migrations not run
    assert_tables_exist(ch)

    # Fetch and filter schemes
    schemes = fetch_schemes_from_db(ch)
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