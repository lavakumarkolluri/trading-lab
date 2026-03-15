import requests
import clickhouse_connect
import pandas as pd
import os
import logging
import time
from datetime import datetime, date

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
DELAY_PER_REQUEST = 0.3   # seconds between API calls
BATCH_SIZE        = 500   # insert rows to ClickHouse in batches

# ── Results Tracker ────────────────────────────────────
results = {"success": [], "skipped": [], "failed": []}


# ── ClickHouse Client ──────────────────────────────────
def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASS
    )


# ── Ensure Tables Exist ────────────────────────────────
def setup_tables(ch):
    ch.command("""
        CREATE TABLE IF NOT EXISTS market.mf_schemes
        (
            scheme_code     UInt32,
            scheme_name     String,
            fund_house      String,
            scheme_type     String,
            scheme_category String,
            is_active       UInt8,
            added_on        Date
        )
        ENGINE = ReplacingMergeTree
        ORDER BY scheme_code
    """)
    log.info("Table ready: market.mf_schemes")

    ch.command("""
        CREATE TABLE IF NOT EXISTS market.mf_nav
        (
            date        Date,
            scheme_code UInt32,
            nav         Float64,
            version     UInt64
        )
        ENGINE = ReplacingMergeTree(version)
        ORDER BY (scheme_code, date)
    """)
    log.info("Table ready: market.mf_nav")


# ── Fetch All Schemes from MFAPI ───────────────────────
def fetch_all_schemes():
    log.info("Fetching list of all mutual fund schemes from mfapi.in...")
    try:
        resp = requests.get(MFAPI_BASE, timeout=30)
        resp.raise_for_status()
        schemes = resp.json()
        log.info(f"Found {len(schemes)} schemes")
        return schemes
    except Exception as e:
        log.error(f"Failed to fetch scheme list: {e}")
        raise


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
    # Fallback: first word(s) before first dash or bracket
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
        "ELSS":             "ELSS",
        "LIQUID":           "Liquid",
        "OVERNIGHT":        "Overnight",
        "ULTRA SHORT":      "Ultra Short Duration",
        "LOW DURATION":     "Low Duration",
        "SHORT DURATION":   "Short Duration",
        "MEDIUM DURATION":  "Medium Duration",
        "LONG DURATION":    "Long Duration",
        "GILT":             "Gilt",
        "CREDIT RISK":      "Credit Risk",
        "CORPORATE BOND":   "Corporate Bond",
        "BANKING AND PSU":  "Banking & PSU",
        "DYNAMIC BOND":     "Dynamic Bond",
        "FLOATING RATE":    "Floating Rate",
        "ARBITRAGE":        "Arbitrage",
        "MULTI ASSET":      "Multi Asset",
        "BALANCED ADVANTAGE":"Balanced Advantage",
        "AGGRESSIVE HYBRID":"Aggressive Hybrid",
        "CONSERVATIVE HYBRID":"Conservative Hybrid",
        "EQUITY SAVINGS":   "Equity Savings",
        "LARGE AND MID":    "Large & Mid Cap",
        "LARGE CAP":        "Large Cap",
        "MID CAP":          "Mid Cap",
        "SMALL CAP":        "Small Cap",
        "MULTI CAP":        "Multi Cap",
        "FLEXI CAP":        "Flexi Cap",
        "FOCUSED":          "Focused",
        "CONTRA":           "Contra",
        "VALUE":            "Value",
        "DIVIDEND YIELD":   "Dividend Yield",
        "THEMATIC":         "Thematic",
        "SECTORAL":         "Sectoral",
        "INDEX":            "Index",
        "ETF":              "ETF",
        "GOLD":             "Gold",
        "SILVER":           "Silver",
        "FUND OF FUND":     "Fund of Funds",
        "FOF":              "Fund of Funds",
        "INTERNATIONAL":    "International",
        "OVERSEAS":         "International",
    }
    for key, cat in cats.items():
        if key in name_upper:
            return cat
    return "Other"


# ── Load Schemes Master into ClickHouse ────────────────
def load_schemes_master(ch, schemes):
    log.info("Loading scheme master list into ClickHouse...")
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
    log.info(f"Loaded {len(df)} schemes into market.mf_schemes")


# ── Get Last NAV Date for a Scheme ────────────────────
def get_last_nav_date(ch, scheme_code):
    result = ch.query(f"""
        SELECT max(date) FROM market.mf_nav
        WHERE scheme_code = {scheme_code}
    """)
    return result.result_rows[0][0]  # None if not loaded yet


# ── Fetch NAV History for One Scheme ──────────────────
def fetch_nav_history(scheme_code):
    url  = f"{MFAPI_BASE}/{scheme_code}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])   # list of {date, nav}


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
def process_scheme(ch, scheme_code, scheme_name, idx, total):
    try:
        last_date = get_last_nav_date(ch, scheme_code)
        today     = date.today()

        if last_date and last_date >= today:
            results["skipped"].append(scheme_code)
            return

        nav_list = fetch_nav_history(scheme_code)
        if not nav_list:
            raise ValueError(f"No NAV data returned")

        rows = parse_nav_data(scheme_code, nav_list, last_date)
        if not rows:
            results["skipped"].append(scheme_code)
            return

        insert_nav_rows(ch, rows)
        results["success"].append(scheme_code)

        if idx % 100 == 0:
            log.info(f"  Progress: {idx}/{total} | "
                     f"✅ {len(results['success'])} | "
                     f"⏭️  {len(results['skipped'])} | "
                     f"❌ {len(results['failed'])}")

    except Exception as e:
        log.error(f"  FAILED [{scheme_code}] {scheme_name[:40]}: {e}")
        results["failed"].append(f"{scheme_code} → {e}")


# ── Print Summary ──────────────────────────────────────
def print_summary():
    print("\n" + "=" * 55)
    print("MUTUAL FUND PIPELINE SUMMARY")
    print("=" * 55)
    print(f"✅ Loaded:   {len(results['success'])} schemes")
    print(f"⏭️  Skipped:  {len(results['skipped'])} (already up to date)")
    print(f"❌ Failed:   {len(results['failed'])}")
    if results["failed"]:
        for f in results["failed"][:20]:   # show first 20 only
            print(f"   {f}")
        if len(results["failed"]) > 20:
            print(f"   ... and {len(results['failed']) - 20} more")
    print("=" * 55)


# ── Main ───────────────────────────────────────────────
def main():
    log.info("=== Mutual Fund Pipeline Starting ===")
    log.info(f"Start time: {datetime.now()}")

    ch = get_ch_client()
    setup_tables(ch)

    # Step 1 — fetch all scheme codes
    schemes = fetch_all_schemes()
    total   = len(schemes)

    # Step 2 — load/refresh schemes master table
    load_schemes_master(ch, schemes)

    # Step 3 — download NAV history for each scheme
    log.info(f"\nDownloading NAV history for {total} schemes...")
    log.info("This will take ~2-3 hours on first run (17000 schemes)")
    log.info("On daily runs it will be fast (only new NAVs)\n")

    for idx, scheme in enumerate(schemes, 1):
        code = int(scheme["schemeCode"])
        name = scheme["schemeName"]
        process_scheme(ch, code, name, idx, total)
        time.sleep(DELAY_PER_REQUEST)

    print_summary()
    log.info(f"End time: {datetime.now()}")


if __name__ == "__main__":
    main()
