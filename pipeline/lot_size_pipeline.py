#!/usr/bin/env python3
"""
lot_size_pipeline.py
─────────────────────
Extracts F&O lot size history from NSE bhavcopy files stored in MinIO,
then loads change events into market.fo_lot_sizes in ClickHouse.

Source: bhavcopy/fo/*.zip in MinIO → NewBrdLotQty column.
One ClickHouse row per (symbol, effective_from) when lot_size changes.

Usage:
  python lot_size_pipeline.py          # incremental (only new bhavcopy files)
  python lot_size_pipeline.py --full   # reprocess all files
  python lot_size_pipeline.py --status # show table summary

Docker:
  docker compose run --rm lot_size_pipeline
"""

import io
import os
import zipfile
import logging
import argparse
from datetime import datetime, date

import pandas as pd
import clickhouse_connect
from minio import Minio

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
MINIO_PREFIX = "bhavcopy/fo/"


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


def get_mc():
    return Minio(MINIO_HOST, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)


def last_loaded_date(ch) -> date | None:
    try:
        r = ch.query("SELECT max(effective_from) FROM market.fo_lot_sizes FINAL")
        val = r.result_rows[0][0]
        return val if val and val > date(2000, 1, 1) else None
    except Exception:
        return None


def extract_lot_sizes(zip_bytes: bytes, trade_date: date) -> dict[str, int]:
    """
    Return {symbol: lot_size} from a single bhavcopy zip.
    Uses the MINIMUM lot size per symbol — on transition days, old long-dated
    contracts retain the previous lot size while new near-term contracts already
    have the new smaller size. Min gives the effective size for new trades.
    """
    try:
        zf  = zipfile.ZipFile(io.BytesIO(zip_bytes))
        df  = pd.read_csv(zf.open(zf.namelist()[0]), low_memory=False)
        if "NewBrdLotQty" not in df.columns or "TckrSymb" not in df.columns:
            return {}
        valid = df[df["NewBrdLotQty"].notna() & (df["NewBrdLotQty"] > 0)].copy()
        valid["TckrSymb"] = valid["TckrSymb"].str.strip()
        return {
            sym: int(grp["NewBrdLotQty"].min())
            for sym, grp in valid.groupby("TckrSymb")
        }
    except Exception as e:
        log.warning(f"{trade_date}: failed to parse — {e}")
        return {}


def build_change_events(daily: list[tuple[date, dict[str, int]]]) -> list[dict]:
    """
    Given [(date, {symbol: lot_size})] sorted by date asc,
    return change-event rows: one row per (symbol, date) when lot_size first appears or changes.
    """
    current: dict[str, int] = {}
    events: list[dict] = []
    now = datetime.utcnow()
    version = int(now.timestamp())

    for trade_date, sym_lots in daily:
        for sym, ls in sym_lots.items():
            if current.get(sym) != ls:
                events.append({
                    "symbol":         sym,
                    "effective_from": trade_date,
                    "lot_size":       ls,
                    "computed_at":    now,
                    "version":        version,
                })
                current[sym] = ls

    return events


def show_status(ch):
    r = ch.query("""
        SELECT count(), countDistinct(symbol), min(effective_from), max(effective_from)
        FROM market.fo_lot_sizes FINAL
    """)
    cnt, syms, mn, mx = r.result_rows[0]
    print(f"  market.fo_lot_sizes: {cnt} change-events, {syms} symbols, {mn} → {mx}")

    r2 = ch.query("""
        SELECT symbol, effective_from, lot_size
        FROM market.fo_lot_sizes FINAL
        WHERE symbol IN ('NIFTY','BANKNIFTY','FINNIFTY','MIDCPNIFTY')
        ORDER BY symbol, effective_from
    """)
    for row in r2.result_rows:
        print(f"    {row[0]:<15} from {row[1]}  lot={row[2]}")


def main():
    parser = argparse.ArgumentParser(description="F&O lot size pipeline — bhavcopy → ClickHouse")
    parser.add_argument("--full",   action="store_true", help="Reprocess all bhavcopy files")
    parser.add_argument("--status", action="store_true", help="Show table summary")
    args = parser.parse_args()

    log.info("=== Lot Size Pipeline ===")
    ch = get_ch()
    mc = get_mc()

    if args.status:
        show_status(ch)
        return

    since = None if args.full else last_loaded_date(ch)
    if since:
        log.info(f"Incremental mode — processing files after {since}")
    else:
        log.info("Full mode — processing all bhavcopy files")

    # List all bhavcopy zips sorted by date
    objs = sorted(
        mc.list_objects(MINIO_BUCKET, prefix=MINIO_PREFIX, recursive=True),
        key=lambda o: o.object_name
    )
    log.info(f"Found {len(objs)} bhavcopy files in MinIO")

    daily: list[tuple[date, dict[str, int]]] = []
    for obj in objs:
        # Object name: bhavcopy/fo/YYYYMMDD.zip
        name = obj.object_name.split("/")[-1].replace(".zip", "")
        try:
            trade_date = datetime.strptime(name, "%Y%m%d").date()
        except ValueError:
            continue
        if since and trade_date <= since:
            continue

        resp      = mc.get_object(MINIO_BUCKET, obj.object_name)
        zip_bytes = resp.read()
        resp.close()

        sym_lots = extract_lot_sizes(zip_bytes, trade_date)
        if sym_lots:
            daily.append((trade_date, sym_lots))

    log.info(f"Parsed {len(daily)} files")

    if not daily:
        log.info("Nothing new to process")
        return

    events = build_change_events(daily)
    log.info(f"Found {len(events)} lot-size change events")

    if events:
        ch.insert_df("market.fo_lot_sizes", pd.DataFrame(events))
        log.info(f"Inserted {len(events)} rows → market.fo_lot_sizes")

    show_status(ch)
    log.info("Lot size pipeline complete ✅")


if __name__ == "__main__":
    main()
