#!/usr/bin/env python3
"""
cleanup_schemes.py
──────────────────
One-time script to remove unwanted MF schemes from ClickHouse.

KEEP criteria (any one must match):
  1. Name contains "DIRECT" AND "GROWTH"
  2. Name contains "ETF"
  3. Name contains "FUND OF FUND" or " FOF"
  4. Name contains "OVERSEAS" or "INTERNATIONAL" or "GLOBAL"

REMOVE additionally if:
  - Last NAV date is older than 10 days (inactive fund)

Tables cleaned:
  - market.mf_schemes
  - market.mf_nav
  - market.mf_nav_enriched

Usage:
  python cleanup_schemes.py           # dry run — shows what will be deleted
  python cleanup_schemes.py --execute # actually deletes
"""

import os
import sys
import argparse
import logging
from datetime import datetime, date, timedelta
import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

INACTIVITY_DAYS = 10  # remove if last NAV older than this


def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


def is_relevant_scheme(name: str) -> bool:
    """
    Returns True if the scheme should be KEPT.
    """
    n = name.upper()
    is_direct_growth = "DIRECT" in n and "GROWTH" in n
    is_etf           = "ETF" in n
    is_fof           = "FUND OF FUND" in n or " FOF" in n or n.startswith("FOF ")
    is_overseas      = any(x in n for x in ["OVERSEAS", "INTERNATIONAL", "GLOBAL"])
    return is_direct_growth or is_etf or is_fof or is_overseas


def main():
    parser = argparse.ArgumentParser(description="Cleanup unwanted MF schemes")
    parser.add_argument("--execute", action="store_true",
                        help="Actually delete. Without this flag, runs as dry-run.")
    args = parser.parse_args()

    dry_run = not args.execute

    if dry_run:
        log.info("=== DRY RUN — no data will be deleted ===")
        log.info("Run with --execute to actually delete.")
    else:
        log.info("=== EXECUTE MODE — data will be permanently deleted ===")

    ch = get_ch_client()
    log.info("ClickHouse connected ✅")

    # ── Step 1: Fetch all schemes ──────────────────────
    log.info("Fetching all schemes from market.mf_schemes...")
    result = ch.query(
        "SELECT scheme_code, scheme_name FROM market.mf_schemes"
    )
    all_schemes = {row[0]: row[1] for row in result.result_rows}
    log.info(f"Total schemes in DB: {len(all_schemes)}")

    # ── Step 2: Apply name filter ──────────────────────
    relevant_by_name   = {c for c, n in all_schemes.items() if is_relevant_scheme(n)}
    irrelevant_by_name = set(all_schemes.keys()) - relevant_by_name
    log.info(f"Relevant by name   : {len(relevant_by_name)}")
    log.info(f"Irrelevant by name : {len(irrelevant_by_name)}")

    # ── Step 3: Apply inactivity filter ───────────────
    log.info(f"Checking last NAV date for relevant schemes (inactive = no NAV in {INACTIVITY_DAYS} days)...")
    cutoff_date = date.today() - timedelta(days=INACTIVITY_DAYS)

    result = ch.query(
        "SELECT scheme_code, max(date) as last_nav "
        "FROM market.mf_nav "
        "GROUP BY scheme_code"
    )
    last_nav_dates = {row[0]: row[1] for row in result.result_rows}

    inactive = set()
    for code in relevant_by_name:
        last_date = last_nav_dates.get(code)
        if last_date is None or last_date < cutoff_date:
            inactive.add(code)

    log.info(f"Inactive (no NAV in {INACTIVITY_DAYS} days): {len(inactive)}")

    # ── Step 4: Final delete list ──────────────────────
    to_delete = irrelevant_by_name | inactive
    to_keep   = set(all_schemes.keys()) - to_delete

    log.info(f"\n{'='*55}")
    log.info(f"Schemes to KEEP   : {len(to_keep)}")
    log.info(f"Schemes to DELETE : {len(to_delete)}")
    log.info(f"  → Irrelevant by name : {len(irrelevant_by_name)}")
    log.info(f"  → Inactive funds     : {len(inactive)}")
    log.info(f"{'='*55}\n")

    # ── Step 5: Show sample of what will be deleted ────
    log.info("Sample schemes being DELETED:")
    for code in list(to_delete)[:10]:
        log.info(f"  [{code}] {all_schemes.get(code, 'unknown')}")

    log.info("\nSample schemes being KEPT:")
    for code in list(to_keep)[:10]:
        log.info(f"  [{code}] {all_schemes.get(code, 'unknown')}")

    if dry_run:
        log.info("\nDry run complete. Run with --execute to delete.")
        return

    # ── Step 6: Execute deletes ────────────────────────
    if not to_delete:
        log.info("Nothing to delete.")
        return

    # Convert to comma-separated string for IN clause
    delete_codes = ",".join(str(c) for c in to_delete)

    log.info(f"\nDeleting {len(to_delete)} schemes from market.mf_schemes...")
    ch.command(
        f"ALTER TABLE market.mf_schemes DELETE "
        f"WHERE scheme_code IN ({delete_codes}) "
        f"SETTINGS mutations_sync = 1"
    )
    log.info("✅ market.mf_schemes cleaned")

    log.info(f"Deleting NAV rows from market.mf_nav...")
    ch.command(
        f"ALTER TABLE market.mf_nav DELETE "
        f"WHERE scheme_code IN ({delete_codes}) "
        f"SETTINGS mutations_sync = 1"
    )
    log.info("✅ market.mf_nav cleaned")

    log.info(f"Deleting enriched rows from market.mf_nav_enriched...")
    ch.command(
        f"ALTER TABLE market.mf_nav_enriched DELETE "
        f"WHERE scheme_code IN ({delete_codes}) "
        f"SETTINGS mutations_sync = 1"
    )
    log.info("✅ market.mf_nav_enriched cleaned")

    # ── Step 7: Verify ─────────────────────────────────
    log.info("\nVerifying cleanup...")
    r1 = ch.query("SELECT count() FROM market.mf_schemes").result_rows[0][0]
    r2 = ch.query("SELECT count(DISTINCT scheme_code) FROM market.mf_nav").result_rows[0][0]
    log.info(f"market.mf_schemes         : {r1} schemes remaining")
    log.info(f"market.mf_nav             : {r2} distinct schemes remaining")
    log.info("\n✅ Cleanup complete.")


if __name__ == "__main__":
    main()