#!/usr/bin/env python3
"""
data_freshness_check.py
───────────────────────
Daily data quality watchdog. Checks every market's latest date against
expected freshness thresholds, alerts via Telegram, and auto-triggers
backfill pipelines when data is stale.

Run after the EOD pipeline (14:30 UTC = 20:00 IST) so today's data
should already be loaded.

Usage:
  python data_freshness_check.py          # check and auto-fix
  python data_freshness_check.py --report # report only, no fixes
"""

import os
import logging
import argparse
import subprocess
from datetime import datetime, date, timedelta

import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

_COMPOSE_FILE = os.getenv("COMPOSE_FILE", "/trading-lab/docker-compose.yml")
_PROJECT_DIR  = os.path.dirname(_COMPOSE_FILE)
_COMPOSE_CMD  = ["docker", "compose", "-f", _COMPOSE_FILE, "--project-directory", _PROJECT_DIR, "run", "--rm"]

_TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def _send_telegram(msg: str):
    if not (_TG_TOKEN and _TG_CHAT_ID):
        return
    try:
        import urllib.request, json as _json
        payload = _json.dumps({"chat_id": _TG_CHAT_ID, "text": msg}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{_TG_TOKEN}/sendMessage",
            data=payload, headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        log.warning("Telegram alert failed: %s", e)


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


def _run_pipeline(service: str, *args):
    cmd = _COMPOSE_CMD + [service] + list(args)
    log.info("Auto-fix: running %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=False, text=True, check=False)
    return result.returncode == 0


# ── Freshness rules ──────────────────────────────────────────────────────────
# Each entry: (label, query, max_stale_days, fix_fn)
# fix_fn is called when stale; None = alert only.

def _check_ohlcv(ch) -> list[dict]:
    """Check each market's max date in ohlcv_daily."""
    rows = ch.query(
        "SELECT market, max(date) as last_date "
        "FROM market.ohlcv_daily FINAL GROUP BY market"
    ).result_rows
    issues = []
    # Markets close Fri; allow 3 calendar days (Fri→Mon gap)
    threshold = date.today() - timedelta(days=3)
    for market, last_date in rows:
        if last_date and last_date < threshold:
            days_stale = (date.today() - last_date).days
            issues.append({
                "label": f"ohlcv_daily[{market}]",
                "last_date": last_date,
                "days_stale": days_stale,
                "fix": "meta_pipeline",
            })
    return issues


def _check_mf_nav(ch) -> list[dict]:
    """Check MF NAV max date."""
    rows = ch.query("SELECT max(date) FROM market.mf_nav FINAL").result_rows
    last_date = rows[0][0] if rows else None
    threshold = date.today() - timedelta(days=5)  # weekend+holiday tolerance
    if last_date and last_date < threshold:
        return [{
            "label": "mf_nav",
            "last_date": last_date,
            "days_stale": (date.today() - last_date).days,
            "fix": "mf_pipeline",
        }]
    return []


def _check_vix(ch) -> list[dict]:
    """Check VIX / nifty_live max date."""
    rows = ch.query("SELECT max(toDate(timestamp)) FROM market.nifty_live FINAL").result_rows
    last_date = rows[0][0] if rows else None
    threshold = date.today() - timedelta(days=4)
    if last_date and last_date < threshold:
        return [{
            "label": "nifty_live (VIX)",
            "last_date": last_date,
            "days_stale": (date.today() - last_date).days,
            "fix": "vix_pipeline",
        }]
    return []


def _check_options_chain(ch) -> list[dict]:
    """Check options chain EOD rows max date."""
    rows = ch.query(
        "SELECT symbol, max(toDate(timestamp)) as last_date "
        "FROM market.options_chain FINAL "
        "WHERE toHour(timestamp) = 15 GROUP BY symbol"
    ).result_rows
    issues = []
    threshold = date.today() - timedelta(days=4)
    for symbol, last_date in rows:
        if last_date and last_date < threshold:
            issues.append({
                "label": f"options_chain[{symbol}]",
                "last_date": last_date,
                "days_stale": (date.today() - last_date).days,
                "fix": "option_chain_historical",
            })
    return issues


def _check_per_symbol_staleness(ch) -> list[dict]:
    """Find individual symbols lagging >10 days behind their market's max date."""
    rows = ch.query("""
        SELECT market, symbol, last_date, market_max,
               toInt32(dateDiff('day', last_date, market_max)) as days_behind
        FROM (
          SELECT market, symbol, max(date) as last_date
          FROM market.ohlcv_daily FINAL GROUP BY market, symbol
        ) t
        JOIN (
          SELECT market, max(date) as market_max
          FROM market.ohlcv_daily FINAL GROUP BY market
        ) m USING (market)
        WHERE dateDiff('day', last_date, market_max) > 10
        ORDER BY market, days_behind DESC
        LIMIT 100
    """).result_rows
    issues = []
    for market, symbol, last_date, market_max, days_behind in rows:
        issues.append({
            "label": f"stale_symbol[{market}/{symbol}]",
            "last_date": last_date,
            "days_stale": days_behind,
            "fix": None,  # logged only; manual investigation needed
        })
    return issues


def main():
    parser = argparse.ArgumentParser(description="Data freshness watchdog")
    parser.add_argument("--report", action="store_true",
                        help="Report only — do not trigger auto-fix pipelines")
    args = parser.parse_args()

    log.info("=== Data Freshness Check ===")
    ch = get_ch()

    all_issues = []
    all_issues.extend(_check_ohlcv(ch))
    all_issues.extend(_check_mf_nav(ch))
    all_issues.extend(_check_vix(ch))
    all_issues.extend(_check_options_chain(ch))
    all_issues.extend(_check_per_symbol_staleness(ch))

    if not all_issues:
        log.info("✅ All data sources are fresh — no action needed")
        return

    # Separate auto-fixable from report-only
    fixable = [i for i in all_issues if i["fix"] and not args.report]
    report_only = [i for i in all_issues if not i["fix"] or args.report]

    for issue in all_issues:
        log.warning("STALE: %s — last=%s (%d days ago)",
                    issue["label"], issue["last_date"], issue["days_stale"])

    # Auto-fix: run each unique pipeline once
    fixed_services = set()
    for issue in fixable:
        svc = issue["fix"]
        if svc not in fixed_services:
            log.info("Auto-fixing: running %s", svc)
            ok = _run_pipeline(svc)
            fixed_services.add(svc)
            if ok:
                log.info("  ✅ %s completed", svc)
            else:
                log.error("  ❌ %s failed", svc)

    # Alert via Telegram
    stale_lines = "\n".join(
        f"• {i['label']}: {i['last_date']} ({i['days_stale']}d ago)"
        for i in all_issues[:15]  # cap to avoid huge messages
    )
    auto_fix_note = f"\nAuto-triggered: {', '.join(fixed_services)}" if fixed_services else ""
    _send_telegram(
        f"⚠️ Data freshness issues ({len(all_issues)} found):\n"
        f"{stale_lines}"
        f"{auto_fix_note}"
    )

    if report_only and not args.report:
        log.warning("%d symbol-level issues need manual investigation (see logs)", len(report_only))

    log.info("Freshness check complete — %d issues found, %d auto-fixed",
             len(all_issues), len(fixed_services))


if __name__ == "__main__":
    main()
