#!/usr/bin/env python3
"""
pre_market_check.py
────────────────────
GO/NO-GO system readiness report at 08:30 IST (03:00 UTC) Mon–Fri.
Runs as a one-shot container via scheduler: _run("pre_market_check").
Sends a Telegram message with full system status before market opens at 09:15 IST.
ClickHouse being unreachable is itself a CRIT alert.
"""

import json
import os
import subprocess
import urllib.request
import zoneinfo
from datetime import date, datetime, timedelta
from typing import Optional

from ch_utils import ch_client as _ch_factory, write_alert_log
from logging_utils import get_logger

log = get_logger(__name__)

IST = zoneinfo.ZoneInfo("Asia/Kolkata")

_TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

_ch_ref = None  # set in main() so _send() can write to alert_log

SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]
YESTERDAY_PIPELINES = ["option_chain_historical", "compute_oi_features", "vix_pipeline"]


def _send(msg: str, level: str = "INFO") -> None:
    if _ch_ref is not None:
        write_alert_log(_ch_ref, "pre_market_check", level, msg)
    if not (_TG_TOKEN and _TG_CHAT_ID):
        log.info("Telegram not configured — would send: %s", msg[:80])
        return
    try:
        payload = json.dumps({"chat_id": _TG_CHAT_ID, "text": msg}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{_TG_TOKEN}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        log.warning("Telegram send failed: %s", e)


# ── Individual checks ─────────────────────────────────────────────────────────

def _check_clickhouse(ch) -> tuple[bool, str]:
    try:
        rows = ch.query("SELECT 1").result_rows
        if rows and rows[0][0] == 1:
            return True, "reachable"
        return False, "unexpected response"
    except Exception as e:
        return False, f"unreachable: {e}"


def _check_dashboard() -> tuple[bool, str]:
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format={{.State.Status}}", "trading-lab-dashboard-1"],
            capture_output=True, text=True, timeout=10,
        )
        status = result.stdout.strip()
        if status == "running":
            return True, "running"
        return False, f"status: {status or 'not found'}"
    except Exception as e:
        return False, f"inspect failed: {e}"


def _check_options_chain(ch) -> tuple[bool, str]:
    threshold = date.today() - timedelta(days=2)
    try:
        rows = ch.query(
            "SELECT symbol, max(toDate(timestamp)) AS last_date "
            "FROM market.options_chain FINAL "
            "WHERE toHour(timestamp) = 15 AND symbol IN {syms:Array(String)} "
            "GROUP BY symbol",
            parameters={"syms": SYMBOLS},
        ).result_rows
    except Exception as e:
        return False, f"query failed: {e}"

    found = {r[0]: r[1] for r in rows}
    stale = []
    for sym in SYMBOLS:
        last = found.get(sym)
        if last is None or last < threshold:
            age = (date.today() - last).days if last else "N/A"
            stale.append(f"{sym} stale (last: {last}, {age}d)")
    if stale:
        return False, "; ".join(stale)
    return True, "/".join(SYMBOLS) + " fresh"


def _check_vix(ch) -> tuple[bool, str]:
    threshold = date.today() - timedelta(days=2)
    try:
        rows = ch.query(
            "SELECT max(toDate(timestamp)) FROM market.nifty_live FINAL"
        ).result_rows
    except Exception as e:
        return False, f"query failed: {e}"
    if not rows or rows[0][0] is None:
        return False, "no data"
    last = rows[0][0]
    if last < threshold:
        age = (date.today() - last).days
        return False, f"stale (last: {last}, {age}d old)"
    return True, f"fresh ({last})"


def _check_ohlcv(ch) -> tuple[bool, str]:
    threshold = date.today() - timedelta(days=3)
    try:
        rows = ch.query(
            "SELECT market, max(date) AS max_date FROM market.ohlcv_daily FINAL "
            "GROUP BY market ORDER BY market"
        ).result_rows
    except Exception as e:
        return False, f"query failed: {e}"
    if not rows:
        return False, "no data"
    stale = []
    for mkt, last in rows:
        if last < threshold:
            age = (date.today() - last).days
            stale.append(f"{mkt} stale ({age}d)")
    if stale:
        return False, "; ".join(stale)
    latest_mkt, latest_date = max(rows, key=lambda r: r[1])
    return True, f"{latest_mkt} {latest_date}"


def _check_confidence_scores(ch) -> tuple[bool, str]:
    cutoff = date.today() - timedelta(days=1)
    try:
        rows = ch.query(
            "SELECT symbol, round(confidence, 0) AS conf "
            "FROM analysis.confidence_scores FINAL "
            "WHERE symbol IN {syms:Array(String)} AND score_date >= {cutoff:Date} "
            "ORDER BY symbol",
            parameters={"syms": SYMBOLS, "cutoff": cutoff},
        ).result_rows
    except Exception as e:
        return False, f"query failed: {e}"

    found = {r[0]: int(r[1]) for r in rows}
    missing = [sym for sym in SYMBOLS if sym not in found]
    if missing:
        return False, f"missing: {', '.join(missing)}"
    return True, " · ".join(f"{sym} {found[sym]}" for sym in SYMBOLS if sym in found)


def _check_yesterday_pipelines(ch) -> tuple[bool, str]:
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    not_run, failed = [], []
    try:
        for svc in YESTERDAY_PIPELINES:
            rows = ch.query(
                "SELECT status FROM system_meta.pipeline_runs FINAL "
                "WHERE service = {service:String} AND run_date = {run_date:Date} "
                "ORDER BY version DESC LIMIT 1",
                parameters={"service": svc, "run_date": yesterday},
            ).result_rows
            if not rows:
                not_run.append(svc)
            elif rows[0][0] != "success":
                failed.append(f"{svc}={rows[0][0]}")
    except Exception as e:
        return False, f"query failed: {e}"

    issues = not_run + failed
    if issues:
        return False, "; ".join(issues)
    return True, "all success"


def _check_open_positions(ch) -> tuple[bool, str]:
    try:
        rows = ch.query(
            "SELECT count() FROM market.open_positions FINAL WHERE status = 'open'"
        ).result_rows
    except Exception as e:
        return False, f"query failed: {e}"
    count = int(rows[0][0]) if rows else 0
    if count > 0:
        return False, f"{count} open position(s) found"
    return True, "none"


# ── Orchestration ─────────────────────────────────────────────────────────────

CHECKS: list[tuple[str, object]] = [
    ("ClickHouse",          _check_clickhouse),
    ("Dashboard",           _check_dashboard),
    ("Options chain",       _check_options_chain),
    ("VIX",                 _check_vix),
    ("OHLCV",               _check_ohlcv),
    ("Confidence scores",   _check_confidence_scores),
    ("Yesterday pipelines", _check_yesterday_pipelines),
    ("Open positions",      _check_open_positions),
]


def run_all_checks(ch) -> list[dict]:
    """Return list of {name, ok, detail} for each check."""
    results = []
    ch_ok = True
    for name, fn in CHECKS:
        if name == "Dashboard":
            ok, detail = fn()  # CH-independent
        elif not ch_ok:
            ok, detail = False, "CH unavailable"
        else:
            ok, detail = fn(ch)
            if name == "ClickHouse":
                ch_ok = ok
        results.append({"name": name, "ok": ok, "detail": detail})
    return results


def format_report(results: list[dict], check_date: date) -> str:
    date_str = check_date.strftime("%-d %b %Y")
    all_ok = all(r["ok"] for r in results)
    header = f"{'🟢' if all_ok else '🔴'} PRE-MARKET CHECK — {date_str} · 08:30 IST"
    lines = [header, "─" * 33]
    for r in results:
        icon = "✅" if r["ok"] else "❌"
        lines.append(f"{icon} {r['name']}: {r['detail']}")
    lines.append("─" * 33)
    if all_ok:
        lines.append("SYSTEM GO — ready for paper trades")
    else:
        issue_count = sum(1 for r in results if not r["ok"])
        lines.append(f"NOT READY — {issue_count} issue(s) require attention before trading")
    return "\n".join(lines)


def main():
    global _ch_ref
    log.info("Pre-market check starting...")
    try:
        ch = _ch_factory()
        _ch_ref = ch
    except Exception as e:
        msg = (
            "🔴 PRE-MARKET CHECK FAILED\n"
            f"Could not connect to ClickHouse: {e}\n"
            "NOT READY — manual intervention required."
        )
        log.error("ClickHouse connection failed: %s", e)
        _send(msg, level="CRIT")
        return

    results = run_all_checks(ch)
    report = format_report(results, date.today())
    all_ok = all(r["ok"] for r in results)
    _send(report, level="INFO" if all_ok else "CRIT")
    log.info("Pre-market check complete — %s", "GO" if all_ok else "NOT READY")


if __name__ == "__main__":
    main()
