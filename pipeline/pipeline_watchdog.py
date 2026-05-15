#!/usr/bin/env python3
"""
pipeline_watchdog.py
────────────────────
Continuous pipeline health daemon. Polls every 30 min:
  1. Detect: failed or missing pipeline_runs rows vs expected schedule
  2. Fix: re-trigger backfill via docker compose run --rm (max 2 retries/day)
  3. Verify: 25 min after fix, re-check freshness; send OK or escalation
  4. Pattern: if same service failed ≥3 times in 7d, flag as recurring

Writes to system_meta.alert_log after every Telegram send.
"""

import json
import os
import subprocess
import time
import urllib.request
import zoneinfo
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone
from typing import Optional

from ch_utils import ch_client as _ch_client, write_alert_log
from logging_utils import get_logger

log = get_logger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

LOOP_INTERVAL_S     = 30 * 60
VERIFY_DELAY_S      = 25 * 60
MAX_RETRIES         = 2
RECURRING_THRESHOLD = 3
RECURRING_WINDOW_DAYS = 7

IST = zoneinfo.ZoneInfo("Asia/Kolkata")

_COMPOSE_FILE = os.getenv("COMPOSE_FILE", "/trading-lab/docker-compose.yml")
_PROJECT_DIR  = os.path.dirname(_COMPOSE_FILE)
_COMPOSE_CMD  = [
    "docker", "compose", "-f", _COMPOSE_FILE,
    "--project-directory", _PROJECT_DIR,
    "run", "--rm",
]

_TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Expected UTC run times; alert only after expected_time + 30 min grace
EXPECTED_JOBS: dict[str, str] = {
    "option_chain_historical": "12:30",
    "compute_oi_features":     "13:00",
    "confidence_scorer":       "13:00",
    "strategy_selector":       "13:30",
    "vix_pipeline":            "12:30",
}

# Maps pipeline → which freshness check key applies; None = verify via pipeline_runs status
PIPELINE_TO_FRESHNESS: dict[str, Optional[str]] = {
    "option_chain_historical": "options_chain",
    "meta_pipeline":           "ohlcv",
    "vix_pipeline":            "vix",
    "mf_pipeline":             "mf_nav",
    "compute_oi_features":     "options_chain",
    "confidence_scorer":       None,
    "strategy_selector":       None,
}

# ── State ─────────────────────────────────────────────────────────────────────

@dataclass
class PendingVerification:
    service: str
    triggered_at: datetime
    freshness_target: Optional[str]
    error_msg: str

@dataclass
class WatchdogState:
    retry_counts: dict = field(default_factory=dict)           # (service, date_str) → int
    pending_verifications: dict = field(default_factory=dict)  # service → PendingVerification

# ── Telegram + alert_log ──────────────────────────────────────────────────────

_ch_ref = None  # set in run() so _send() can write to alert_log


def _send(msg: str, level: str = "WARN") -> None:
    if _ch_ref is not None:
        write_alert_log(_ch_ref, "pipeline_watchdog", level, msg)
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


# ── Detection ─────────────────────────────────────────────────────────────────

def _query_pipeline_status(ch, service: str, run_date: str) -> Optional[str]:
    """Return latest status for (service, run_date), or None if no row exists."""
    rows = ch.query(
        """
        SELECT status FROM system_meta.pipeline_runs FINAL
        WHERE service  = {service:String}
          AND run_date = {run_date:Date}
        ORDER BY version DESC LIMIT 1
        """,
        parameters={"service": service, "run_date": run_date},
    ).result_rows
    return rows[0][0] if rows else None


def _get_error_msg(ch, service: str, run_date: str) -> str:
    rows = ch.query(
        """
        SELECT error_msg FROM system_meta.pipeline_runs FINAL
        WHERE service  = {service:String}
          AND run_date = {run_date:Date}
          AND status   = 'failed'
        ORDER BY version DESC LIMIT 1
        """,
        parameters={"service": service, "run_date": run_date},
    ).result_rows
    return str(rows[0][0]) if rows else ""


def _detect_failed_runs(ch, today: str) -> list[dict]:
    """Return EXPECTED_JOBS services whose latest status today is 'failed'."""
    issues = []
    for service in EXPECTED_JOBS:
        status = _query_pipeline_status(ch, service, today)
        if status == "failed":
            issues.append({
                "service":   service,
                "status":    "failed",
                "error_msg": _get_error_msg(ch, service, today),
            })
    return issues


def _detect_missing_runs(ch, today: str, now_utc: datetime) -> list[dict]:
    """Return EXPECTED_JOBS services with no row today after expected_time + 30 min grace."""
    issues = []
    for service, expected_hhmm in EXPECTED_JOBS.items():
        exp_h, exp_m = map(int, expected_hhmm.split(":"))
        deadline = now_utc.replace(
            hour=exp_h, minute=exp_m, second=0, microsecond=0
        ) + timedelta(minutes=30)
        if now_utc < deadline:
            continue  # grace period not expired yet
        status = _query_pipeline_status(ch, service, today)
        if status is None:  # no row at all → missing
            issues.append({
                "service":   service,
                "status":    "missing",
                "error_msg": "",
            })
    return issues


def detect_issues(ch, today: str, now_utc: datetime) -> list[dict]:
    """Merge failed + missing; 'failed' wins deduplication."""
    failed  = _detect_failed_runs(ch, today)
    missing = _detect_missing_runs(ch, today, now_utc)
    seen    = {i["service"] for i in failed}
    result  = list(failed)
    for m in missing:
        if m["service"] not in seen:
            result.append(m)
    return result


# ── Freshness checks ──────────────────────────────────────────────────────────

def _is_ohlcv_fresh(ch) -> bool:
    threshold = date.today() - timedelta(days=3)
    rows = ch.query(
        "SELECT min(max_date) FROM ("
        "  SELECT max(date) AS max_date FROM market.ohlcv_daily FINAL GROUP BY market"
        ")"
    ).result_rows
    if not rows or rows[0][0] is None:
        return False
    return rows[0][0] >= threshold


def _is_mf_nav_fresh(ch) -> bool:
    threshold = date.today() - timedelta(days=5)
    rows = ch.query("SELECT max(date) FROM market.mf_nav FINAL").result_rows
    if not rows or rows[0][0] is None:
        return False
    return rows[0][0] >= threshold


def _is_vix_fresh(ch) -> bool:
    threshold = date.today() - timedelta(days=4)
    rows = ch.query(
        "SELECT max(toDate(timestamp)) FROM market.nifty_live FINAL"
    ).result_rows
    if not rows or rows[0][0] is None:
        return False
    return rows[0][0] >= threshold


def _is_options_chain_fresh(ch) -> bool:
    threshold = date.today() - timedelta(days=4)
    rows = ch.query(
        "SELECT min(last_date) FROM ("
        "  SELECT max(toDate(timestamp)) AS last_date"
        "  FROM market.options_chain FINAL"
        "  WHERE toHour(timestamp) = 15"
        "  GROUP BY symbol"
        ")"
    ).result_rows
    if not rows or rows[0][0] is None:
        return False
    return rows[0][0] >= threshold


FRESHNESS_CHECKS: dict[str, callable] = {
    "ohlcv":         _is_ohlcv_fresh,
    "mf_nav":        _is_mf_nav_fresh,
    "vix":           _is_vix_fresh,
    "options_chain": _is_options_chain_fresh,
}


def check_freshness_for_service(ch, service: str) -> Optional[bool]:
    """
    Return True/False if a data-table freshness check applies.
    Return None if freshness must be verified via pipeline_runs status instead.
    """
    key = PIPELINE_TO_FRESHNESS.get(service)
    if key is None:
        return None
    check_fn = FRESHNESS_CHECKS.get(key)
    if check_fn is None:
        return None
    try:
        return check_fn(ch)
    except Exception as e:
        log.warning("Freshness check error for %s: %s", service, e)
        return None


# ── Pattern tracking ──────────────────────────────────────────────────────────

def _count_recent_failures(ch, service: str, window_days: int = RECURRING_WINDOW_DAYS) -> int:
    rows = ch.query(
        """
        SELECT count() FROM system_meta.pipeline_runs FINAL
        WHERE service  = {service:String}
          AND status   = 'failed'
          AND run_date >= today() - {window:Int32}
        """,
        parameters={"service": service, "window": window_days},
    ).result_rows
    return int(rows[0][0]) if rows else 0


# ── Fix triggering ────────────────────────────────────────────────────────────

def _run_fix(service: str) -> tuple[bool, str]:
    cmd = _COMPOSE_CMD + [service]
    log.info("Watchdog fix: running %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=600,
        )
        return result.returncode == 0, result.stderr[-500:] if result.stderr else ""
    except subprocess.TimeoutExpired:
        return False, "timeout after 600s"
    except Exception as e:
        return False, str(e)


def attempt_fix(
    ch,
    issue: dict,
    state: WatchdogState,
    today: str,
    now_utc: datetime,
) -> None:
    service   = issue["service"]
    error_msg = issue["error_msg"]
    status    = issue["status"]
    key       = (service, today)

    retry_count = state.retry_counts.get(key, 0)
    if retry_count >= MAX_RETRIES:
        log.warning("Max retries (%d) exhausted for %s on %s — skipping", MAX_RETRIES, service, today)
        return

    state.retry_counts[key] = retry_count + 1
    fail_count = _count_recent_failures(ch, service)
    recurring  = fail_count >= RECURRING_THRESHOLD

    msg_lines = [
        f"Pipeline issue detected: {service} ({status})",
        f"Retry: {retry_count + 1}/{MAX_RETRIES}",
    ]
    if error_msg:
        msg_lines.append(f"Error: {error_msg[:200]}")
    msg_lines.append(f"Failures in last {RECURRING_WINDOW_DAYS}d: {fail_count}")
    if recurring:
        msg_lines.append("Recurring failure — consider investigating root cause")
    msg_lines.append("Action: triggering backfill pipeline...")

    _send("\n".join(msg_lines), level="CRIT" if recurring else "WARN")

    ok, stderr = _run_fix(service)
    if not ok:
        log.error("Fix pipeline %s failed: %s", service, stderr[:200])

    state.pending_verifications[service] = PendingVerification(
        service=service,
        triggered_at=now_utc,
        freshness_target=PIPELINE_TO_FRESHNESS.get(service),
        error_msg=error_msg,
    )
    log.info("Scheduled verification for %s in %d min", service, VERIFY_DELAY_S // 60)


# ── Verification ──────────────────────────────────────────────────────────────

def run_pending_verifications(
    ch,
    state: WatchdogState,
    today: str,
    now_utc: datetime,
) -> None:
    to_remove = []
    for service, pv in state.pending_verifications.items():
        elapsed = (now_utc - pv.triggered_at).total_seconds()
        if elapsed < VERIFY_DELAY_S:
            continue

        fresh = check_freshness_for_service(ch, service)
        if fresh is None:
            # Fall back: check pipeline_runs status
            status = _query_pipeline_status(ch, service, today)
            fresh = (status == "success")

        if fresh:
            _send(f"Fixed: {service} — data is now fresh", level="INFO")
            log.info("Verification OK: %s recovered", service)
        else:
            _send(
                f"Fix failed: {service} — data still stale after backfill\n"
                "Manual intervention required.",
                level="CRIT",
            )
            log.error("Verification FAILED: %s still stale", service)

        to_remove.append(service)

    for service in to_remove:
        del state.pending_verifications[service]


# ── Main loop ─────────────────────────────────────────────────────────────────

def _tick(ch, state: WatchdogState) -> None:
    now_utc = datetime.now(timezone.utc)
    today   = now_utc.strftime("%Y-%m-%d")

    run_pending_verifications(ch, state, today, now_utc)

    try:
        issues = detect_issues(ch, today, now_utc)
    except Exception as e:
        log.error("detect_issues failed: %s", e)
        return

    for issue in issues:
        if issue["service"] not in state.pending_verifications:
            attempt_fix(ch, issue, state, today, now_utc)

    log.info(
        "Watchdog tick — %d issue(s) found, %d pending verification(s)",
        len(issues), len(state.pending_verifications),
    )


def run() -> None:
    log.info("Pipeline Watchdog started — polling every %d min", LOOP_INTERVAL_S // 60)
    global _ch_ref
    ch = _ch_client()
    _ch_ref = ch
    state = WatchdogState()

    while True:
        try:
            _tick(ch, state)
        except Exception as e:
            log.error("Watchdog tick failed: %s", e, exc_info=True)
        time.sleep(LOOP_INTERVAL_S)


if __name__ == "__main__":
    run()
