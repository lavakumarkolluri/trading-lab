#!/usr/bin/env python3
"""
scheduler.py
────────────
Docker-native cron scheduler for the trading pipeline (OPS-001).
Runs inside a long-lived container — no host cron required.

Schedule (all times UTC, IST = UTC+5:30):
  Daily    — Mon–Fri 11:00 UTC (16:30 IST)  → meta_pipeline (steps 1-12)
  Weekly   — Sun     00:30 UTC (06:00 IST)  → meta_pipeline --weekly (steps 13-16)
  Weekly   — Sun     01:00 UTC (06:30 IST)  → gap_analyzer
  Weekly   — Sun     02:00 UTC (07:30 IST)  → option_backtest (full 2yr refresh)
  Weekly   — Sun     03:00 UTC (08:30 IST)  → mf_pipeline (NAV refresh)
  Monthly  — 1st     04:00 UTC (09:30 IST)  → holidays_pipeline (seed next year)

Docker:
  docker compose up -d scheduler
"""

import os
import logging
import subprocess
import time
from datetime import datetime

import schedule

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

_COMPOSE_FILE = os.getenv("COMPOSE_FILE", "docker-compose.yml")
_PROJECT_DIR  = os.path.dirname(_COMPOSE_FILE) if "/" in _COMPOSE_FILE else "."

COMPOSE_CMD = [
    "docker", "compose",
    "-f", _COMPOSE_FILE,
    "--project-directory", _PROJECT_DIR,
    "run", "--rm",
]


def _run(service: str, *args: str):
    cmd = COMPOSE_CMD + [service] + list(args)
    log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=False, text=True, check=False)
    if result.returncode != 0:
        log.error("Command failed (exit %d): %s", result.returncode, " ".join(cmd))
    else:
        log.info("Done: %s", service)


def job_daily():
    log.info("=== Daily pipeline triggered ===")
    _run("meta_pipeline")


def job_weekly():
    log.info("=== Weekly refresh triggered ===")
    _run("meta_pipeline", "--weekly")


def job_gap_analyzer():
    log.info("=== Gap analyzer triggered ===")
    _run("gap_analyzer")


def job_option_backtest():
    log.info("=== Option backtest weekly refresh triggered ===")
    _run("option_backtest")                        # buy strategy
    _run("option_backtest", "--strategy", "sell")  # sell strategy


def job_mf_pipeline():
    log.info("=== MF pipeline weekly NAV refresh triggered ===")
    _run("mf_pipeline")


def job_holidays():
    """Run only on the 1st of each month."""
    if datetime.utcnow().day != 1:
        return
    log.info("=== Holidays pipeline monthly refresh triggered ===")
    _run("holidays_pipeline")


def main():
    log.info("Scheduler started — all times UTC")
    log.info("  Daily   pipeline    : Mon–Fri 11:00 UTC (16:30 IST)")
    log.info("  Weekly  refresh     : Sun     00:30 UTC (06:00 IST)")
    log.info("  Gap     analyzer    : Sun     01:00 UTC (06:30 IST)")
    log.info("  Option  backtest    : Sun     02:00 UTC (07:30 IST)")
    log.info("  MF      pipeline    : Sun     03:00 UTC (08:30 IST)")
    log.info("  Holidays pipeline   : 1st of month 04:00 UTC (09:30 IST)")

    schedule.every().monday.at("11:00").do(job_daily)
    schedule.every().tuesday.at("11:00").do(job_daily)
    schedule.every().wednesday.at("11:00").do(job_daily)
    schedule.every().thursday.at("11:00").do(job_daily)
    schedule.every().friday.at("11:00").do(job_daily)

    schedule.every().sunday.at("00:30").do(job_weekly)
    schedule.every().sunday.at("01:00").do(job_gap_analyzer)
    schedule.every().sunday.at("02:00").do(job_option_backtest)
    schedule.every().sunday.at("03:00").do(job_mf_pipeline)

    # Monthly: schedule runs daily at 04:00, guard inside job checks day==1
    schedule.every().day.at("04:00").do(job_holidays)

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
