#!/usr/bin/env python3
"""
scheduler.py
────────────
Docker-native cron scheduler for the trading pipeline (OPS-001).
Runs inside a long-lived container — no host cron required.

Schedule:
  Daily    — Monday–Friday at 16:30 IST (11:00 UTC)   → meta_pipeline daily
  Weekly   — Sunday        at 06:00 IST (00:30 UTC)   → meta_pipeline --weekly
  Weekly   — Sunday        at 06:30 IST (01:00 UTC)   → gap_analyzer

All times in UTC (IST = UTC+5:30).

Environment variables:
  COMPOSE_FILE        path to docker-compose.yml (default: docker-compose.yml)
  TZ                  set to UTC in docker-compose.yml service definition

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


def main():
    log.info("Scheduler started — all times UTC")
    log.info("  Daily   pipeline : Mon–Fri 11:00 UTC (16:30 IST)")
    log.info("  Weekly  refresh  : Sun     00:30 UTC (06:00 IST)")
    log.info("  Gap     analyzer : Sun     01:00 UTC (06:30 IST)")

    # Monday=0 … Friday=4 in schedule; weekdays() covers Mon–Fri
    schedule.every().monday.at("11:00").do(job_daily)
    schedule.every().tuesday.at("11:00").do(job_daily)
    schedule.every().wednesday.at("11:00").do(job_daily)
    schedule.every().thursday.at("11:00").do(job_daily)
    schedule.every().friday.at("11:00").do(job_daily)

    schedule.every().sunday.at("00:30").do(job_weekly)
    schedule.every().sunday.at("01:00").do(job_gap_analyzer)

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
