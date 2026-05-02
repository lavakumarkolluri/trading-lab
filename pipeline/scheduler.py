#!/usr/bin/env python3
"""
scheduler.py
────────────
Docker-native cron scheduler for the trading pipeline (OPS-001).
Runs inside a long-lived container — no host cron required.

Schedule (all times UTC, IST = UTC+5:30):
  Daily    — Mon–Fri 03:40 UTC (09:10 IST)  → option_chain_intraday (self-exits 15:35 IST)
  Daily    — Mon–Fri 11:00 UTC (16:30 IST)  → meta_pipeline (steps 1-12)
  Daily    — Mon–Fri 12:30 UTC (18:00 IST)  → option_chain_historical (bhavcopy pickup)
  Daily    — Mon–Fri 13:00 UTC (18:30 IST)  → options_eod_summary_pipeline (PCR/max pain)
  Daily    — Mon–Fri 13:30 UTC (19:00 IST)  → compute_historical_iv (ATM IV + rank)
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
    _run("nifty_straddle_backtest")               # weekly straddle backtest refresh


def job_mf_pipeline():
    log.info("=== MF pipeline weekly NAV refresh triggered ===")
    _run("mf_pipeline")


def job_fundamental_pipeline():
    log.info("=== Fundamental data pipeline weekly refresh triggered ===")
    _run("fundamental_pipeline")


def job_lot_size_pipeline():
    log.info("=== F&O lot size pipeline weekly refresh triggered ===")
    _run("lot_size_pipeline")


def job_confidence_scorer():
    log.info("=== Confidence scorer weekly retrain triggered ===")
    _run("confidence_scorer")


def job_holidays():
    """Run only on the 1st of each month."""
    if datetime.utcnow().day != 1:
        return
    log.info("=== Holidays pipeline monthly refresh triggered ===")
    _run("holidays_pipeline")


def job_option_chain_intraday():
    log.info("=== Option chain intraday scraper triggered ===")
    _run("option_chain_intraday")


def job_option_chain_eod():
    """
    Daily bhavcopy chain: download → PCR/max pain → IV → OI walls/skew.
    Runs sequentially after NSE publishes bhavcopy (~17:30-18:00 IST).
    Each step is idempotent — safe to re-run if a step fails.
    """
    log.info("=== Option chain EOD pipeline triggered ===")
    _run("option_chain_historical")         # download new bhavcopy day
    _run("options_eod_summary_pipeline")    # compute PCR + max pain
    _run("compute_historical_iv")           # compute ATM IV + iv_rank
    _run("compute_oi_features")             # compute OI walls + IV skew + FII futures


def main():
    log.info("Scheduler started — all times UTC")
    log.info("  Intraday OC scraper : Mon–Fri 03:40 UTC (09:10 IST)")
    log.info("  Daily   pipeline    : Mon–Fri 11:00 UTC (16:30 IST)")
    log.info("  Option chain EOD    : Mon–Fri 12:30 UTC (18:00 IST) → historical+PCR+IV")
    log.info("  Weekly  refresh     : Sun     00:30 UTC (06:00 IST)")
    log.info("  Gap     analyzer    : Sun     01:00 UTC (06:30 IST)")
    log.info("  Option  backtest    : Sun     02:00 UTC (07:30 IST)")
    log.info("  MF      pipeline    : Sun     03:00 UTC (08:30 IST)")
    log.info("  Fundamentals        : Sun     04:30 UTC (10:00 IST)")
    log.info("  Lot sizes           : Sun     05:00 UTC (10:30 IST)")
    log.info("  Confidence scorer   : Sun     05:30 UTC (11:00 IST)")
    log.info("  Holidays pipeline   : 1st of month 04:00 UTC (09:30 IST)")

    # Intraday option chain: start at 09:10 IST (03:40 UTC), self-exits at 15:35 IST
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at("03:40").do(job_option_chain_intraday)

    # Daily meta pipeline: 16:30 IST (11:00 UTC)
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at("11:00").do(job_daily)

    # Option chain EOD: bhavcopy + PCR/max pain + IV at 18:00 IST (12:30 UTC)
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at("12:30").do(job_option_chain_eod)

    schedule.every().sunday.at("00:30").do(job_weekly)
    schedule.every().sunday.at("01:00").do(job_gap_analyzer)
    schedule.every().sunday.at("02:00").do(job_option_backtest)
    schedule.every().sunday.at("03:00").do(job_mf_pipeline)
    schedule.every().sunday.at("04:30").do(job_fundamental_pipeline)
    schedule.every().sunday.at("05:00").do(job_lot_size_pipeline)
    schedule.every().sunday.at("05:30").do(job_confidence_scorer)

    # Monthly: schedule runs daily at 04:00, guard inside job checks day==1
    schedule.every().day.at("04:00").do(job_holidays)

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
