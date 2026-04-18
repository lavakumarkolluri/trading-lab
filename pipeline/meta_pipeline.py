#!/usr/bin/env python3
"""
meta_pipeline.py
────────────────
Daily orchestrator for the meta-learning trading system.
Runs all pipeline components in the correct sequence after market close.

Sequence:
  Step  1  — OHLCV pipeline (main.py)              fetch today's market data
  Step  2  — FII/DII pipeline                       fetch today's FII/DII flows   [OPS-006]
  Step  3  — Participant OI pipeline                 fetch today's F&O OI          [OPS-005]
  Step  4  — VIX pipeline                            India VIX + market regime
  Step  5  — Option chain pipeline                   PCR, max pain, IV rank, strike recs
  Step  6  — Event detector                          detect today's ≥2% moves
  Step  7  — Pattern feature extractor               compute features for new events
  Step  8  — Validation engine                       validate yesterday's predictions
  Step  9  — Pattern builder (incremental)           map new events to patterns
  Step 10  — Backtest engine (incremental)           backtest new pattern matches
  Step 11  — Star rater                              refresh pattern ratings
  Step 12  — Prediction engine                       generate tomorrow's predictions

Weekly (Sundays):
  Step 13  — Pattern builder --full                  full pattern remap
  Step 14  — Backtest engine --full                  full backtest refresh
  Step 15  — Star rater (post-full-backtest)         refresh ratings
  Step 16  — Gap analyzer                            FN/FP rate analysis (OPS-002/DATA-005)

COMPOSE_CMD fix [BUG-001]:
  The pipeline image now includes docker-ce-cli (see Dockerfile).
  COMPOSE_FILE env var (set in docker-compose.yml) tells the CLI where to find
  the compose file when running inside the container.

Usage:
  python meta_pipeline.py                  # full daily run
  python meta_pipeline.py --skip-ohlcv    # skip OHLCV fetch (already done)
  python meta_pipeline.py --skip-fii      # skip FII/DII fetch
  python meta_pipeline.py --weekly        # run weekly refresh steps only
  python meta_pipeline.py --dry-run       # show steps without executing
  python meta_pipeline.py --from-step 4   # resume from a specific step

Docker usage (recommended):
  docker compose run --rm meta_pipeline
  docker compose run --rm meta_pipeline --skip-ohlcv
  docker compose run --rm meta_pipeline --weekly
"""

import os
import sys
import logging
import argparse
import subprocess
from datetime import datetime, date

try:
    import urllib.request
    import json as _json
    _URLLIB_OK = True
except ImportError:
    _URLLIB_OK = False

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Telegram alerting (OPS-003) ────────────────────────
_TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def _send_telegram(message: str):
    if not (_TG_TOKEN and _TG_CHAT_ID and _URLLIB_OK):
        return
    try:
        payload = _json.dumps({"chat_id": _TG_CHAT_ID, "text": message}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{_TG_TOKEN}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        log.warning("Telegram alert failed: %s", e)


# ── Config ─────────────────────────────────────────────
# BUG-001 FIX: use --project-directory so docker compose can locate the
# compose file and resolve relative build paths correctly when called from
# inside the pipeline container (where CWD is /app, not the project root).
# COMPOSE_FILE env var is set in docker-compose.yml for the meta_pipeline
# service, pointing to /trading-lab/docker-compose.yml.
_COMPOSE_FILE = os.getenv("COMPOSE_FILE", "docker-compose.yml")
_PROJECT_DIR  = os.path.dirname(_COMPOSE_FILE) if "/" in _COMPOSE_FILE else "."

COMPOSE_CMD = [
    "docker", "compose",
    "-f", _COMPOSE_FILE,
    "--project-directory", _PROJECT_DIR,
    "run", "--rm",
]

# Colour codes
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
RESET  = "\033[0m"

def ok(msg):   print(f"{GREEN}✅ {msg}{RESET}")
def err(msg):  print(f"{RED}❌ {msg}{RESET}")
def warn(msg): print(f"{YELLOW}⚠️  {msg}{RESET}")
def info(msg): print(f"{BLUE}ℹ️  {msg}{RESET}")


# ══════════════════════════════════════════════════════
# Step definitions
# ══════════════════════════════════════════════════════

DAILY_STEPS = [
    {
        "step":        1,
        "name":        "OHLCV Pipeline",
        "description": "Fetch today's market data (stocks, crypto, forex)",
        "service":     "pipeline",
        "args":        [],
        "skippable":   True,
        "skip_key":    "skip_ohlcv",
    },
    # ── OPS-006 FIX: FII/DII pipeline added to daily orchestration ────────
    {
        "step":        2,
        "name":        "FII/DII Pipeline",
        "description": "Fetch today's FII/DII institutional cash-market flows (required by P005 pattern)",
        "service":     "fii_dii_pipeline",
        "args":        [],
        "skippable":   True,
        "skip_key":    "skip_fii",
        "soft_fail":   True,   # NSE API can be flaky; don't block the rest of the pipeline
    },
    # ── OPS-005 FIX: Participant OI pipeline added to daily orchestration ──
    {
        "step":        3,
        "name":        "Participant OI Pipeline",
        "description": "Fetch today's NSE F&O participant-wise open interest",
        "service":     "participant_oi_pipeline",
        "args":        [],
        "skippable":   True,
        "skip_key":    "skip_poi",
        "soft_fail":   True,   # archive data may not be available until evening
    },
    {
        "step":        4,
        "name":        "VIX Pipeline",
        "description": "Fetch India VIX + Nifty spot; write market regime classification",
        "service":     "vix_pipeline",
        "args":        [],
        "skippable":   True,
        "skip_key":    "skip_vix",
        "soft_fail":   True,   # yfinance outage must not block the main pipeline
    },
    {
        "step":        5,
        "name":        "Option Chain Pipeline",
        "description": "Fetch NSE option chain; compute PCR, max pain, IV rank, strike recs",
        "service":     "option_chain_pipeline",
        "args":        [],
        "skippable":   True,
        "skip_key":    "skip_oc",
        "soft_fail":   True,   # NSE API flaky; options context enriches but doesn't block
    },
    {
        "step":        6,
        "name":        "Event Detector",
        "description": "Detect ≥2% single-day moves in today's OHLCV",
        "service":     "event_detector",
        "args":        [],
        "skippable":   False,
    },
    {
        "step":        7,
        "name":        "Pattern Feature Extractor",
        "description": "Compute feature vectors (incl. VIX/PCR/IV) for new events",
        "service":     "pattern_feature_extractor",
        "args":        [],
        "skippable":   False,
    },
    {
        "step":        8,
        "name":        "Validation Engine",
        "description": "Validate yesterday's predictions against actual outcomes",
        "service":     "validation_engine",
        "args":        [],
        "skippable":   False,
        "soft_fail":   True,   # no predictions to validate on first run
    },
    {
        "step":        9,
        "name":        "Pattern Builder (incremental)",
        "description": "Map new events to existing patterns",
        "service":     "pattern_builder",
        "args":        ["--map-only"],
        "skippable":   False,
    },
    {
        "step":        10,
        "name":        "Backtest Engine (incremental)",
        "description": "Compute forward returns for new pattern matches",
        "service":     "backtest_engine",
        "args":        [],
        "skippable":   False,
    },
    {
        "step":        11,
        "name":        "Star Rater",
        "description": "Refresh pattern star ratings",
        "service":     "star_rater",
        "args":        [],
        "skippable":   False,
    },
    {
        "step":        12,
        "name":        "Prediction Engine",
        "description": "Generate predictions for tomorrow",
        "service":     "prediction_engine",
        "args":        [],
        "skippable":   False,
    },
]

WEEKLY_STEPS = [
    {
        "step":        13,
        "name":        "Pattern Builder (full remap)",
        "description": "Full remap of all features to all patterns",
        "service":     "pattern_builder",
        "args":        ["--full"],
        "skippable":   False,
    },
    {
        "step":        14,
        "name":        "Backtest Engine (full reprocess)",
        "description": "Recompute all backtest rows from scratch",
        "service":     "backtest_engine",
        "args":        ["--full"],
        "skippable":   False,
    },
    {
        "step":        15,
        "name":        "Star Rater (post-full-backtest)",
        "description": "Refresh ratings after full backtest",
        "service":     "star_rater",
        "args":        [],
        "skippable":   False,
    },
    {
        "step":        16,
        "name":        "Gap Analyzer",
        "description": "Measure false negative / false positive rates; optional LLM diagnosis",
        "service":     "gap_analyzer",
        "args":        [],
        "skippable":   False,
        "soft_fail":   True,
    },
]


# ══════════════════════════════════════════════════════
# Step runner
# ══════════════════════════════════════════════════════

def run_step(step: dict, dry_run: bool) -> bool:
    """
    Run a single pipeline step using docker compose run.
    Returns True on success, False on failure.
    """
    service = step["service"]
    args    = step["args"]
    name    = step["name"]
    step_n  = step["step"]

    cmd = COMPOSE_CMD + [service] + args

    info(f"\nStep {step_n}: {name}")
    info(f"  {step['description']}")
    info(f"  Command: {' '.join(cmd)}")

    if dry_run:
        warn(f"  [DRY RUN] Skipping execution")
        return True

    start = datetime.now()
    try:
        result = subprocess.run(
            cmd,
            capture_output=False,   # stream output directly to terminal
            text=True,
            check=False
        )
        duration = round((datetime.now() - start).total_seconds(), 1)

        if result.returncode == 0:
            ok(f"Step {step_n} completed in {duration}s")
            return True
        else:
            soft = step.get("soft_fail", False)
            if soft:
                warn(
                    f"Step {step_n} exited with code {result.returncode} "
                    f"(soft fail — continuing)"
                )
                return True
            else:
                msg = f"❌ Trading pipeline Step {step_n} ({name}) FAILED (exit {result.returncode})"
                err(f"Step {step_n} FAILED (exit code {result.returncode})")
                _send_telegram(msg)
                return False

    except FileNotFoundError:
        msg = f"❌ Trading pipeline Step {step_n} ({name}) FAILED — docker binary not found"
        err(
            f"Step {step_n} FAILED — 'docker' binary not found.\n"
            f"  Ensure the pipeline Docker image was rebuilt after adding\n"
            f"  docker-ce-cli to the Dockerfile (see BUG-001 fix).\n"
            f"  Rebuild with: docker compose build pipeline"
        )
        _send_telegram(msg)
        return False
    except Exception as e:
        err(f"Step {step_n} FAILED — {e}")
        _send_telegram(f"❌ Trading pipeline Step {step_n} ({name}) FAILED — {e}")
        return False


# ══════════════════════════════════════════════════════
# Pipeline runners
# ══════════════════════════════════════════════════════

def run_daily(skip_flags: dict, from_step: int, dry_run: bool) -> bool:
    for step in DAILY_STEPS:
        step_n = step["step"]

        if step_n < from_step:
            warn(f"Step {step_n}: {step['name']} — skipped (--from-step {from_step})")
            continue

        skip_key = step.get("skip_key")
        if skip_key and skip_flags.get(skip_key):
            warn(f"Step {step_n}: {step['name']} — skipped (--{skip_key.replace('_','-')})")
            continue

        success = run_step(step, dry_run)
        if not success:
            err(f"\nPipeline stopped at Step {step_n}: {step['name']}")
            err(f"Fix the issue and resume with: --from-step {step_n}")
            return False

    return True


def run_weekly(dry_run: bool) -> bool:
    for step in WEEKLY_STEPS:
        success = run_step(step, dry_run)
        if not success:
            err(f"\nWeekly refresh stopped at Step {step['step']}: {step['name']}")
            return False
    return True


# ══════════════════════════════════════════════════════
# Header / footer
# ══════════════════════════════════════════════════════

def print_header(mode: str, today: date, dry_run: bool):
    print(f"\n{BLUE}{'═'*60}")
    print(f"  META PIPELINE — {mode}")
    print(f"  Date    : {today}")
    print(f"  Dry run : {dry_run}")
    print(f"  Started : {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Compose : {_COMPOSE_FILE}")
    print(f"{'═'*60}{RESET}\n")


def print_footer(success: bool, start: datetime):
    duration = round((datetime.now() - start).total_seconds(), 1)
    print(f"\n{GREEN if success else RED}{'═'*60}")
    if success:
        print(f"  ✅ PIPELINE COMPLETE — {duration}s")
    else:
        print(f"  ❌ PIPELINE FAILED — {duration}s")
    print(f"{'═'*60}{RESET}\n")


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Meta Pipeline — daily orchestrator for the trading system"
    )
    parser.add_argument("--skip-ohlcv", action="store_true",
                        help="Skip Step 1 OHLCV fetch")
    parser.add_argument("--skip-fii",   action="store_true",
                        help="Skip Step 2 FII/DII fetch")
    parser.add_argument("--skip-poi",   action="store_true",
                        help="Skip Step 3 Participant OI fetch")
    parser.add_argument("--skip-vix",   action="store_true",
                        help="Skip Step 4 VIX pipeline")
    parser.add_argument("--skip-oc",    action="store_true",
                        help="Skip Step 5 Option chain pipeline")
    parser.add_argument("--weekly",     action="store_true",
                        help="Run weekly full refresh steps (11-13) only")
    parser.add_argument("--dry-run",    action="store_true",
                        help="Show steps without executing them")
    parser.add_argument("--from-step",  type=int, default=1,
                        help="Resume pipeline from this step number (default: 1)")
    args = parser.parse_args()

    today = date.today()
    start = datetime.now()

    skip_flags = {
        "skip_ohlcv": args.skip_ohlcv,
        "skip_fii":   args.skip_fii,
        "skip_poi":   args.skip_poi,
        "skip_vix":   args.skip_vix,
        "skip_oc":    args.skip_oc,
    }

    if args.weekly:
        print_header("WEEKLY REFRESH", today, args.dry_run)
        success = run_weekly(args.dry_run)
    else:
        parts = ["DAILY"]
        if args.skip_ohlcv: parts.append("no OHLCV")
        if args.skip_fii:   parts.append("no FII")
        if args.skip_poi:   parts.append("no POI")
        if args.from_step > 1: parts.append(f"from step {args.from_step}")
        print_header(" | ".join(parts), today, args.dry_run)
        success = run_daily(skip_flags, args.from_step, args.dry_run)

    print_footer(success, start)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
