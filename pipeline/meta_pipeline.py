#!/usr/bin/env python3
"""
meta_pipeline.py
────────────────
Daily orchestrator for the meta-learning trading system.
Runs all pipeline components in the correct sequence after market close.

Sequence:
  Step 1  — OHLCV pipeline (main.py)              fetch today's market data
  Step 2  — Event detector                         detect today's ≥2% moves
  Step 3  — Pattern feature extractor              compute features for new events
  Step 4  — Validation engine                      validate yesterday's predictions
  Step 5  — Pattern builder (incremental)          map new events to patterns
  Step 6  — Backtest engine (incremental)          backtest new pattern matches
  Step 7  — Star rater                             refresh pattern ratings
  Step 8  — Prediction engine                      generate tomorrow's predictions
  Step 9  — [Weekly] Pattern builder --full        full pattern remap (Sundays)
  Step 10 — [Weekly] Backtest engine --full        full backtest refresh (Sundays)

Each step is run as a subprocess using docker compose run.
If a step fails, the pipeline stops and reports which step failed.
Steps 4–8 are skipped gracefully if no data is available yet.

Schedule recommendation:
  Run at 4:30 PM IST on weekdays (Mon–Fri) after OHLCV data is available.
  On Sundays: run weekly refresh (Steps 9–10) to keep patterns up to date.

Usage:
  python meta_pipeline.py                  # full daily run
  python meta_pipeline.py --skip-ohlcv    # skip OHLCV fetch (already done)
  python meta_pipeline.py --weekly         # run weekly refresh steps only
  python meta_pipeline.py --dry-run        # show steps without executing
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

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────
COMPOSE_CMD = ["docker", "compose", "run", "--rm"]

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
        "skippable":   True,   # skip via --skip-ohlcv
        "skip_key":    "skip_ohlcv",
    },
    {
        "step":        2,
        "name":        "Event Detector",
        "description": "Detect ≥2% single-day moves in today's OHLCV",
        "service":     "event_detector",
        "args":        [],
        "skippable":   False,
    },
    {
        "step":        3,
        "name":        "Pattern Feature Extractor",
        "description": "Compute 20-dim feature vectors for new events",
        "service":     "pattern_feature_extractor",
        "args":        [],
        "skippable":   False,
    },
    {
        "step":        4,
        "name":        "Validation Engine",
        "description": "Validate yesterday's predictions against actual outcomes",
        "service":     "validation_engine",
        "args":        [],
        "skippable":   False,
        "soft_fail":   True,   # don't stop pipeline if no predictions to validate
    },
    {
        "step":        5,
        "name":        "Pattern Builder (incremental)",
        "description": "Map new events to existing patterns",
        "service":     "pattern_builder",
        "args":        ["--map-only"],
        "skippable":   False,
    },
    {
        "step":        6,
        "name":        "Backtest Engine (incremental)",
        "description": "Compute forward returns for new pattern matches",
        "service":     "backtest_engine",
        "args":        [],
        "skippable":   False,
    },
    {
        "step":        7,
        "name":        "Star Rater",
        "description": "Refresh pattern star ratings",
        "service":     "star_rater",
        "args":        [],
        "skippable":   False,
    },
    {
        "step":        8,
        "name":        "Prediction Engine",
        "description": "Generate predictions for tomorrow",
        "service":     "prediction_engine",
        "args":        [],
        "skippable":   False,
    },
]

WEEKLY_STEPS = [
    {
        "step":        9,
        "name":        "Pattern Builder (full remap)",
        "description": "Full remap of all features to all patterns",
        "service":     "pattern_builder",
        "args":        ["--full"],
        "skippable":   False,
    },
    {
        "step":        10,
        "name":        "Backtest Engine (full reprocess)",
        "description": "Recompute all backtest rows from scratch",
        "service":     "backtest_engine",
        "args":        ["--full"],
        "skippable":   False,
    },
    {
        "step":        11,
        "name":        "Star Rater (post-full-backtest)",
        "description": "Refresh ratings after full backtest",
        "service":     "star_rater",
        "args":        [],
        "skippable":   False,
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
            check=False             # handle returncode manually
        )
        duration = round((datetime.now() - start).total_seconds(), 1)

        if result.returncode == 0:
            ok(f"Step {step_n} completed in {duration}s")
            return True
        else:
            soft = step.get("soft_fail", False)
            if soft:
                warn(f"Step {step_n} exited with code {result.returncode} "
                     f"(soft fail — continuing)")
                return True
            else:
                err(f"Step {step_n} FAILED (exit code {result.returncode})")
                return False

    except FileNotFoundError:
        err(f"Step {step_n} FAILED — docker compose not found. "
            f"Run meta_pipeline.py from inside the trading-lab directory.")
        return False
    except Exception as e:
        err(f"Step {step_n} FAILED — {e}")
        return False


# ══════════════════════════════════════════════════════
# Pipeline runners
# ══════════════════════════════════════════════════════

def run_daily(skip_flags: dict, from_step: int, dry_run: bool) -> bool:
    """Run all daily pipeline steps in sequence."""
    for step in DAILY_STEPS:
        step_n = step["step"]

        # Skip if before from_step
        if step_n < from_step:
            warn(f"Step {step_n}: {step['name']} — skipped (--from-step {from_step})")
            continue

        # Skip if flagged
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
    """Run weekly refresh steps."""
    for step in WEEKLY_STEPS:
        success = run_step(step, dry_run)
        if not success:
            err(f"\nWeekly refresh stopped at Step {step['step']}: {step['name']}")
            return False
    return True


# ══════════════════════════════════════════════════════
# Summary header
# ══════════════════════════════════════════════════════

def print_header(mode: str, today: date, dry_run: bool):
    print(f"\n{BLUE}{'═'*60}")
    print(f"  META PIPELINE — {mode}")
    print(f"  Date    : {today}")
    print(f"  Dry run : {dry_run}")
    print(f"  Started : {datetime.now().strftime('%H:%M:%S')}")
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
                        help="Skip Step 1 OHLCV fetch (if already done separately)")
    parser.add_argument("--weekly",     action="store_true",
                        help="Run weekly full refresh steps (9-11) only")
    parser.add_argument("--dry-run",    action="store_true",
                        help="Show steps without executing them")
    parser.add_argument("--from-step",  type=int, default=1,
                        help="Resume pipeline from this step number (default: 1)")
    args = parser.parse_args()

    today = date.today()
    start = datetime.now()

    skip_flags = {
        "skip_ohlcv": args.skip_ohlcv,
    }

    if args.weekly:
        print_header("WEEKLY REFRESH", today, args.dry_run)
        success = run_weekly(args.dry_run)
    else:
        mode = "DAILY"
        if args.skip_ohlcv:
            mode += " (no OHLCV)"
        if args.from_step > 1:
            mode += f" (from step {args.from_step})"
        print_header(mode, today, args.dry_run)
        success = run_daily(skip_flags, args.from_step, args.dry_run)

    print_footer(success, start)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
