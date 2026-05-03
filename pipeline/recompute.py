#!/usr/bin/env python3
"""
recompute.py
────────────
Cascade recompute tool: truncate derived tables and re-run downstream
pipeline services in topological order from a given starting point.

Usage:
  python recompute.py --from options_eod_summary_pipeline
  python recompute.py --from compute_historical_iv --from-date 2025-01-01
  python recompute.py --from confidence_scorer --dry-run
  python recompute.py --from strategy_backtester --yes   # skip confirmation

Docker:
  docker compose run --rm recompute --from options_eod_summary_pipeline
"""

import argparse
import os
import subprocess
import sys
from datetime import date

# ── DAG in topological order (source → sink) ──────────────────────────────
# Each service that can be recomputed, in the order they must run.
DAG_ORDER = [
    "option_chain_historical",
    "options_eod_summary_pipeline",
    "compute_historical_iv",
    "compute_oi_features",
    "strategy_backtester",
    "confidence_scorer",
    "strategy_selector",
]

# Tables to TRUNCATE before re-running each service.
# Only derived/computed tables — never source tables.
SERVICE_TABLES: dict[str, list[str]] = {
    "options_eod_summary_pipeline": ["analysis.options_eod_summary"],
    "compute_historical_iv":        [],   # updates options_eod_summary in-place (ALTER UPDATE)
    "compute_oi_features":          ["analysis.oi_features"],
    "strategy_backtester":          ["analysis.spread_backtest"],
    "confidence_scorer":            ["analysis.confidence_scores"],
    "strategy_selector":            ["analysis.strategy_simulation"],
}

# Extra CLI args to pass when invoking each service for full recompute.
SERVICE_EXTRA_ARGS: dict[str, list[str]] = {
    "option_chain_historical":      ["--reprocess"],
    "options_eod_summary_pipeline": [],   # --from-date injected below
    "compute_historical_iv":        [],
    "compute_oi_features":          [],
    "strategy_backtester":          [],
    "confidence_scorer":            [],
    "strategy_selector":            ["--backtest"],
}

# Services that accept a --from YYYY-MM-DD argument
SUPPORTS_FROM_DATE = {
    "options_eod_summary_pipeline",
    "compute_historical_iv",
    "compute_oi_features",
    "strategy_backtester",
    "confidence_scorer",
}

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
RESET  = "\033[0m"


def build_cascade(from_service: str) -> list[str]:
    if from_service not in DAG_ORDER:
        print(f"{RED}Unknown service: {from_service}{RESET}")
        print(f"  Valid services: {', '.join(DAG_ORDER)}")
        sys.exit(1)
    idx = DAG_ORDER.index(from_service)
    return DAG_ORDER[idx:]


def get_compose_cmd() -> list[str]:
    compose_file = os.getenv("COMPOSE_FILE", "docker-compose.yml")
    project_dir  = os.path.dirname(compose_file) if "/" in compose_file else "."
    return [
        "docker", "compose",
        "-f", compose_file,
        "--project-directory", project_dir,
        "run", "--rm",
    ]


def truncate_tables(tables: list[str], dry_run: bool):
    if not tables:
        return
    try:
        from ch_utils import ch_client
        ch = ch_client()
        for tbl in tables:
            if dry_run:
                print(f"  {YELLOW}[DRY RUN]{RESET} TRUNCATE TABLE {tbl}")
            else:
                print(f"  Truncating {tbl}...")
                ch.command(f"TRUNCATE TABLE IF EXISTS {tbl}")
    except ImportError:
        print(f"  {YELLOW}ch_utils not available — skipping truncate{RESET}")
    except Exception as e:
        print(f"  {RED}Truncate failed: {e}{RESET}")
        sys.exit(1)


def run_service(service: str, extra_args: list[str], dry_run: bool) -> bool:
    cmd = get_compose_cmd() + [service] + extra_args
    print(f"  Command: {' '.join(cmd)}")
    if dry_run:
        print(f"  {YELLOW}[DRY RUN]{RESET} Skipping execution")
        return True
    result = subprocess.run(cmd, capture_output=False, text=True, check=False)
    if result.returncode != 0:
        print(f"{RED}  FAILED (exit {result.returncode}){RESET}")
        return False
    print(f"{GREEN}  Done{RESET}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Cascade recompute: truncate + re-run from a given pipeline stage"
    )
    parser.add_argument(
        "--from", dest="from_service", required=True,
        metavar="SERVICE",
        help=f"Start recompute from this service. Valid: {', '.join(DAG_ORDER)}"
    )
    parser.add_argument(
        "--from-date", dest="from_date", default=None,
        metavar="YYYY-MM-DD",
        help="Earliest date to recompute (default: full history). Passed as --from to each service."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would run without executing")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Skip confirmation prompt")
    args = parser.parse_args()

    cascade = build_cascade(args.from_service)

    print(f"\n{BLUE}{'═'*60}")
    print(f"  RECOMPUTE CASCADE")
    print(f"  Starting from : {args.from_service}")
    if args.from_date:
        print(f"  From date     : {args.from_date}")
    print(f"  Dry run       : {args.dry_run}")
    print(f"  Services      : {' → '.join(cascade)}")
    print(f"{'═'*60}{RESET}\n")

    tables_to_clear = []
    for svc in cascade:
        for tbl in SERVICE_TABLES.get(svc, []):
            tables_to_clear.append(tbl)

    if tables_to_clear:
        print(f"{YELLOW}Tables that will be TRUNCATED:{RESET}")
        for tbl in tables_to_clear:
            print(f"  • {tbl}")
        print()

    if not args.yes and not args.dry_run:
        answer = input("Proceed? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            sys.exit(0)

    for svc in cascade:
        print(f"\n{BLUE}── {svc} ──{RESET}")
        truncate_tables(SERVICE_TABLES.get(svc, []), args.dry_run)

        extra = list(SERVICE_EXTRA_ARGS.get(svc, []))
        if args.from_date and svc in SUPPORTS_FROM_DATE:
            extra += ["--from", args.from_date]

        success = run_service(svc, extra, args.dry_run)
        if not success:
            print(f"\n{RED}Cascade stopped at {svc}. Fix the issue and re-run with --from {svc}.{RESET}")
            sys.exit(1)

    print(f"\n{GREEN}{'═'*60}")
    print(f"  RECOMPUTE COMPLETE — {len(cascade)} service(s) processed")
    print(f"{'═'*60}{RESET}\n")


if __name__ == "__main__":
    main()
