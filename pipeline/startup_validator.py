"""
startup_validator.py
────────────────────
Validates required env vars at process start. Call validate() in each
service's __main__ block before opening any connections or doing any work.

Exits with code 1 if any required var is missing or empty — loud failure
is better than a cryptic ClickHouse auth error or silent bad data.
"""
import os
import sys
import logging

log = logging.getLogger(__name__)

# Per-service required env vars. Import and call validate_service(name) from __main__.
REQUIRED_VARS: dict[str, list[str]] = {
    "scheduler": ["CH_PASSWORD"],
    "meta_pipeline": ["CH_PASSWORD", "HOST_PROJECT_DIR"],
    "confidence_scorer": ["CH_PASSWORD"],
    "strategy_selector": ["CH_PASSWORD"],
    "strategy_backtester": ["CH_PASSWORD"],
    "compute_oi_features": ["CH_PASSWORD"],
    "compute_historical_iv": ["CH_PASSWORD"],
    "options_eod_summary_pipeline": ["CH_PASSWORD"],
    "option_chain_historical": ["CH_PASSWORD"],
    "gap_analyzer": ["CH_PASSWORD"],
    "graduation_gate": ["CH_PASSWORD"],
    "intraday_monitor": ["CH_PASSWORD", "KITE_API_KEY", "KITE_ACCESS_TOKEN"],
    "option_chain_intraday": ["CH_PASSWORD"],
}


def validate(required: list[str], service: str = "") -> None:
    """Check that all vars in `required` are set and non-empty.
    Logs ERROR for every missing var, then exits(1).
    No-op when `required` is empty.
    """
    prefix = f"[{service}] " if service else ""
    missing = [v for v in required if not os.environ.get(v, "").strip()]
    if missing:
        for var in missing:
            log.error("%sREQUIRED env var %s is not set — refusing to start", prefix, var)
        sys.exit(1)


def validate_service(service: str) -> None:
    """Validate the standard required vars for a named service.
    Convenience wrapper: looks up REQUIRED_VARS[service] and calls validate().
    """
    required = REQUIRED_VARS.get(service, ["CH_PASSWORD"])
    validate(required, service=service)
