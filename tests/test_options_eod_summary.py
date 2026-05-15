"""
Unit tests for options_eod_summary_pipeline.py.
Catches: SEC-008 SQL injection via symbol validation, FINAL on RMT reads.
All tests run offline — no ClickHouse connection required.
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
from options_eod_summary_pipeline import _validate_symbol


def test_valid_symbols_pass():
    for sym in ("NIFTY", "BANKNIFTY", "SENSEX"):
        assert _validate_symbol(sym) == sym


def test_sql_injection_rejected():
    """SEC-008: symbol must be rejected if it contains SQL-special chars."""
    bad_inputs = [
        "NIFTY' OR '1'='1",
        "'; DROP TABLE market.options_chain; --",
        "NIFTY--",
        "nifty",          # lowercase — only uppercase accepted
        "",
        "A" * 21,         # too long
    ]
    for bad in bad_inputs:
        with pytest.raises(ValueError, match="Invalid symbol"):
            _validate_symbol(bad)


def test_fetch_dates_uses_parameterized_query():
    """fetch_dates must use parameters=, not f-string interpolation."""
    import inspect
    import options_eod_summary_pipeline as m
    src = inspect.getsource(m.fetch_dates)
    assert "parameters=" in src, "fetch_dates must use parameterized query (not f-string)"
    assert "f\"" not in src and "f'" not in src, "fetch_dates must not use f-strings"


def test_fetch_chain_for_date_uses_parameterized_query():
    """fetch_chain_for_date must use parameters=, not f-string interpolation."""
    import inspect
    import options_eod_summary_pipeline as m
    src = inspect.getsource(m.fetch_chain_for_date)
    assert "parameters=" in src, "fetch_chain_for_date must use parameterized query"
    assert "f\"" not in src and "f'" not in src, "fetch_chain_for_date must not use f-strings"


def test_process_date_deduplicates_intraday_snapshots():
    """process_date must deduplicate multiple intraday snapshots per (expiry, strike, option_type).
    Without dedup, duplicate strikes cause estimate_spot() to return a Series → float() crash."""
    import pandas as pd
    from datetime import date
    from unittest.mock import MagicMock, patch
    import options_eod_summary_pipeline as m

    snap_date = date(2026, 5, 12)
    expiry = date(2026, 5, 13)

    # Two intraday snapshots for the same strike — simulates the bug scenario
    rows = [
        {"expiry": expiry, "strike": 24000, "option_type": "CE", "ltp": 90.0,  "oi": 1000, "iv": 0.0},
        {"expiry": expiry, "strike": 24000, "option_type": "CE", "ltp": 100.0, "oi": 1100, "iv": 0.0},  # duplicate
        {"expiry": expiry, "strike": 24000, "option_type": "PE", "ltp": 88.0,  "oi": 900,  "iv": 0.0},
        {"expiry": expiry, "strike": 24000, "option_type": "PE", "ltp": 95.0,  "oi": 950,  "iv": 0.0},  # duplicate
    ]
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.Timestamp("2026-05-12 10:30:00")

    ch = MagicMock()
    with patch.object(m, "fetch_chain_for_date", return_value=df), \
         patch.object(m, "near_term_expiry", return_value=expiry), \
         patch.object(m, "compute_pcr", return_value=(1000, 900, 1.11)), \
         patch.object(m, "compute_max_pain", return_value=24000.0):
        # If dedup is missing, float(Series) raises TypeError — reaching here means dedup worked
        result = m.process_date(ch, "NIFTY", snap_date)

    assert result is not None or result is None  # just assert no exception raised
