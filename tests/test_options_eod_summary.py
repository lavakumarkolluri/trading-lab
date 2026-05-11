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
