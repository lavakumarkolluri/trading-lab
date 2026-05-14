"""
Tests for option_chain_historical.py.
All tests run offline — no ClickHouse or network access required.
"""
import sys
import os
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import option_chain_historical as m


# ── CRIT-006: default from-date must cover 2019 ──────────────────────────────

def test_default_from_date_is_2019_or_earlier():
    """DEFAULT_FROM_DATE constant must be 2019-01-01 or earlier."""
    assert hasattr(m, "DEFAULT_FROM_DATE"), "DEFAULT_FROM_DATE constant missing"
    assert m.DEFAULT_FROM_DATE <= date(2019, 1, 1)


def test_lookback_covers_2019():
    """When no --from arg is given, computed from_date must be <= 2019-01-01."""
    from_date = m.DEFAULT_FROM_DATE
    assert from_date <= date(2019, 1, 1), (
        f"Default from_date {from_date} does not reach 2019-01-01. "
        "Set DEFAULT_FROM_DATE = date(2019, 1, 1) in option_chain_historical.py"
    )


# ── URL format selection ──────────────────────────────────────────────────────

def test_old_url_format_contains_correct_date_parts():
    """Old format URL must embed month as uppercase 3-letter abbreviation."""
    d = date(2023, 6, 15)
    url = m.BHAVCOPY_URL_OLD.format(
        yyyy=d.strftime("%Y"),
        mmm=d.strftime("%b").upper(),
        ddmmmyyyy=d.strftime("%d%b%Y").upper(),
    )
    assert "2023" in url
    assert "JUN" in url
    assert "15JUN2023" in url


def test_new_url_format_contains_yyyymmdd():
    """New format URL must embed date as YYYYMMDD."""
    d = date(2024, 8, 5)
    url = m.BHAVCOPY_URL_NEW.format(yyyymmdd=d.strftime("%Y%m%d"))
    assert "20240805" in url


# ── TARGET_SYMBOLS includes all 4 NSE indices ─────────────────────────────────

def test_target_symbols_includes_all_four_indices():
    required = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}
    assert required.issubset(m.TARGET_SYMBOLS), (
        f"Missing symbols: {required - m.TARGET_SYMBOLS}"
    )


# ── fetch_loaded_dates handles empty table ────────────────────────────────────

def test_fetch_loaded_dates_empty_table_returns_empty_set():
    ch = MagicMock()
    ch.query.return_value.result_rows = []
    result = m.fetch_loaded_dates(ch)
    assert result == set()
