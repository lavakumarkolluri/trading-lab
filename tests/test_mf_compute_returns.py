"""
Tests for mf_compute_returns.py — PERF-002 fix.
Verifies correctness of the monthly-bucket XIRR optimisation.
All tests run offline — no ClickHouse or network required.
"""
import sys
import os
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import mf_compute_returns as m


def _make_nav_df(n_years: int = 5, daily_return: float = 0.0003) -> pd.DataFrame:
    """Synthetic NAV series: n_years of daily data growing at daily_return."""
    start = date(2020, 1, 1)
    dates, navs = [], []
    nav = 100.0
    d = start
    while (d - start).days < n_years * 365:
        if d.weekday() < 5:  # Mon–Fri only
            dates.append(d)
            navs.append(round(nav, 4))
            nav *= (1 + daily_return)
        d += timedelta(days=1)
    return pd.DataFrame({"date": dates, "nav": navs})


# ── Correctness ─────────────────────────────────────────────────────────────

def test_sip_returns_columns_present():
    df = _make_nav_df(3)
    result = m.compute_sip_returns(df)
    for col in ("sip_return_1y", "sip_return_3y", "sip_return_5y"):
        assert col in result.columns, f"Missing column: {col}"


def test_sip_returns_positive_for_rising_nav():
    """For a steadily rising NAV, all SIP returns must be positive."""
    df = _make_nav_df(6, daily_return=0.0005)  # ~13% annual
    result = m.compute_sip_returns(df)
    # Skip first year (no full lookback yet)
    tail = result.iloc[260:]
    assert (tail["sip_return_1y"] > 0).all(), "sip_return_1y must be positive for rising NAV"
    assert (tail["sip_return_3y"] > 0).all(), "sip_return_3y must be positive for rising NAV"


def test_sip_returns_no_nan():
    df = _make_nav_df(3)
    result = m.compute_sip_returns(df)
    assert not result["sip_return_1y"].isna().any()
    assert not result["sip_return_3y"].isna().any()
    assert not result["sip_return_5y"].isna().any()


def test_sip_return_1y_reasonable_magnitude():
    """XIRR on ₹1000/month SIP with ~12%/yr NAV growth should be around 12%."""
    df = _make_nav_df(3, daily_return=0.000453)  # ≈12% annual
    result = m.compute_sip_returns(df)
    tail_val = result["sip_return_1y"].iloc[-1]
    assert 5.0 < tail_val < 25.0, f"sip_return_1y out of expected range: {tail_val:.2f}"


def test_sip_return_same_value_within_month():
    """All rows in the same calendar month must have the same SIP return (monthly bucket)."""
    df = _make_nav_df(2)
    result = m.compute_sip_returns(df)
    result["ym"] = result["date"].apply(lambda d: (d.year, d.month))
    for ym, grp in result.groupby("ym"):
        vals = grp["sip_return_1y"].unique()
        assert len(vals) == 1, (
            f"Month {ym}: expected one unique sip_return_1y per month, got {vals}"
        )


# ── Performance ──────────────────────────────────────────────────────────────

def test_sip_returns_fast_for_large_dataset():
    """PERF-002: compute_sip_returns must run in <5s on 10 years of daily data (~2500 rows)."""
    df = _make_nav_df(10)
    assert len(df) > 2000, f"Expected >2000 rows, got {len(df)}"
    t0 = time.perf_counter()
    m.compute_sip_returns(df)
    elapsed = time.perf_counter() - t0
    assert elapsed < 5.0, (
        f"compute_sip_returns took {elapsed:.2f}s on {len(df)} rows — PERF-002 regression"
    )
