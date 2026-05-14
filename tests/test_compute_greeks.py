"""
Unit tests for compute_greeks.py — Black-Scholes math, IV round-trip, derive_spot.
All tests run offline — no ClickHouse connection required.
"""
import sys
import os
import math
from datetime import date

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import compute_greeks as cg


# ── Black-Scholes pricing ──────────────────────────────────────────────────────

def test_bs_call_atm_positive():
    """ATM call must have positive value with time remaining."""
    price = cg._bs_call(S=100, K=100, T=30/365, r=0.065, sigma=0.2)
    assert price > 0

def test_bs_put_atm_positive():
    price = cg._bs_put(S=100, K=100, T=30/365, r=0.065, sigma=0.2)
    assert price > 0

def test_bs_call_zero_time():
    """At expiry (T=0), call = max(S-K, 0)."""
    assert cg._bs_call(S=110, K=100, T=0, r=0.065, sigma=0.2) == pytest.approx(10.0)
    assert cg._bs_call(S=90,  K=100, T=0, r=0.065, sigma=0.2) == pytest.approx(0.0)

def test_bs_put_zero_time():
    assert cg._bs_put(S=90,  K=100, T=0, r=0.065, sigma=0.2) == pytest.approx(10.0)
    assert cg._bs_put(S=110, K=100, T=0, r=0.065, sigma=0.2) == pytest.approx(0.0)

def test_put_call_parity():
    """C - P = S - K*exp(-rT) (put-call parity)."""
    S, K, T, r, sigma = 100, 100, 30/365, 0.065, 0.2
    call = cg._bs_call(S, K, T, r, sigma)
    put  = cg._bs_put(S, K, T, r, sigma)
    parity = S - K * math.exp(-r * T)
    assert abs((call - put) - parity) < 0.001


# ── IV round-trip ──────────────────────────────────────────────────────────────

def test_implied_vol_roundtrip_call():
    """Compute BS call price at known IV, then recover IV from that price."""
    S, K, T, r, true_iv = 100, 100, 30/365, 0.065, 0.25
    price = cg._bs_call(S, K, T, r, true_iv)
    recovered = cg._implied_vol(S, K, T, r, price, is_call=True)
    assert abs(recovered - true_iv) < 0.002

def test_implied_vol_roundtrip_put():
    S, K, T, r, true_iv = 100, 98, 30/365, 0.065, 0.22
    price = cg._bs_put(S, K, T, r, true_iv)
    recovered = cg._implied_vol(S, K, T, r, price, is_call=False)
    assert abs(recovered - true_iv) < 0.002

def test_implied_vol_zero_for_intrinsic_only():
    """Deep intrinsic price → IV should return 0 (no time value)."""
    iv = cg._implied_vol(S=100, K=50, T=1/365, r=0.065, market_price=49.9, is_call=True)
    assert iv == 0.0

def test_implied_vol_zero_time():
    iv = cg._implied_vol(S=100, K=100, T=0, r=0.065, market_price=5.0, is_call=True)
    assert iv == 0.0


# ── compute_greeks_row ─────────────────────────────────────────────────────────

def test_atm_call_delta_near_half():
    """ATM call delta should be close to 0.5."""
    iv, delta, theta = cg.compute_greeks_row(S=100, K=100, T=30/365,
                                              r=0.065, ltp=4.0, is_call=True)
    assert 0.45 <= delta <= 0.60

def test_atm_put_delta_near_neg_half():
    """ATM put delta should be close to -0.5."""
    iv, delta, theta = cg.compute_greeks_row(S=100, K=100, T=30/365,
                                              r=0.065, ltp=3.5, is_call=False)
    assert -0.60 <= delta <= -0.40

def test_deep_itm_call_delta_near_one():
    """Deep ITM call (S >> K) should have delta close to 1."""
    price = cg._bs_call(S=150, K=100, T=30/365, r=0.065, sigma=0.2)
    iv, delta, theta = cg.compute_greeks_row(S=150, K=100, T=30/365,
                                              r=0.065, ltp=price, is_call=True)
    assert delta > 0.90

def test_theta_is_negative_for_long_time():
    """Short option theta should be negative (time decay costs the holder)."""
    iv, delta, theta = cg.compute_greeks_row(S=100, K=100, T=30/365,
                                              r=0.065, ltp=4.0, is_call=True)
    assert theta < 0


# ── derive_spot ───────────────────────────────────────────────────────────────

def test_derive_spot_put_call_parity():
    """
    For a balanced CE/PE pair, spot = ATM_strike + CE_ltp - PE_ltp.
    With CE=PE at ATM, spot should equal the strike.
    """
    rows = []
    for strike in [9800, 9900, 10000, 10100]:
        # CE > PE for ITM calls (strike < 10000), CE < PE for OTM
        rows.append({"expiry": date(2024, 1, 25), "timestamp": "2024-01-24",
                     "option_type": "CE", "strike": float(strike), "ltp": max(10000 - strike, 0) + 50})
        rows.append({"expiry": date(2024, 1, 25), "timestamp": "2024-01-24",
                     "option_type": "PE", "strike": float(strike), "ltp": max(strike - 10000, 0) + 50})
    df = pd.DataFrame(rows)
    spots = cg.derive_spot(df)
    assert (date(2024, 1, 25), "2024-01-24") in spots
    spot = spots[(date(2024, 1, 25), "2024-01-24")]
    assert abs(spot - 10000) < 5   # should be ~10000


def test_derive_spot_returns_empty_for_no_common_strikes():
    rows = [
        {"expiry": date(2024, 1, 25), "timestamp": "t1", "option_type": "CE", "strike": 100.0, "ltp": 10.0},
        {"expiry": date(2024, 1, 25), "timestamp": "t1", "option_type": "PE", "strike": 200.0, "ltp": 10.0},
    ]
    spots = cg.derive_spot(pd.DataFrame(rows))
    assert spots == {}
