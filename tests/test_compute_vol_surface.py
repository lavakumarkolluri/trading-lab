"""
Unit tests for compute_vol_surface.py — interp_atm_iv, skew_at_moneyness, derive_spot.
All tests run offline — no ClickHouse connection required.
"""
import sys
import os
from datetime import date

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import compute_vol_surface as cvs


# ── interp_atm_iv ─────────────────────────────────────────────────────────────

def test_interp_exact_knot():
    """At an exact DTE knot, return the value directly."""
    atm = {7: 0.15, 30: 0.18, 90: 0.20}
    assert cvs.interp_atm_iv(atm, 7)  == pytest.approx(0.15)
    assert cvs.interp_atm_iv(atm, 30) == pytest.approx(0.18)
    assert cvs.interp_atm_iv(atm, 90) == pytest.approx(0.20)

def test_interp_midpoint():
    """Midpoint between two knots → linear average."""
    atm = {7: 0.10, 30: 0.20}
    mid = cvs.interp_atm_iv(atm, 18)   # (18-7)/(30-7) = 11/23 ≈ 0.478
    expected = 0.10 + (0.20 - 0.10) * (18 - 7) / (30 - 7)
    assert mid == pytest.approx(expected, abs=1e-6)

def test_interp_below_min_dte_extrapolates_flat():
    """DTE below smallest knot → return lowest-DTE value (no extrapolation down)."""
    atm = {14: 0.15, 30: 0.18}
    assert cvs.interp_atm_iv(atm, 5) == pytest.approx(0.15)

def test_interp_above_max_dte_extrapolates_flat():
    """DTE above largest knot → return highest-DTE value."""
    atm = {7: 0.15, 30: 0.18}
    assert cvs.interp_atm_iv(atm, 90) == pytest.approx(0.18)

def test_interp_empty_returns_zero():
    assert cvs.interp_atm_iv({}, 30) == 0.0


# ── skew_at_moneyness ─────────────────────────────────────────────────────────

def _make_surface(pe_iv=0.22, ce_iv=0.18):
    """Build a minimal surface DataFrame with PE at 0.98 moneyness, CE at 1.02."""
    rows = [
        {"option_type": "PE", "moneyness": 0.980, "iv": pe_iv},
        {"option_type": "PE", "moneyness": 0.979, "iv": pe_iv},
        {"option_type": "CE", "moneyness": 1.020, "iv": ce_iv},
        {"option_type": "CE", "moneyness": 1.021, "iv": ce_iv},
    ]
    return pd.DataFrame(rows)

def test_skew_positive_when_put_iv_higher():
    surf = _make_surface(pe_iv=0.22, ce_iv=0.18)
    skew = cvs.skew_at_moneyness(surf, pe_m=0.98)
    assert skew == pytest.approx(0.22 - 0.18, abs=1e-5)

def test_skew_zero_when_no_pe_data():
    surf = _make_surface()
    surf = surf[surf["option_type"] == "CE"]   # drop PE rows
    assert cvs.skew_at_moneyness(surf, pe_m=0.98) == 0.0

def test_skew_zero_when_no_ce_data():
    surf = _make_surface()
    surf = surf[surf["option_type"] == "PE"]
    assert cvs.skew_at_moneyness(surf, pe_m=0.98) == 0.0

def test_skew_mirror_moneyness():
    """pe_m=0.97 should compare against ce_m=1.03 (mirror: 2.0 - 0.97)."""
    rows = [
        {"option_type": "PE", "moneyness": 0.970, "iv": 0.25},
        {"option_type": "CE", "moneyness": 1.030, "iv": 0.20},
    ]
    surf = pd.DataFrame(rows)
    skew = cvs.skew_at_moneyness(surf, pe_m=0.97, tol=0.02)
    assert skew == pytest.approx(0.25 - 0.20, abs=1e-5)


# ── derive_spot (vol surface version) ─────────────────────────────────────────

def test_vol_surface_derive_spot_basic():
    """For CE=PE at each strike, spot should equal the ATM strike."""
    rows = []
    for k in [19800, 19900, 20000, 20100]:
        rows.append({"expiry": date(2024, 3, 28), "option_type": "CE",
                     "strike": float(k), "ltp": float(max(20000 - k, 0) + 100)})
        rows.append({"expiry": date(2024, 3, 28), "option_type": "PE",
                     "strike": float(k), "ltp": float(max(k - 20000, 0) + 100)})
    df = pd.DataFrame(rows)
    spots = cvs.derive_spot(df)
    assert date(2024, 3, 28) in spots
    assert abs(spots[date(2024, 3, 28)] - 20000) < 10

def test_vol_surface_derive_spot_empty_when_no_common():
    rows = [
        {"expiry": date(2024, 3, 28), "option_type": "CE", "strike": 100.0, "ltp": 10.0},
        {"expiry": date(2024, 3, 28), "option_type": "PE", "strike": 200.0, "ltp": 10.0},
    ]
    spots = cvs.derive_spot(pd.DataFrame(rows))
    assert spots == {}


# ── Migration 063 sanity ───────────────────────────────────────────────────────

def test_migration_063_exists():
    path = os.path.join(os.path.dirname(__file__), "..",
                        "clickhouse", "migrations", "063_create_vol_surface.sql")
    assert os.path.exists(path)
    with open(path) as f:
        sql = f.read()
    assert "vol_surface" in sql
    assert "vol_term_structure" in sql
    assert "term_slope" in sql
    assert "skew_2pct" in sql
