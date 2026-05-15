"""
Tests for confidence_scorer.py feature extraction functions.
All offline — no ClickHouse required.

Critical: extract_chain_features() builds the feature vector fed to XGBoost.
If it returns wrong values or crashes, model predictions are garbage.

Catches:
  - Duplicate strikes after set_index → .get() returns Series → float() crash
  - Missing ATM → straddle_premium = 0 → return None (correct) vs zeros (wrong)
  - PCR OI sign error (PE/CE swapped) → inverted signal
  - extract_eod_features missing key → KeyError in scoring loop
"""
import sys
import os
from datetime import date
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import confidence_scorer as cs


# ── Helpers ────────────────────────────────────────────────────────────────────

def _chain_df(rows):
    """Build chain DataFrame from list of (snap_date, expiry, strike, otype, ltp, oi)."""
    return pd.DataFrame(
        rows,
        columns=["snap_date", "expiry", "strike", "option_type", "ltp", "oi"]
    )


def _single_snap(snap_date=None, expiry=None, atm=24000.0,
                 ce_ltp=120.0, pe_ltp=118.0, oi=10000):
    """Build a chain where atm has the smallest |CE-PE| diff (required by find_atm_strike).
    OTM strikes have CE much higher than PE (or vice versa) to ensure atm is chosen."""
    snap_date = snap_date or date(2025, 5, 1)
    expiry    = expiry    or date(2025, 5, 8)
    return _chain_df([
        (snap_date, expiry, atm - 100.0, "CE", 200.0, oi),   # deep ITM CE, big diff
        (snap_date, expiry, atm - 100.0, "PE",  10.0, oi),
        (snap_date, expiry, atm,         "CE", ce_ltp, oi),  # near ATM → small diff
        (snap_date, expiry, atm,         "PE", pe_ltp, oi),
        (snap_date, expiry, atm + 100.0, "CE",  10.0, oi // 2),
        (snap_date, expiry, atm + 100.0, "PE", 200.0, oi // 2),  # deep ITM PE, big diff
    ])


# ── extract_chain_features — happy path ───────────────────────────────────────

def test_extract_chain_features_returns_dict():
    snap_date = date(2025, 5, 1)
    expiry    = date(2025, 5, 8)
    chain = _single_snap(snap_date, expiry)
    snap_df = chain[(chain.snap_date == snap_date) & (chain.expiry == expiry)]
    result = cs.extract_chain_features(snap_df)
    assert result is not None
    assert isinstance(result, dict)


def test_extract_chain_features_straddle_premium():
    """straddle_premium = ATM CE ltp + ATM PE ltp."""
    snap_date = date(2025, 5, 1)
    expiry    = date(2025, 5, 8)
    ce_ltp, pe_ltp = 120.0, 118.0
    chain = _single_snap(snap_date, expiry, ce_ltp=ce_ltp, pe_ltp=pe_ltp)
    snap_df = chain[(chain.snap_date == snap_date) & (chain.expiry == expiry)]
    r = cs.extract_chain_features(snap_df)
    assert r is not None
    assert abs(r["straddle_premium"] - (ce_ltp + pe_ltp)) < 0.01


def test_extract_chain_features_straddle_pct():
    """straddle_pct = straddle_premium / atm_strike * 100."""
    snap_date = date(2025, 5, 1)
    expiry    = date(2025, 5, 8)
    atm, ce_ltp, pe_ltp = 24000.0, 120.0, 118.0
    chain = _single_snap(snap_date, expiry, atm=atm, ce_ltp=ce_ltp, pe_ltp=pe_ltp)
    snap_df = chain[(chain.snap_date == snap_date) & (chain.expiry == expiry)]
    r = cs.extract_chain_features(snap_df)
    expected = (ce_ltp + pe_ltp) / atm * 100
    assert abs(r["straddle_pct"] - expected) < 0.001


def test_extract_chain_features_pcr_oi_direction():
    """pcr_oi = total_PE_OI / total_CE_OI. Higher PE OI → pcr > 1 (bullish signal)."""
    snap_date = date(2025, 5, 1)
    expiry    = date(2025, 5, 8)
    chain = _chain_df([
        (snap_date, expiry, 23900.0, "CE", 180.0, 2000),
        (snap_date, expiry, 23900.0, "PE", 90.0,  8000),
        (snap_date, expiry, 24000.0, "CE", 120.0, 5000),
        (snap_date, expiry, 24000.0, "PE", 110.0, 15000),  # 3× PE OI
        (snap_date, expiry, 24100.0, "CE", 50.0,  2000),
        (snap_date, expiry, 24100.0, "PE", 40.0,  4000),
    ])
    snap_df = chain[(chain.snap_date == snap_date) & (chain.expiry == expiry)]
    r = cs.extract_chain_features(snap_df)
    assert r is not None
    # PE OI = 19000, CE OI = 7000 → pcr ≈ 2.71
    assert r["pcr_oi"] > 1.0, "pcr_oi should be > 1 when PE OI > CE OI"


def test_extract_chain_features_returns_none_on_empty_snap():
    """No data for this date/expiry → return None, not an empty dict."""
    snap_df = pd.DataFrame(columns=["snap_date", "expiry", "strike", "option_type", "ltp", "oi"])
    result = cs.extract_chain_features(snap_df)
    assert result is None


def test_extract_chain_features_returns_none_when_straddle_too_small():
    """All ltps below the 0.5 floor → find_atm_strike returns None → return None."""
    snap_date = date(2025, 5, 1)
    expiry    = date(2025, 5, 8)
    # All ltps < 0.5 so find_atm_strike filters every strike → no common set → None
    chain = _chain_df([
        (snap_date, expiry, 23900.0, "CE", 0.3, 1000),
        (snap_date, expiry, 23900.0, "PE", 0.3, 1000),
        (snap_date, expiry, 24000.0, "CE", 0.3, 1000),
        (snap_date, expiry, 24000.0, "PE", 0.3, 1000),
        (snap_date, expiry, 24100.0, "CE", 0.3, 1000),
        (snap_date, expiry, 24100.0, "PE", 0.3, 1000),
    ])
    snap_df = chain[(chain.snap_date == snap_date) & (chain.expiry == expiry)]
    result = cs.extract_chain_features(snap_df)
    assert result is None


# ── Duplicate-strike regression (CRIT bug) ────────────────────────────────────

def test_extract_chain_features_handles_duplicate_strikes():
    """
    Multiple intraday snapshots for the same expiry produce duplicate strikes
    after set_index('strike'). .get(atm) on a duplicate index returns a Series,
    and float(Series) raises TypeError. groupby().last() must deduplicate first.
    """
    snap_date = date(2025, 5, 1)
    expiry    = date(2025, 5, 8)
    # Simulate two intraday snapshots at same date — same strikes appear twice.
    # ATM=24000 must have the smallest |CE-PE| to be chosen.
    chain = _chain_df([
        # First snapshot
        (snap_date, expiry, 23900.0, "CE", 200.0, 8000),  # deep ITM CE
        (snap_date, expiry, 23900.0, "PE",  10.0, 9000),
        (snap_date, expiry, 24000.0, "CE", 120.0, 10000), # near ATM, small diff
        (snap_date, expiry, 24000.0, "PE", 118.0, 10000),
        (snap_date, expiry, 24100.0, "CE",  10.0,  5000),
        (snap_date, expiry, 24100.0, "PE", 200.0,  5000), # deep ITM PE
        # Second snapshot — duplicate rows for all strikes
        (snap_date, expiry, 23900.0, "CE", 202.0, 8100),
        (snap_date, expiry, 23900.0, "PE",   9.0, 9100),
        (snap_date, expiry, 24000.0, "CE", 122.0, 10500),
        (snap_date, expiry, 24000.0, "PE", 119.0, 10500),
        (snap_date, expiry, 24100.0, "CE",   9.0,  5100),
        (snap_date, expiry, 24100.0, "PE", 201.0,  5100),
    ])
    # Must not raise TypeError — groupby().last() must be applied
    snap_df = chain[(chain.snap_date == snap_date) & (chain.expiry == expiry)]
    result = cs.extract_chain_features(snap_df)
    assert result is not None
    # straddle_premium: last() CE=122, last() PE=119 → 241
    assert abs(result["straddle_premium"] - 241.0) < 0.01


# ── extract_eod_features — key presence ──────────────────────────────────────

def _eod_df(rows):
    """Build EOD DataFrame indexed by snap_date."""
    df = pd.DataFrame(rows, columns=[
        "snap_date", "nifty_spot", "iv_rank", "iv_percentile",
        "atm_ce_iv", "atm_pe_iv", "iv_skew", "pcr_eod",
        "ce_wall_strike", "pe_wall_strike",
    ])
    return df.set_index("snap_date")


def test_extract_eod_features_returns_all_keys():
    """All 8 expected feature keys must be present."""
    snap_date = date(2025, 5, 1)
    eod = _eod_df([(snap_date, 24000.0, 55.0, 60.0, 15.0, 16.0, 1.0, 1.2, 24500.0, 23500.0)])
    r = cs.extract_eod_features(eod, snap_date)
    for key in ("iv_rank", "iv_percentile", "atm_ce_iv", "atm_pe_iv",
                "iv_skew", "pcr_eod", "ce_wall_pct", "pe_wall_pct"):
        assert key in r, f"Missing EOD feature: {key}"


def test_extract_eod_features_returns_empty_when_date_missing():
    """Date not in EOD data → return {} (not crash)."""
    snap_date = date(2025, 5, 1)
    eod = _eod_df([(snap_date, 24000.0, 55.0, 60.0, 15.0, 16.0, 1.0, 1.2, 24500.0, 23500.0)])
    r = cs.extract_eod_features(eod, date(2020, 1, 1))
    assert r == {}


def test_extract_eod_features_wall_pct_positive_when_otm():
    """CE wall above spot → ce_wall_pct > 0. PE wall below spot → pe_wall_pct > 0."""
    snap_date = date(2025, 5, 1)
    eod = _eod_df([(snap_date, 24000.0, 55.0, 60.0, 15.0, 16.0, 1.0, 1.2, 24500.0, 23500.0)])
    r = cs.extract_eod_features(eod, snap_date)
    assert r["ce_wall_pct"] > 0, "CE wall above spot → should be positive"
    assert r["pe_wall_pct"] > 0, "PE wall below spot → should be positive"
