"""
Unit and integration tests for vol surface integration in strategy_selector.py.
Covers pick_strategy skew tiebreaker, compute_trade_score term_slope bonus,
get_vol_surface_signals DB call, insert_recommendation column alignment,
and migration 065.
All tests run offline — no ClickHouse connection required.
"""
import sys
import os
from datetime import date
from unittest.mock import MagicMock, call

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import strategy_selector as ss


# ── pick_strategy — skew tiebreaker ───────────────────────────────────────────

def test_pick_strategy_bull_put_on_high_pcr():
    assert ss.pick_strategy(70, pcr_bias=1.5, iv_skew=0.0) == "bull_put"

def test_pick_strategy_bear_call_on_low_pcr():
    assert ss.pick_strategy(70, pcr_bias=0.5, iv_skew=0.0) == "bear_call"

def test_pick_strategy_iron_condor_neutral():
    assert ss.pick_strategy(70, pcr_bias=1.0, iv_skew=0.0) == "iron_condor"

def test_pick_strategy_skew_tiebreaker_bull_put():
    """Neutral PCR but elevated surface skew → bull_put."""
    result = ss.pick_strategy(70, pcr_bias=1.0, iv_skew=0.0,
                              skew_from_surface=0.03)  # > VOL_SKEW_BULL_PUT_THRESH (0.02)
    assert result == "bull_put"

def test_pick_strategy_skew_tiebreaker_not_triggered_below_thresh():
    """Skew below threshold → still iron_condor."""
    result = ss.pick_strategy(70, pcr_bias=1.0, iv_skew=0.0,
                              skew_from_surface=0.01)  # < 0.02
    assert result == "iron_condor"

def test_pick_strategy_skew_tiebreaker_overridden_by_bear_signal():
    """Elevated skew should NOT override a bear_call signal (PCR bearish)."""
    result = ss.pick_strategy(70, pcr_bias=0.5, iv_skew=-3.0,
                              skew_from_surface=0.03)
    assert result == "bear_call"

def test_pick_strategy_skew_tiebreaker_blocked_by_call_skew():
    """If options chain shows call skew (skew_bear=True), don't go bull_put."""
    result = ss.pick_strategy(70, pcr_bias=1.0, iv_skew=-3.0,
                              skew_from_surface=0.03)
    assert result == "iron_condor"  # iv_skew < -IV_SKEW_THRESH blocks bull_put


# ── compute_trade_score — term_slope bonus ─────────────────────────────────────

def test_trade_score_baseline_no_term_slope():
    score, bd = ss.compute_trade_score(70, vix=18, iv_rank=50,
                                        pcr_bias=1.0, iv_skew=0.0,
                                        term_slope=0.0)
    assert bd["s_term_slope"] == 0.0

def test_trade_score_bonus_moderate_backwardation():
    """term_slope between -4% and -1% → +3pts bonus."""
    score_base, _ = ss.compute_trade_score(70, vix=18, iv_rank=50,
                                            pcr_bias=1.0, iv_skew=0.0,
                                            term_slope=0.0)
    score_backw, bd = ss.compute_trade_score(70, vix=18, iv_rank=50,
                                              pcr_bias=1.0, iv_skew=0.0,
                                              term_slope=-0.02)
    assert bd["s_term_slope"] == ss.TERM_BACKW_BONUS_PTS
    assert score_backw == pytest.approx(score_base + ss.TERM_BACKW_BONUS_PTS)

def test_trade_score_no_bonus_severe_backwardation():
    """term_slope < -4% (panic) → no bonus."""
    _, bd = ss.compute_trade_score(70, vix=18, iv_rank=50,
                                    pcr_bias=1.0, iv_skew=0.0,
                                    term_slope=-0.05)
    assert bd["s_term_slope"] == 0.0

def test_trade_score_no_bonus_contango():
    """Positive term_slope (normal contango) → no bonus."""
    _, bd = ss.compute_trade_score(70, vix=18, iv_rank=50,
                                    pcr_bias=1.0, iv_skew=0.0,
                                    term_slope=0.02)
    assert bd["s_term_slope"] == 0.0

def test_trade_score_bonus_boundary_values():
    """Exact boundary: term_slope = -0.04 and -0.01 → both in range → bonus."""
    _, bd1 = ss.compute_trade_score(70, vix=18, iv_rank=50,
                                     pcr_bias=1.0, iv_skew=0.0,
                                     term_slope=ss.TERM_BACKW_BONUS_MIN)
    _, bd2 = ss.compute_trade_score(70, vix=18, iv_rank=50,
                                     pcr_bias=1.0, iv_skew=0.0,
                                     term_slope=ss.TERM_BACKW_BONUS_MAX)
    assert bd1["s_term_slope"] == ss.TERM_BACKW_BONUS_PTS
    assert bd2["s_term_slope"] == ss.TERM_BACKW_BONUS_PTS

def test_trade_score_breakdown_has_term_slope_key():
    """Breakdown dict must always contain s_term_slope key."""
    _, bd = ss.compute_trade_score(70, vix=18, iv_rank=50,
                                    pcr_bias=1.0, iv_skew=0.0)
    assert "s_term_slope" in bd


# ── get_vol_surface_signals ────────────────────────────────────────────────────

def test_get_vol_surface_signals_returns_correct_values():
    ch = MagicMock()
    ch.query.return_value.result_rows = [(-0.02, 0.025, 0.035)]
    result = ss.get_vol_surface_signals(ch, "NIFTY", date(2024, 3, 14))
    assert result["term_slope"] == pytest.approx(-0.02)
    assert result["skew_2pct"]  == pytest.approx(0.025)
    assert result["skew_3pct"]  == pytest.approx(0.035)

def test_get_vol_surface_signals_returns_zeros_when_empty():
    ch = MagicMock()
    ch.query.return_value.result_rows = []
    result = ss.get_vol_surface_signals(ch, "NIFTY", date(2024, 3, 14))
    assert result == {"term_slope": 0.0, "skew_2pct": 0.0, "skew_3pct": 0.0}

def test_get_vol_surface_signals_returns_zeros_when_snap_date_none():
    ch = MagicMock()
    result = ss.get_vol_surface_signals(ch, "NIFTY", None)
    assert result["term_slope"] == 0.0
    ch.query.assert_not_called()

def test_get_vol_surface_signals_handles_none_db_values():
    ch = MagicMock()
    ch.query.return_value.result_rows = [(None, None, None)]
    result = ss.get_vol_surface_signals(ch, "NIFTY", date(2024, 3, 14))
    assert result["term_slope"] == 0.0
    assert result["skew_2pct"]  == 0.0

def test_get_vol_surface_signals_queries_correct_params():
    ch = MagicMock()
    ch.query.return_value.result_rows = []
    ss.get_vol_surface_signals(ch, "BANKNIFTY", date(2024, 5, 1))
    params = ch.query.call_args[1]["parameters"]
    assert params["sym"] == "BANKNIFTY"
    assert params["d"] == date(2024, 5, 1)


# ── insert_recommendation column alignment ────────────────────────────────────

def test_insert_recommendation_columns_match_values():
    """Values list and column_names must have same length (prevents ClickHouse crash)."""
    ch = MagicMock()
    rec = {
        "rec_date": date(2024, 3, 14), "symbol": "NIFTY",
        "expiry": date(2024, 3, 14), "strategy": "iron_condor",
        "short_n": 2, "wing_m": 4, "strike_step": 50.0, "atm_strike": 22000.0,
        "short_ce_strike": 22100.0, "short_pe_strike": 21900.0,
        "long_ce_strike": 22300.0, "long_pe_strike": 21700.0,
        "short_ce_entry": 50.0, "short_pe_entry": 50.0,
        "long_ce_entry": 20.0, "long_pe_entry": 20.0,
        "net_credit": 60.0, "max_loss": 140.0, "lots": 2,
        "capital_at_risk": 210000.0, "confidence": 70.0,
        "pcr_bias": 1.1, "iv_skew": 1.5,
        "term_slope": -0.02, "skew_2pct": 0.025,
        "pnl_pts": 0.0, "pnl_amount": 0.0, "outcome": "pending",
    }
    ss.insert_recommendation(ch, rec)
    call_args = ch.insert.call_args
    values_row = call_args[0][1][0]
    col_names  = call_args[1]["column_names"]
    assert len(values_row) == len(col_names), (
        f"insert_recommendation mismatch: {len(values_row)} values vs {len(col_names)} columns"
    )

def test_insert_recommendation_includes_term_slope_and_skew():
    ch = MagicMock()
    rec = {
        "rec_date": date(2024, 3, 14), "symbol": "NIFTY",
        "expiry": date(2024, 3, 14), "strategy": "bull_put",
        "short_n": 1, "wing_m": 3, "strike_step": 50.0, "atm_strike": 22000.0,
        "short_ce_strike": 22050.0, "short_pe_strike": 21950.0,
        "long_ce_strike": 22200.0, "long_pe_strike": 21800.0,
        "short_ce_entry": 30.0, "short_pe_entry": 70.0,
        "long_ce_entry": 10.0, "long_pe_entry": 20.0,
        "net_credit": 70.0, "max_loss": 80.0, "lots": 1,
        "capital_at_risk": 60000.0, "confidence": 72.0,
        "pcr_bias": 1.0, "iv_skew": 0.5,
        "term_slope": -0.02, "skew_2pct": 0.03,
        "pnl_pts": 0.0, "pnl_amount": 0.0, "outcome": "pending",
    }
    ss.insert_recommendation(ch, rec)
    col_names = ch.insert.call_args[1]["column_names"]
    assert "term_slope" in col_names
    assert "skew_2pct"  in col_names


# ── Migration 065 ──────────────────────────────────────────────────────────────

def test_migration_065_exists_and_has_correct_columns():
    path = os.path.join(os.path.dirname(__file__), "..",
                        "clickhouse", "migrations",
                        "065_add_vol_surface_to_trade_recommendations.sql")
    assert os.path.exists(path)
    with open(path) as f:
        sql = f.read()
    assert "trade_recommendations" in sql
    assert "term_slope" in sql
    assert "skew_2pct" in sql
