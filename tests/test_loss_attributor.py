"""
Tests for pipeline/loss_attributor.py.
Verifies feature attribution logic for closed losing trades.
All tests run offline — no ClickHouse connection required.
"""
import sys
import os
import json
from datetime import date, datetime
from unittest.mock import MagicMock, call, patch
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import loss_attributor as la


# ── compute_attribution ────────────────────────────────────────────────────────

def test_attribution_zero_when_feature_equals_win_mean():
    """If the losing trade's feature value equals the winning population mean,
    attribution is zero — the feature was not unusual."""
    attr = la.compute_attribution(feature_val=50.0, win_mean=50.0, win_std=10.0)
    assert attr == pytest.approx(0.0)


def test_attribution_positive_when_feature_worse_than_winners():
    """iv_rank=10 (low) on a losing short-premium trade — lower than winners' mean of 60.
    Attribution should be negative (protective direction for short premium is high iv_rank).
    But for features where HIGH = good and LOW = present in losses, attribution is negative.

    We use (feature_val - win_mean) / win_std — positive means feature exceeded winning mean,
    which for most features means "riskier side." The caller interprets sign per feature semantics.
    """
    attr = la.compute_attribution(feature_val=80.0, win_mean=60.0, win_std=10.0)
    assert attr == pytest.approx(2.0)   # 2 standard deviations above win mean


def test_attribution_negative_when_feature_below_win_mean():
    attr = la.compute_attribution(feature_val=40.0, win_mean=60.0, win_std=10.0)
    assert attr == pytest.approx(-2.0)


def test_attribution_stable_with_zero_std():
    """Zero std (all winners had same value) must not raise ZeroDivisionError."""
    attr = la.compute_attribution(feature_val=50.0, win_mean=50.0, win_std=0.0)
    assert attr == pytest.approx(0.0)


def test_attribution_large_deviation_on_zero_std():
    """Feature deviated from win_mean but win_std=0 — caps at ±10 to avoid inf."""
    attr = la.compute_attribution(feature_val=80.0, win_mean=50.0, win_std=0.0)
    assert abs(attr) <= 10.0


# ── build_win_profile ─────────────────────────────────────────────────────────

def test_build_win_profile_computes_mean_and_std():
    """Returns {feature: (mean, std)} from a list of winning trade feature dicts."""
    winning_features = [
        {"iv_rank": 60.0, "vix": 18.0},
        {"iv_rank": 70.0, "vix": 20.0},
        {"iv_rank": 80.0, "vix": 22.0},
    ]
    profile = la.build_win_profile(winning_features)
    assert profile["iv_rank"][0] == pytest.approx(70.0)   # mean
    assert profile["vix"][0] == pytest.approx(20.0)


def test_build_win_profile_returns_empty_on_no_winners():
    """No winning trades → empty profile (don't crash)."""
    profile = la.build_win_profile([])
    assert profile == {}


def test_build_win_profile_skips_none_values():
    """None values in features JSON must be ignored (treated as missing, not zero)."""
    winning_features = [
        {"iv_rank": 60.0, "vix": None},
        {"iv_rank": 70.0, "vix": 20.0},
    ]
    profile = la.build_win_profile(winning_features)
    assert "iv_rank" in profile
    # vix has one None — profile should still compute from valid values
    assert profile["vix"][0] == pytest.approx(20.0)


# ── attribute_trade ───────────────────────────────────────────────────────────

def test_attribute_trade_returns_row_per_feature():
    """For a loss with N features, returns N attribution rows."""
    win_profile = {
        "iv_rank": (60.0, 10.0),
        "vix":     (18.0, 3.0),
    }
    rows = la.attribute_trade(
        trade_id="abc123",
        symbol="NIFTY",
        exit_date=date(2026, 5, 30),
        exit_reason="stop",
        pnl_pts=-45.0,
        features={"iv_rank": 30.0, "vix": 25.0},
        win_profile=win_profile,
    )
    assert len(rows) == 2
    feature_names = {r["feature_name"] for r in rows}
    assert feature_names == {"iv_rank", "vix"}


def test_attribute_trade_skips_features_not_in_profile():
    """Features absent from win_profile (no historical winners) are skipped."""
    win_profile = {"iv_rank": (60.0, 10.0)}
    rows = la.attribute_trade(
        trade_id="abc", symbol="NIFTY", exit_date=date(2026, 5, 30),
        exit_reason="stop", pnl_pts=-20.0,
        features={"iv_rank": 30.0, "unknown_feature": 5.0},
        win_profile=win_profile,
    )
    assert len(rows) == 1
    assert rows[0]["feature_name"] == "iv_rank"


def test_attribute_trade_row_has_required_fields():
    """Each row must contain all columns expected by the DB insert."""
    win_profile = {"vix": (18.0, 3.0)}
    rows = la.attribute_trade(
        trade_id="t1", symbol="BANKNIFTY", exit_date=date(2026, 5, 30),
        exit_reason="eod", pnl_pts=-10.0,
        features={"vix": 25.0},
        win_profile=win_profile,
    )
    row = rows[0]
    for field in ("trade_id", "symbol", "exit_date", "exit_reason",
                  "pnl_pts", "feature_name", "feature_value", "win_mean", "attribution"):
        assert field in row, f"Missing field: {field}"


# ── run() — integration ───────────────────────────────────────────────────────

def _loss_row(trade_id, pnl_pts, features: dict):
    """Tuple matching: trade_id, symbol, exit_date, exit_reason, pnl_pts, entry_features."""
    return (trade_id, "NIFTY", date(2026, 5, 30), "stop", float(pnl_pts), json.dumps(features))


def _win_feat_row(features: dict):
    """Win profile query returns only (entry_features,)."""
    return (json.dumps(features),)


# 5 winning trades (MIN_WIN_TRADES threshold)
_WINS = [_win_feat_row({"iv_rank": 70.0 + i, "vix": 18.0 + i}) for i in range(5)]


def _make_ch(live_losses=None, hist_losses=None, live_wins=None, hist_wins=None):
    """Build a CH mock routing 4 distinct queries in loss_attributor.run()."""
    ch = MagicMock()

    def side_effect(sql, parameters=None):
        result = MagicMock()
        if "pnl_pts < 0" in sql:                        # live losses
            result.result_rows = list(live_losses or [])
        elif "target = 0" in sql:                        # historical losses
            result.result_rows = list(hist_losses or [])
        elif "pnl_pts >= 0" in sql:                      # live wins
            result.result_rows = list(live_wins or [])
        elif "target = 1" in sql:                        # historical wins
            result.result_rows = list(hist_wins or [])
        else:
            result.result_rows = []
        return result

    ch.query.side_effect = side_effect
    return ch


def test_run_attributes_losses_only():
    """run() must only attribute trades where pnl_pts < 0."""
    loss = _loss_row("loss1", -30.0, {"iv_rank": 20.0, "vix": 25.0})
    ch = _make_ch(live_losses=[loss], live_wins=_WINS)

    la.run(ch, run_date=date(2026, 5, 30))

    assert ch.insert.called
    insert_rows = ch.insert.call_args[0][1]
    trade_ids = {r[0] for r in insert_rows}
    assert "loss1" in trade_ids


def test_run_skips_when_no_losses():
    """If no losses (live or historical), insert is not called."""
    ch = _make_ch()
    la.run(ch, run_date=date(2026, 5, 30))
    ch.insert.assert_not_called()


def test_run_skips_when_no_win_profile():
    """If no winning trades at all (live or historical), attribution cannot be computed."""
    loss = _loss_row("loss1", -30.0, {"iv_rank": 20.0})
    ch = _make_ch(live_losses=[loss])   # no wins anywhere

    la.run(ch, run_date=date(2026, 5, 30))
    ch.insert.assert_not_called()


def test_run_handles_missing_entry_features_gracefully():
    """If entry_features JSON is empty, skip that trade."""
    loss_no_features = _loss_row("loss2", -20.0, {})
    ch = _make_ch(live_losses=[loss_no_features], live_wins=_WINS)

    la.run(ch, run_date=date(2026, 5, 30))
    ch.insert.assert_not_called()


# ── historical data integration ───────────────────────────────────────────────

def test_run_uses_historical_data_when_no_live_trades():
    """When no live paper trades exist, historical losses + wins still produce attributions.
    This ensures the dashboard shows real data before any paper trades close.
    """
    ch = MagicMock()

    hist_loss = (
        "NIFTY_2024-01-15",           # trade_id (concat)
        "NIFTY",                       # symbol
        date(2024, 1, 15),             # exit_date (entry_date)
        "iron_fly",                    # exit_reason (strategy)
        -25.0,                         # pnl_pts
        json.dumps({"iv_rank": 20.0, "vix": 28.0}),  # features_json
    )

    def side_effect(sql, parameters=None):
        result = MagicMock()
        if "pnl_pts < 0" in sql and "trade_outcomes" in sql:
            result.result_rows = []            # no live losses
        elif "target = 0" in sql:
            result.result_rows = [hist_loss]   # historical losses
        elif "trade_outcomes" in sql and "pnl_pts >= 0" in sql:
            result.result_rows = []            # no live wins
        elif "target = 1" in sql:
            # 5 historical winners to meet MIN_WIN_TRADES
            result.result_rows = [
                (json.dumps({"iv_rank": float(70 + i), "vix": float(16 + i)}),)
                for i in range(5)
            ]
        else:
            result.result_rows = []
        return result

    ch.query.side_effect = side_effect
    la.run(ch, run_date=date(2026, 5, 31))

    assert ch.insert.called, "Expected attributions even with no live paper trades"
    insert_rows = ch.insert.call_args[0][1]
    trade_ids = {r[0] for r in insert_rows}
    assert "NIFTY_2024-01-15" in trade_ids


# ── migration 085 ─────────────────────────────────────────────────────────────

def test_migration_085_exists_with_required_columns():
    path = os.path.join(os.path.dirname(__file__), "..",
                        "clickhouse", "migrations", "085_create_trade_attributions.sql")
    assert os.path.exists(path), "Migration 085 not found"
    sql = open(path).read()
    for col in ("trade_id", "feature_name", "attribution", "win_mean", "exit_date"):
        assert col in sql, f"Column '{col}' missing from migration 085"
