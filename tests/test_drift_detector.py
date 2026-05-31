"""
Tests for pipeline/drift_detector.py.
Verifies rolling feature correlation, drift detection, and retrain trigger logic.
All tests run offline — no ClickHouse connection required.
"""
import sys
import os
import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch, call
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import drift_detector as dd


# ── pearson_correlation ────────────────────────────────────────────────────────

def test_pearson_positive_correlation():
    """High iv_rank → more wins. Pearson should be positive."""
    xs = [30.0, 50.0, 70.0, 80.0, 90.0]
    ys = [0.0, 0.0, 1.0, 1.0, 1.0]
    r = dd.pearson_correlation(xs, ys)
    assert r > 0.5


def test_pearson_negative_correlation():
    """High vix → more losses. Pearson should be negative."""
    xs = [25.0, 22.0, 18.0, 15.0, 12.0]
    ys = [0.0, 0.0, 1.0, 1.0, 1.0]
    r = dd.pearson_correlation(xs, ys)
    assert r < -0.5


def test_pearson_zero_on_constant_feature():
    """Constant feature has no variance — correlation is 0 (not NaN)."""
    xs = [50.0, 50.0, 50.0, 50.0]
    ys = [0.0, 1.0, 0.0, 1.0]
    r = dd.pearson_correlation(xs, ys)
    assert r == pytest.approx(0.0)


def test_pearson_returns_zero_on_insufficient_data():
    """Fewer than 3 pairs → returns 0 (not enough to be meaningful)."""
    r = dd.pearson_correlation([1.0, 2.0], [0.0, 1.0])
    assert r == pytest.approx(0.0)


def test_pearson_bounded_minus_one_to_one():
    """Pearson must always be in [-1, 1]."""
    import random
    random.seed(42)
    xs = [random.gauss(0, 1) for _ in range(50)]
    ys = [random.gauss(0, 1) for _ in range(50)]
    r = dd.pearson_correlation(xs, ys)
    assert -1.0 <= r <= 1.0


# ── is_drifted ─────────────────────────────────────────────────────────────────

def test_is_drifted_true_when_correlation_drops_significantly():
    """Baseline corr=0.6, current=0.3 → |0.3-0.6|=0.3 > threshold 0.15 → drifted."""
    assert dd.is_drifted(baseline_corr=0.6, current_corr=0.3) is True


def test_is_drifted_false_when_change_is_small():
    """Baseline=0.6, current=0.55 → |0.05| < 0.15 → not drifted."""
    assert dd.is_drifted(baseline_corr=0.6, current_corr=0.55) is False


def test_is_drifted_true_when_sign_flips():
    """Positive correlation flipping to negative is always drifted."""
    assert dd.is_drifted(baseline_corr=0.4, current_corr=-0.2) is True


def test_is_drifted_false_when_baseline_is_zero():
    """Zero baseline means the feature was never predictive — skip drift check."""
    assert dd.is_drifted(baseline_corr=0.0, current_corr=0.3) is False


# ── extract_feature_win_pairs ──────────────────────────────────────────────────

def _outcome_row(features: dict, pnl_pts: float):
    return (json.dumps(features), float(pnl_pts))


def test_extract_returns_feature_win_pairs():
    """Parses entry_features JSON and returns (feature_val, win_label) pairs."""
    rows = [
        _outcome_row({"iv_rank": 70.0, "vix": 18.0}, 30.0),   # win
        _outcome_row({"iv_rank": 20.0, "vix": 26.0}, -15.0),  # loss
    ]
    pairs = dd.extract_feature_win_pairs(rows)
    assert "iv_rank" in pairs
    assert len(pairs["iv_rank"]) == 2
    # First row: iv_rank=70 win=1, second: iv_rank=20 win=0
    assert pairs["iv_rank"][0] == (70.0, 1)
    assert pairs["iv_rank"][1] == (20.0, 0)


def test_extract_skips_malformed_json():
    """Malformed entry_features JSON must not crash — row is silently skipped."""
    rows = [
        ("{not json}", 30.0),
        _outcome_row({"iv_rank": 60.0}, 10.0),
    ]
    pairs = dd.extract_feature_win_pairs(rows)
    assert len(pairs["iv_rank"]) == 1


def test_extract_skips_none_feature_values():
    """None feature values are excluded from correlation computation."""
    rows = [
        _outcome_row({"iv_rank": None, "vix": 18.0}, 30.0),
        _outcome_row({"iv_rank": 70.0, "vix": None}, 20.0),
    ]
    pairs = dd.extract_feature_win_pairs(rows)
    assert len(pairs.get("iv_rank", [])) == 1  # only the non-None one
    assert len(pairs.get("vix", [])) == 1


# ── run() ─────────────────────────────────────────────────────────────────────

def _build_ch_mock(trade_rows, baseline_rows=None):
    """Helper: return a CH mock that returns trade_rows for trade queries and
    baseline_rows for baseline queries."""
    ch = MagicMock()
    call_count = {"n": 0}

    def side_effect(sql, parameters=None):
        result = MagicMock()
        if "baseline" in sql.lower() or (baseline_rows is not None and call_count["n"] > 0):
            result.result_rows = baseline_rows if baseline_rows else trade_rows
        else:
            result.result_rows = trade_rows
        call_count["n"] += 1
        return result

    ch.query.side_effect = side_effect
    return ch


def test_run_inserts_feature_performance_rows():
    """run() must write at least one row to analysis.feature_performance."""
    trade_rows = [
        _outcome_row({"iv_rank": float(70 + i), "vix": float(18 + i)}, 10.0 * (i % 2 or -1))
        for i in range(20)
    ]
    ch = MagicMock()
    ch.query.return_value.result_rows = trade_rows
    dd.run(ch, symbol="NIFTY", run_date=date(2026, 5, 31))
    assert ch.insert.called


def test_run_emits_retrain_trigger_when_two_features_drift():
    """If 2+ features drift for the same symbol, a retrain trigger is inserted."""
    # Create data where both iv_rank and vix have drifted correlation
    trade_rows = [_outcome_row({"iv_rank": 50.0, "vix": 20.0}, -10.0) for _ in range(30)]
    # Baseline: strong positive correlation for both features
    baseline_rows = [_outcome_row({"iv_rank": float(60 + i), "vix": float(15 + i)}, float(i % 2 * 20))
                     for i in range(60)]

    ch = MagicMock()
    call_count = {"n": 0}

    def side_effect(sql, parameters=None):
        result = MagicMock()
        # First call = window data (30d), second+ = baseline data
        call_count["n"] += 1
        if call_count["n"] == 1:
            result.result_rows = trade_rows
        else:
            result.result_rows = baseline_rows
        return result

    ch.query.side_effect = side_effect

    with patch.object(dd, "_should_trigger_retrain", return_value=True):
        dd.run(ch, symbol="NIFTY", run_date=date(2026, 5, 31))

    # Check that retrain_triggers table was written to
    insert_calls = [c for c in ch.insert.call_args_list
                    if "retrain_triggers" in str(c)]
    assert insert_calls, "Expected retrain_triggers insert when 2+ features drift"


def test_run_no_retrain_trigger_when_insufficient_data():
    """With fewer than MIN_TRADES_FOR_DRIFT trades, no drift detected, no trigger."""
    trade_rows = [_outcome_row({"iv_rank": 50.0}, -10.0) for _ in range(3)]
    ch = MagicMock()
    ch.query.return_value.result_rows = trade_rows
    dd.run(ch, symbol="NIFTY", run_date=date(2026, 5, 31))
    # No retrain triggers should be written
    insert_calls = [c for c in ch.insert.call_args_list
                    if "retrain_triggers" in str(c)]
    assert not insert_calls


# ── historical baseline / live-only current window ────────────────────────────

def test_run_current_window_uses_live_only_no_false_drift():
    """Current windows must query trades.trade_outcomes, NOT historical_trade_features.
    If only historical data exists (no live trades), no drift is detected and no
    retrain trigger is emitted — preventing false positive retrains from backtest
    data variance poisoning the drift signal.
    """
    ch = MagicMock()

    def side_effect(sql, parameters=None):
        result = MagicMock()
        if "historical_trade_features" in sql:
            # Baseline: return rich historical data
            result.result_rows = [
                (json.dumps({"iv_rank": float(60 + i), "vix": float(15 + i)}), float(i % 2 * 20))
                for i in range(60)
            ]
        else:
            # Live trades (trade_outcomes): empty — no paper trades yet
            result.result_rows = []
        return result

    ch.query.side_effect = side_effect
    dd.run(ch, symbol="NIFTY", run_date=date(2026, 5, 31))

    # With no live trades in any current window, no drift can be detected
    insert_calls = [c for c in ch.insert.call_args_list
                    if "retrain_triggers" in str(c)]
    assert not insert_calls, (
        "No retrain trigger should fire when current window has zero live trades. "
        "Historical data must only be used for baseline, not current windows."
    )


# ── migration 086 ─────────────────────────────────────────────────────────────

def test_migration_086_exists_with_required_tables():
    path = os.path.join(os.path.dirname(__file__), "..",
                        "clickhouse", "migrations", "086_create_feature_performance.sql")
    assert os.path.exists(path)
    sql = open(path).read()
    assert "feature_performance" in sql
    assert "retrain_triggers" in sql
    assert "drift_detected" in sql
    assert "correlation" in sql
