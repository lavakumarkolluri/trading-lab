"""
Tests for pipeline/historical_seeder.py.
Verifies that historical backtest features are seeded from build_dataset()
into analysis.historical_trade_features without circular feedback to the model.
All tests run offline — no ClickHouse connection required.
"""
import sys
import os
import json
from datetime import date
from unittest.mock import MagicMock, patch, call
import pytest
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))


def _make_df(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal DataFrame that mimics build_dataset() output."""
    return pd.DataFrame(rows)


def _base_row(entry_date=date(2024, 1, 15), expiry=date(2024, 1, 16),
              strategy="iron_fly", pnl_pts=10.0, target=1,
              **features) -> dict:
    row = {
        "entry_date":    entry_date,
        "expiry":        expiry,
        "strategy_type": strategy,
        "pnl_pts":       pnl_pts,
        "target":        target,
        "entry_premium": 100.0,
    }
    row.update(features)
    return row


# ── test_seeder_calls_build_dataset_per_symbol ────────────────────────────────

def test_seeder_calls_build_dataset_per_symbol():
    """run() must call build_dataset once per symbol."""
    import historical_seeder as hs

    ch = MagicMock()
    df = _make_df([_base_row(iv_rank=60.0, vix=18.0)])

    with patch.object(hs, "build_dataset", return_value=df) as mock_bd:
        hs.run(ch, symbols=["NIFTY", "BANKNIFTY"])

    assert mock_bd.call_count == 2
    called_syms = {c[1]["symbol"] if c[1] else c[0][1] for c in mock_bd.call_args_list}
    # build_dataset is called as build_dataset(ch, symbol=sym) or build_dataset(ch, sym)
    calls_flat = [str(c) for c in mock_bd.call_args_list]
    assert any("NIFTY" in s for s in calls_flat)
    assert any("BANKNIFTY" in s for s in calls_flat)


# ── test_seeder_serializes_features_as_json ───────────────────────────────────

def test_seeder_serializes_features_as_json():
    """Feature columns must be serialized to valid JSON in features_json."""
    import historical_seeder as hs

    ch = MagicMock()
    df = _make_df([_base_row(iv_rank=65.5, vix=17.2, rsi14=55.0)])

    with patch.object(hs, "build_dataset", return_value=df):
        hs.run(ch, symbols=["NIFTY"])

    assert ch.insert.called
    insert_rows = ch.insert.call_args[0][1]
    assert len(insert_rows) == 1
    feat_json = insert_rows[0][6]          # features_json is 7th column
    parsed = json.loads(feat_json)
    assert "iv_rank" in parsed
    assert abs(parsed["iv_rank"] - 65.5) < 0.01


# ── test_seeder_uses_target_column_for_win_label ──────────────────────────────

def test_seeder_uses_target_column_for_win_label():
    """target=0 loss and target=1 win must both be stored with correct target value."""
    import historical_seeder as hs

    ch = MagicMock()
    df = _make_df([
        _base_row(pnl_pts=15.0, target=1, iv_rank=70.0),
        _base_row(pnl_pts=-20.0, target=0, iv_rank=30.0),
    ])

    with patch.object(hs, "build_dataset", return_value=df):
        hs.run(ch, symbols=["NIFTY"])

    insert_rows = ch.insert.call_args[0][1]
    assert len(insert_rows) == 2
    targets = {r[5] for r in insert_rows}   # target is 6th column (index 5)
    assert 0 in targets
    assert 1 in targets


# ── test_seeder_skips_rows_with_no_valid_features ─────────────────────────────

def test_seeder_skips_rows_with_no_valid_features():
    """Rows where all feature columns are NaN must not be written to ClickHouse."""
    import historical_seeder as hs
    import numpy as np

    ch = MagicMock()
    # Row with all feature values as NaN — no features to serialize
    bad_row = _base_row(pnl_pts=-10.0, target=0)
    # Set all FEATURE_COLS to NaN explicitly
    for fc in hs.FEATURE_COLS:
        bad_row[fc] = float("nan")
    good_row = _base_row(pnl_pts=15.0, target=1, iv_rank=60.0)

    df = _make_df([bad_row, good_row])

    with patch.object(hs, "build_dataset", return_value=df):
        hs.run(ch, symbols=["NIFTY"])

    insert_rows = ch.insert.call_args[0][1]
    assert len(insert_rows) == 1   # bad row dropped


# ── test_seeder_skips_symbol_when_build_dataset_returns_empty ─────────────────

def test_seeder_skips_symbol_when_build_dataset_returns_empty():
    """If build_dataset returns an empty DataFrame, insert must not be called."""
    import historical_seeder as hs

    ch = MagicMock()

    with patch.object(hs, "build_dataset", return_value=pd.DataFrame()):
        hs.run(ch, symbols=["NIFTY"])

    ch.insert.assert_not_called()


# ── test_migration_088_exists ─────────────────────────────────────────────────

def test_migration_088_exists():
    path = os.path.join(
        os.path.dirname(__file__), "..",
        "clickhouse", "migrations", "088_create_historical_trade_features.sql",
    )
    assert os.path.exists(path), "Migration 088 not found"
    sql = open(path).read()
    for col in ("symbol", "entry_date", "expiry", "strategy", "pnl_pts",
                "target", "features_json"):
        assert col in sql, f"Column '{col}' missing from migration 088"
